<?php

namespace Mimon\KafkaLogger\Support;

use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Jobs\FlushCoalescedErrorJob;
use Mimon\KafkaLogger\Services\LoggerService;
use Throwable;

/**
 * Coalesces identical ERROR/CRITICAL logs within a millisecond burst window (default 1500ms)
 * so only a single error instance is produced per burst along with an accurate occurrence count.
 */
class ErrorBurstDeduplicator
{
    public const COUNT_UPDATE_EVENT = 'ERROR_BURST_COUNT_UPDATE';

    public const CACHE_PREFIX = 'mimon_kafka_logger:err_dedup:';

    public const PENDING_INDEX_KEY = 'mimon_kafka_logger:err_dedup:pending_index';

    /**
     * In-process coalescing buffer keyed by error signature fingerprint.
     *
     * @var array<string, array{
     *     payload: array<string, mixed>,
     *     topics: array<int, string>,
     *     count: int,
     *     first_seen_at_ms: int,
     *     last_seen_at_ms: int,
     *     first_seen_at: string,
     *     last_seen_at: string
     * }>
     */
    protected static array $inProcessBuffer = [];

    protected static bool $shutdownRegistered = false;

    protected static bool $isFlushing = false;

    /** @var (callable(): int)|null */
    protected static $clockMs = null;

    /** @var (callable(array<int, string>, array<string, mixed>): void)|null */
    protected static $publisher = null;

    /**
     * Normalize a log message so dynamic IDs, UUIDs, hex tokens, and timestamps share one fingerprint.
     */
    public static function normalizeMessage(string $message): string
    {
        $normalized = preg_replace(
            '/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i',
            '<uuid>',
            $message
        ) ?? $message;

        $normalized = preg_replace(
            '/\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b/',
            '<ts>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/\b(?:0x)?[0-9a-f]{16,}\b/i',
            '<hex>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace('/\b\d+\b/', '#', $normalized) ?? $normalized;
        $normalized = preg_replace('/\s+/', ' ', trim($normalized)) ?? trim($normalized);

        return substr($normalized, 0, 200);
    }

    /**
     * Compute a deterministic signature hash for an error payload.
     */
    public static function fingerprint(array $payload, ?bool $includeFileLine = null): string
    {
        $includeFileLine ??= (bool) PayloadSanitizer::config('kafka-logger.error_dedup.include_file_line', true);

        $appCode = strtoupper((string) ($payload['application']['code'] ?? ''));
        $level = strtoupper((string) ($payload['event']['level'] ?? 'INFO'));
        $eventType = strtoupper((string) ($payload['event']['type'] ?? ''));
        $component = strtoupper((string) ($payload['application']['component'] ?? ''));
        $errorCode = (string) ($payload['error']['code'] ?? '');
        $normalizedMessage = self::normalizeMessage((string) ($payload['event']['message'] ?? ''));

        $fileLine = '';
        if ($includeFileLine) {
            $meta = is_array($payload['metadata'] ?? null) ? $payload['metadata'] : [];
            $file = isset($meta['file']) && is_string($meta['file']) && $meta['file'] !== ''
                ? basename(str_replace('\\', '/', $meta['file']))
                : '';
            $line = isset($meta['line']) && is_numeric($meta['line']) ? (string) $meta['line'] : '';
            if ($file !== '') {
                $fileLine = $line !== '' ? "{$file}:{$line}" : $file;
            }
        }

        return sha1("{$appCode}|{$level}|{$eventType}|{$component}|{$errorCode}|{$fileLine}|{$normalizedMessage}");
    }

    public static function isEnabled(): bool
    {
        return (bool) PayloadSanitizer::config('kafka-logger.error_dedup.enabled', true)
            && self::windowMs() > 0;
    }

    public static function windowMs(): int
    {
        return max(0, (int) PayloadSanitizer::config('kafka-logger.error_dedup.window_ms', 1500));
    }

    /**
     * @return array<int, string>
     */
    public static function targetLevels(): array
    {
        $raw = PayloadSanitizer::config('kafka-logger.error_dedup.levels', ['ERROR', 'CRITICAL']);
        if (is_array($raw)) {
            return array_values(array_filter(array_map(
                static fn ($l): string => strtoupper(trim((string) $l)),
                $raw
            )));
        }

        return array_values(array_filter(array_map(
            static fn (string $l): string => strtoupper(trim($l)),
            explode(',', (string) $raw)
        )));
    }

    public static function shouldDeduplicate(array $payload): bool
    {
        if (! self::isEnabled()) {
            return false;
        }

        $eventType = strtoupper((string) ($payload['event']['type'] ?? ''));
        if ($eventType === self::COUNT_UPDATE_EVENT) {
            return false;
        }

        $level = strtoupper((string) ($payload['event']['level'] ?? 'INFO'));

        return in_array($level, self::targetLevels(), true);
    }

    /**
     * Capture an error payload into the 1.5s burst coalescer.
     *
     * Returns:
     *   - handled: false when dedup is disabled or level is excluded (caller publishes immediately)
     *   - duplicate: true when collapsed into an existing active burst (returns empty payload to caller)
     *   - payload: enriched representative payload when this is the first occurrence in the window
     *
     * @param  array<int, string>  $targetTopics
     * @return array{handled: bool, duplicate: bool, payload: array<string, mixed>}
     */
    public static function capture(array $payload, array $targetTopics): array
    {
        if (! self::shouldDeduplicate($payload)) {
            return ['handled' => false, 'duplicate' => false, 'payload' => $payload];
        }

        $nowMs = self::nowMs();
        $windowMs = self::windowMs();

        // Flush any already-expired in-process windows first (important for long-running loops/workers)
        self::flushExpiredInProcess($nowMs, $windowMs);

        $fingerprint = self::fingerprint($payload);
        $createdAtIso = (string) ($payload['timestamps']['created_at'] ?? Carbon::now()->toIso8601String());

        if (isset(self::$inProcessBuffer[$fingerprint])) {
            self::$inProcessBuffer[$fingerprint]['count']++;
            self::$inProcessBuffer[$fingerprint]['last_seen_at_ms'] = $nowMs;
            self::$inProcessBuffer[$fingerprint]['last_seen_at'] = $createdAtIso;

            return ['handled' => true, 'duplicate' => true, 'payload' => []];
        }

        $payload['metadata'] = is_array($payload['metadata'] ?? null) ? $payload['metadata'] : [];
        $payload['metadata']['_dedup_fingerprint'] = $fingerprint;
        $payload['metadata']['_dedup_window_ms'] = $windowMs;
        $payload['metadata']['_occurrence_count'] = 1;
        $payload['metadata']['_suppressed_duplicates'] = 0;
        $payload['metadata']['_first_seen_at'] = $createdAtIso;
        $payload['metadata']['_last_seen_at'] = $createdAtIso;

        self::$inProcessBuffer[$fingerprint] = [
            'payload' => $payload,
            'topics' => $targetTopics,
            'count' => 1,
            'first_seen_at_ms' => $nowMs,
            'last_seen_at_ms' => $nowMs,
            'first_seen_at' => $createdAtIso,
            'last_seen_at' => $createdAtIso,
        ];

        self::registerShutdownFlush();

        return ['handled' => true, 'duplicate' => false, 'payload' => $payload];
    }

    /**
     * Flush all buffered in-process errors (called at request termination / shutdown / tests).
     *
     * @return array<int, array<string, mixed>> Published leader payloads
     */
    public static function flush(): array
    {
        if (self::$isFlushing) {
            return [];
        }

        self::$isFlushing = true;
        $emitted = [];

        try {
            $entries = self::$inProcessBuffer;
            self::$inProcessBuffer = [];

            foreach ($entries as $fingerprint => $entry) {
                $published = self::emitOrCoalesceCrossProcess($fingerprint, $entry);
                if ($published !== null) {
                    $emitted[] = $published;
                }
            }

            self::flushExpiredCrossProcessCounts();
        } catch (Throwable) {
            // Never disrupt request termination
        } finally {
            self::$isFlushing = false;
        }

        return $emitted;
    }

    /**
     * Flush only in-process entries whose burst window has already elapsed.
     */
    public static function flushExpiredInProcess(?int $nowMs = null, ?int $windowMs = null): void
    {
        if (empty(self::$inProcessBuffer) || self::$isFlushing) {
            return;
        }

        $nowMs ??= self::nowMs();
        $windowMs ??= self::windowMs();

        $expired = [];
        foreach (self::$inProcessBuffer as $fingerprint => $entry) {
            if (($nowMs - $entry['first_seen_at_ms']) >= $windowMs) {
                $expired[$fingerprint] = $entry;
                unset(self::$inProcessBuffer[$fingerprint]);
            }
        }

        if (empty($expired)) {
            return;
        }

        self::$isFlushing = true;
        try {
            foreach ($expired as $fingerprint => $entry) {
                self::emitOrCoalesceCrossProcess($fingerprint, $entry);
            }
        } catch (Throwable) {
        } finally {
            self::$isFlushing = false;
        }
    }

    /**
     * Called by FlushCoalescedErrorJob after the 1.5s delay when async queue mode is active.
     */
    public static function flushDelayedJobEntry(string $fingerprint): ?array
    {
        $bufKey = self::CACHE_PREFIX . "buf:{$fingerprint}";
        $cntKey = self::CACHE_PREFIX . "cnt:{$fingerprint}";

        $buffered = self::cachePull($bufKey);
        if (! is_array($buffered) || empty($buffered['payload']) || empty($buffered['topics'])) {
            return null;
        }

        $extra = self::cachePull($cntKey);
        $extraCount = is_array($extra) ? max(0, (int) ($extra['delta_count'] ?? 0)) : 0;
        $lastSeenAt = (is_array($extra) && ! empty($extra['last_seen_at']))
            ? (string) $extra['last_seen_at']
            : (string) ($buffered['last_seen_at'] ?? ($buffered['payload']['timestamps']['created_at'] ?? Carbon::now()->toIso8601String()));

        $totalCount = max(1, (int) ($buffered['count'] ?? 1)) + $extraCount;

        $payload = $buffered['payload'];
        $payload['metadata'] = is_array($payload['metadata'] ?? null) ? $payload['metadata'] : [];
        $payload['metadata']['_dedup_fingerprint'] = $fingerprint;
        $payload['metadata']['_dedup_window_ms'] = (int) ($buffered['window_ms'] ?? self::windowMs());
        $payload['metadata']['_occurrence_count'] = $totalCount;
        $payload['metadata']['_suppressed_duplicates'] = max(0, $totalCount - 1);
        $payload['metadata']['_first_seen_at'] = (string) ($buffered['first_seen_at'] ?? ($payload['timestamps']['created_at'] ?? $lastSeenAt));
        $payload['metadata']['_last_seen_at'] = $lastSeenAt;

        self::removeFromPendingIndex($fingerprint);

        if (LoggerService::shouldSuppressBySustainedRateLimit($payload, $fingerprint)) {
            return null;
        }

        self::publishPayload($buffered['topics'], $payload, fromQueue: true);

        return $payload;
    }

    /**
     * Flush any expired cross-process suppressed count rollups as lightweight control events
     * so CLS folds trailing counts into the leader SystemLog row without inserting a second row.
     */
    public static function flushExpiredCrossProcessCounts(): int
    {
        $nowMs = self::nowMs();
        $index = self::cacheGet(self::PENDING_INDEX_KEY);
        if (! is_array($index) || empty($index)) {
            return 0;
        }

        $flushed = 0;
        $remaining = [];

        foreach ($index as $fingerprint => $expiresAtMs) {
            if (! is_string($fingerprint)) {
                continue;
            }

            if ((int) $expiresAtMs > $nowMs) {
                $remaining[$fingerprint] = (int) $expiresAtMs;
                continue;
            }

            // Atomically claim this expired rollup so only one worker emits the count update
            $claimKey = self::CACHE_PREFIX . "claim:{$fingerprint}:{$expiresAtMs}";
            if (! self::cacheAdd($claimKey, 1, 15)) {
                continue;
            }

            $cntKey = self::CACHE_PREFIX . "cnt:{$fingerprint}";
            $pending = self::cachePull($cntKey);

            if (! is_array($pending) || (int) ($pending['delta_count'] ?? 0) <= 0) {
                continue;
            }

            $deltaCount = (int) $pending['delta_count'];
            $leaderLogId = (string) ($pending['leader_log_id'] ?? '');
            if ($leaderLogId === '') {
                continue;
            }

            $topics = is_array($pending['topics'] ?? null) && ! empty($pending['topics'])
                ? $pending['topics']
                : LoggerService::resolveTargetTopics(null);

            $controlPayload = LoggerService::buildEnterprisePayload(
                appCode: (string) ($pending['app_code'] ?? ''),
                eventType: self::COUNT_UPDATE_EVENT,
                message: "Error burst count update (+{$deltaCount}) for {$leaderLogId}.",
                level: LogLevel::INFO,
                component: ComponentSubsystem::tryFrom((string) ($pending['component'] ?? '')) ?? ComponentSubsystem::APPLICATION_LOGIC,
                topic: $topics[0] ?? null,
                userId: 'usr_system',
                userRole: 'SYSTEM',
                metadata: [
                    '_leader_log_id' => $leaderLogId,
                    '_leader_created_at' => (string) ($pending['leader_created_at'] ?? ''),
                    '_dedup_fingerprint' => $fingerprint,
                    '_dedup_window_ms' => (int) ($pending['window_ms'] ?? self::windowMs()),
                    '_delta_count' => $deltaCount,
                    '_last_seen_at' => (string) ($pending['last_seen_at'] ?? Carbon::now()->toIso8601String()),
                    '_target_level' => (string) ($pending['level'] ?? 'ERROR'),
                    '_target_event' => (string) ($pending['event_type'] ?? 'APPLICATION_ERROR'),
                ],
                environment: (string) ($pending['environment'] ?? ''),
                appName: (string) ($pending['app_name'] ?? $pending['app_code'] ?? ''),
            );

            if (! empty($controlPayload)) {
                self::publishPayload($topics, $controlPayload);
                $flushed++;
            }
        }

        if (empty($remaining)) {
            self::cacheForget(self::PENDING_INDEX_KEY);
        } else {
            self::cachePut(self::PENDING_INDEX_KEY, array_slice($remaining, -100, null, true), 60);
        }

        return $flushed;
    }

    /**
     * Reset all in-process state and test overrides (for PHPUnit / Octane).
     */
    public static function reset(): void
    {
        self::$inProcessBuffer = [];
        self::$isFlushing = false;
        self::$clockMs = null;
        self::$publisher = null;
    }

    public static function getBufferedCount(): int
    {
        return count(self::$inProcessBuffer);
    }

    /**
     * @param  (callable(): int)|null  $clockMs
     */
    public static function setClock(?callable $clockMs): void
    {
        self::$clockMs = $clockMs;
    }

    /**
     * @param  (callable(array<int, string>, array<string, mixed>): void)|null  $publisher
     */
    public static function setPublisher(?callable $publisher): void
    {
        self::$publisher = $publisher;
    }

    public static function nowMs(): int
    {
        if (self::$clockMs !== null) {
            return (int) (self::$clockMs)();
        }

        return (int) round(microtime(true) * 1000);
    }

    /**
     * Coordinate cross-process 1500ms deduplication and publish or suppress accordingly.
     *
     * @param  array<string, mixed>  $entry
     */
    protected static function emitOrCoalesceCrossProcess(string $fingerprint, array $entry): ?array
    {
        $nowMs = self::nowMs();
        $windowMs = self::windowMs();
        $expiresAtMs = $nowMs + $windowMs;

        $payload = $entry['payload'];
        $topics = $entry['topics'];
        $count = max(1, (int) $entry['count']);

        $windowKey = self::CACHE_PREFIX . "win:{$fingerprint}";
        $leaderMeta = [
            'leader_log_id' => (string) ($payload['log_id'] ?? ''),
            'leader_created_at' => (string) ($payload['timestamps']['created_at'] ?? ''),
            'app_code' => (string) ($payload['application']['code'] ?? ''),
            'app_name' => (string) ($payload['application']['name'] ?? ''),
            'environment' => (string) ($payload['environment'] ?? ''),
            'component' => (string) ($payload['application']['component'] ?? 'APPLICATION_LOGIC'),
            'level' => (string) ($payload['event']['level'] ?? 'ERROR'),
            'event_type' => (string) ($payload['event']['type'] ?? 'APPLICATION_ERROR'),
            'topics' => $topics,
            'window_ms' => $windowMs,
            'expires_at_ms' => $expiresAtMs,
        ];

        $acquired = self::tryAcquireCrossProcessWindow($windowKey, $leaderMeta, $windowMs, $nowMs);

        if (! $acquired) {
            // Another worker already owns this 1500ms window: suppress full error & count atomically
            $existingLeader = self::cacheGet($windowKey);
            if (is_array($existingLeader) && ! empty($existingLeader['leader_log_id'])) {
                self::recordCrossProcessDuplicate(
                    fingerprint: $fingerprint,
                    leader: $existingLeader,
                    deltaCount: $count,
                    lastSeenAt: (string) $entry['last_seen_at'],
                    windowMs: $windowMs,
                );
            }

            return null;
        }

        // We won the 1500ms window!
        $flushMode = strtolower((string) PayloadSanitizer::config('kafka-logger.error_dedup.flush_mode', 'auto'));
        $isAsync = (bool) PayloadSanitizer::config('kafka-logger.async', false);
        $useDelayedJob = ($flushMode === 'delayed_job' || ($flushMode === 'auto' && $isAsync))
            && function_exists('dispatch')
            && self::$publisher === null;

        if ($useDelayedJob) {
            $bufKey = self::CACHE_PREFIX . "buf:{$fingerprint}";
            self::cachePut($bufKey, [
                'payload' => $payload,
                'topics' => $topics,
                'count' => $count,
                'window_ms' => $windowMs,
                'first_seen_at' => $entry['first_seen_at'],
                'last_seen_at' => $entry['last_seen_at'],
            ], max(15, (int) ceil($windowMs / 1000) * 5));

            try {
                dispatch((new FlushCoalescedErrorJob($fingerprint))->delay(Carbon::now()->addMilliseconds($windowMs)));

                return $payload;
            } catch (Throwable) {
                // Queue unavailable -> fall through to immediate publish below
                self::cacheForget($bufKey);
            }
        }

        // Sync mode (or queue fallback): enrich with in-process burst count and publish 1 error instance
        $payload['metadata'] = is_array($payload['metadata'] ?? null) ? $payload['metadata'] : [];
        $payload['metadata']['_dedup_fingerprint'] = $fingerprint;
        $payload['metadata']['_dedup_window_ms'] = $windowMs;
        $payload['metadata']['_occurrence_count'] = $count;
        $payload['metadata']['_suppressed_duplicates'] = max(0, $count - 1);
        $payload['metadata']['_first_seen_at'] = $entry['first_seen_at'];
        $payload['metadata']['_last_seen_at'] = $entry['last_seen_at'];

        if (LoggerService::shouldSuppressBySustainedRateLimit($payload, $fingerprint)) {
            return null;
        }

        self::publishPayload($topics, $payload);

        return $payload;
    }

    /**
     * Atomically acquire a millisecond-precision cross-process window lock.
     * Uses native Redis `SET NX PX` when Redis is active, with safe Cache::add fallback.
     *
     * @param  array<string, mixed>  $leaderMeta
     */
    protected static function tryAcquireCrossProcessWindow(string $key, array $leaderMeta, int $windowMs, int $nowMs): bool
    {
        try {
            if (! class_exists('\Illuminate\Support\Facades\Cache')) {
                return true;
            }

            $store = Cache::getStore();
            if ($store instanceof \Illuminate\Cache\RedisStore) {
                $prefixed = $store->getPrefix() . $key;
                $serialized = serialize($leaderMeta);
                $result = $store->connection()->set($prefixed, $serialized, 'PX', max(1, $windowMs), 'NX');

                return (bool) $result;
            }

            $existing = Cache::get($key);
            if (is_array($existing)) {
                if ((int) ($existing['expires_at_ms'] ?? 0) > $nowMs) {
                    return false;
                }
                Cache::forget($key);
            }

            return (bool) Cache::add($key, $leaderMeta, max(1, (int) ceil($windowMs / 1000)));
        } catch (Throwable) {
            return true;
        }
    }

    /**
     * Accumulate suppressed cross-process error count for the active leader window.
     *
     * @param  array<string, mixed>  $leader
     */
    protected static function recordCrossProcessDuplicate(
        string $fingerprint,
        array $leader,
        int $deltaCount,
        string $lastSeenAt,
        int $windowMs,
    ): void {
        $cntKey = self::CACHE_PREFIX . "cnt:{$fingerprint}";
        $ttlSeconds = max(30, (int) ceil($windowMs / 1000) * 10);

        $existing = self::cacheGet($cntKey);
        if (! is_array($existing) || ($existing['leader_log_id'] ?? null) !== ($leader['leader_log_id'] ?? null)) {
            $existing = [
                'leader_log_id' => $leader['leader_log_id'] ?? '',
                'leader_created_at' => $leader['leader_created_at'] ?? '',
                'app_code' => $leader['app_code'] ?? '',
                'app_name' => $leader['app_name'] ?? '',
                'environment' => $leader['environment'] ?? '',
                'component' => $leader['component'] ?? 'APPLICATION_LOGIC',
                'level' => $leader['level'] ?? 'ERROR',
                'event_type' => $leader['event_type'] ?? 'APPLICATION_ERROR',
                'topics' => $leader['topics'] ?? [],
                'window_ms' => $leader['window_ms'] ?? $windowMs,
                'delta_count' => 0,
                'last_seen_at' => $lastSeenAt,
            ];
        }

        $existing['delta_count'] = ((int) ($existing['delta_count'] ?? 0)) + max(1, $deltaCount);
        $existing['last_seen_at'] = $lastSeenAt;

        self::cachePut($cntKey, $existing, $ttlSeconds);

        // Register in pending index so trailing count flushes once the window expires
        $index = self::cacheGet(self::PENDING_INDEX_KEY);
        if (! is_array($index)) {
            $index = [];
        }
        $index[$fingerprint] = (int) ($leader['expires_at_ms'] ?? (self::nowMs() + $windowMs));
        if (count($index) > 100) {
            $index = array_slice($index, -100, null, true);
        }
        self::cachePut(self::PENDING_INDEX_KEY, $index, 60);
    }

    protected static function removeFromPendingIndex(string $fingerprint): void
    {
        $index = self::cacheGet(self::PENDING_INDEX_KEY);
        if (! is_array($index) || ! isset($index[$fingerprint])) {
            return;
        }
        unset($index[$fingerprint]);
        if (empty($index)) {
            self::cacheForget(self::PENDING_INDEX_KEY);
        } else {
            self::cachePut(self::PENDING_INDEX_KEY, $index, 60);
        }
    }

    /**
     * @param  array<int, string>  $topics
     * @param  array<string, mixed>  $payload
     */
    protected static function publishPayload(array $topics, array $payload, bool $fromQueue = false): void
    {
        if (self::$publisher !== null) {
            (self::$publisher)($topics, $payload);

            return;
        }

        LoggerService::publishToTopics($topics, $payload, $fromQueue);
    }

    protected static function registerShutdownFlush(): void
    {
        if (self::$shutdownRegistered) {
            return;
        }

        self::$shutdownRegistered = true;

        register_shutdown_function(static function (): void {
            self::flush();
        });
    }

    protected static function cacheGet(string $key, mixed $default = null): mixed
    {
        try {
            if (class_exists('\Illuminate\Support\Facades\Cache')) {
                return Cache::get($key, $default);
            }
        } catch (Throwable) {
        }

        return $default;
    }

    protected static function cachePut(string $key, mixed $value, int $seconds): void
    {
        try {
            if (class_exists('\Illuminate\Support\Facades\Cache')) {
                Cache::put($key, $value, $seconds);
            }
        } catch (Throwable) {
        }
    }

    protected static function cacheAdd(string $key, mixed $value, int $seconds): bool
    {
        try {
            if (class_exists('\Illuminate\Support\Facades\Cache')) {
                return (bool) Cache::add($key, $value, $seconds);
            }
        } catch (Throwable) {
        }

        return false;
    }

    protected static function cachePull(string $key): mixed
    {
        try {
            if (class_exists('\Illuminate\Support\Facades\Cache')) {
                return Cache::pull($key);
            }
        } catch (Throwable) {
        }

        return null;
    }

    protected static function cacheForget(string $key): void
    {
        try {
            if (class_exists('\Illuminate\Support\Facades\Cache')) {
                Cache::forget($key);
            }
        } catch (Throwable) {
        }
    }
}
