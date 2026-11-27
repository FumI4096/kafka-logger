<?php

namespace Mimon\KafkaLogger\Support;

use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Cache;
use Mimon\KafkaLogger\Jobs\FlushCoalescedErrorJob;
use Mimon\KafkaLogger\Services\LoggerService;
use Throwable;

/**
 * Coalesces identical ERROR/CRITICAL logs within a sliding burst window (default 1500ms,
 * max continuous burst span 60s) so only the ERROR/CRITICAL record itself is emitted carrying
 * accurate occurrence counts — never fabricating separate INFO control logs.
 */
class ErrorBurstDeduplicator
{
    /**
     * @deprecated Retained solely so CLS consumers can silently drop legacy v1.2.8 control events.
     */
    public const COUNT_UPDATE_EVENT = 'ERROR_BURST_COUNT_UPDATE';

    public const CACHE_PREFIX = 'mimon_kafka_logger:err_dedup:';

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
     * Normalize a log message so dynamic SQL tails, IDs, UUIDs, emails, IPs, hex tokens,
     * quoted literals, and timestamps share a single deterministic fingerprint.
     */
    public static function normalizeMessage(string $message): string
    {
        // Normalize Laravel QueryException SQL suffixes so identical DB errors across queries share one signature
        $normalized = preg_replace(
            '/\(Connection:\s*[^,]+,\s*SQL:\s*.*\)\s*$/si',
            '(Connection: <conn>, SQL: <sql>)',
            $message
        ) ?? $message;

        $normalized = preg_replace(
            '/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i',
            '<uuid>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b/',
            '<ts>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b/i',
            '<email>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b/',
            '<ip>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/\b(?:0x)?[0-9a-f]{16,}\b/i',
            '<hex>',
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            "/\'[^\'<>]{1,80}\'/",
            "'<str>'",
            $normalized
        ) ?? $normalized;

        $normalized = preg_replace(
            '/"[^"<>]{1,80}"/',
            '"<str>"',
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

    public static function maxBurstMs(): int
    {
        return max(
            self::windowMs(),
            (int) PayloadSanitizer::config('kafka-logger.error_dedup.max_burst_ms', 60000)
        );
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
     * Capture an ERROR/CRITICAL payload into the sliding burst coalescer.
     *
     * Returns:
     *   - handled: false when dedup is disabled or level is excluded (caller publishes immediately)
     *   - duplicate: true when collapsed into an existing active burst (returns empty payload to caller)
     *   - payload: enriched representative ERROR/CRITICAL payload when this is the first occurrence in the window
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
     * @return array<int, array<string, mixed>> Published leader ERROR/CRITICAL payloads
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
        } catch (Throwable) {
            // Never disrupt request termination
        } finally {
            self::$isFlushing = false;
        }

        return $emitted;
    }

    /**
     * Flush only in-process entries whose sliding inactivity gap or max burst span has elapsed.
     */
    public static function flushExpiredInProcess(?int $nowMs = null, ?int $windowMs = null): void
    {
        if (empty(self::$inProcessBuffer) || self::$isFlushing) {
            return;
        }

        $nowMs ??= self::nowMs();
        $windowMs ??= self::windowMs();
        $maxBurstMs = self::maxBurstMs();

        $expired = [];
        foreach (self::$inProcessBuffer as $fingerprint => $entry) {
            $idleElapsed = $nowMs - $entry['last_seen_at_ms'];
            $totalSpan = $nowMs - $entry['first_seen_at_ms'];

            if ($idleElapsed >= $windowMs || $totalSpan >= $maxBurstMs) {
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
     * Called by FlushCoalescedErrorJob after the burst delay when async queue mode is active.
     * Emits a single ERROR/CRITICAL payload carrying the final combined cross-process count.
     */
    public static function flushDelayedJobEntry(string $fingerprint): ?array
    {
        $winKey = self::CACHE_PREFIX . "win:{$fingerprint}";
        $bufKey = self::CACHE_PREFIX . "buf:{$fingerprint}";
        $cntKey = self::CACHE_PREFIX . "cnt:{$fingerprint}";
        $lastKey = self::CACHE_PREFIX . "last:{$fingerprint}";

        $buffered = self::cachePull($bufKey);
        $extraCount = max(0, (int) self::cachePull($cntKey));
        $extraLastSeen = self::cachePull($lastKey);
        self::cacheForget($winKey);

        if (! is_array($buffered) || empty($buffered['payload']) || empty($buffered['topics'])) {
            return null;
        }

        $lastSeenAt = (is_string($extraLastSeen) && $extraLastSeen !== '')
            ? $extraLastSeen
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

        if (LoggerService::shouldSuppressBySustainedRateLimit($payload, $fingerprint)) {
            return null;
        }

        self::publishPayload($buffered['topics'], $payload, fromQueue: true);

        return $payload;
    }

    /**
     * @deprecated No-op since v1.2.9. Cross-process counts are folded directly onto the ERROR/CRITICAL
     *             record (via FlushCoalescedErrorJob in async mode or CLS consumer coalescing in sync mode);
     *             separate INFO control events are never emitted.
     */
    public static function flushExpiredCrossProcessCounts(): int
    {
        return 0;
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
     * Publish the single ERROR/CRITICAL record (or fold cross-process in delayed-job async mode).
     * Never creates separate INFO control messages.
     *
     * @param  array<string, mixed>  $entry
     */
    protected static function emitOrCoalesceCrossProcess(string $fingerprint, array $entry): ?array
    {
        $windowMs = self::windowMs();
        $payload = $entry['payload'];
        $topics = $entry['topics'];
        $count = max(1, (int) $entry['count']);

        $flushMode = strtolower((string) PayloadSanitizer::config('kafka-logger.error_dedup.flush_mode', 'auto'));
        $isAsync = (bool) PayloadSanitizer::config('kafka-logger.async', false);
        $useDelayedJob = ($flushMode === 'delayed_job' || ($flushMode === 'auto' && $isAsync))
            && function_exists('dispatch');

        if ($useDelayedJob) {
            $ttlSeconds = max(15, (int) ceil($windowMs / 1000) * 5);
            $winKey = self::CACHE_PREFIX . "win:{$fingerprint}";
            $bufKey = self::CACHE_PREFIX . "buf:{$fingerprint}";
            $cntKey = self::CACHE_PREFIX . "cnt:{$fingerprint}";
            $lastKey = self::CACHE_PREFIX . "last:{$fingerprint}";

            // Native Cache::add works identically across phpredis, predis, database, file, and array stores.
            $acquired = self::cacheAdd($winKey, 1, max(2, (int) ceil($windowMs / 1000) + 1));

            if (! $acquired) {
                // Another worker already owns the active delayed window: increment scalar counter atomically.
                self::cacheIncrement($cntKey, $count, $ttlSeconds);
                self::cachePut($lastKey, (string) $entry['last_seen_at'], $ttlSeconds);

                return null;
            }

            self::cachePut($bufKey, [
                'payload' => $payload,
                'topics' => $topics,
                'count' => $count,
                'window_ms' => $windowMs,
                'first_seen_at' => $entry['first_seen_at'],
                'last_seen_at' => $entry['last_seen_at'],
            ], $ttlSeconds);

            try {
                dispatch((new FlushCoalescedErrorJob($fingerprint))->delay(Carbon::now()->addMilliseconds($windowMs)));

                return $payload;
            } catch (Throwable) {
                // Queue unavailable -> clean up window and fall through to immediate ERROR/CRITICAL publish
                self::cacheForget($bufKey);
                self::cacheForget($winKey);
            }
        }

        // Sync mode (or queue fallback): enrich the ERROR/CRITICAL record itself with the in-process count
        // and publish it directly. Concurrent PHP-FPM workers are folded into 1 row by CLS LogConsumerHandler.
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

        return true;
    }

    protected static function cacheIncrement(string $key, int $amount, int $seconds): void
    {
        try {
            if (! class_exists('\Illuminate\Support\Facades\Cache')) {
                return;
            }

            Cache::add($key, 0, $seconds);
            Cache::increment($key, max(1, $amount));
        } catch (Throwable) {
        }
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
