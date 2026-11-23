<?php

namespace Mimon\KafkaLogger\Support;

use Illuminate\Support\Facades\Cache;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Throwable;

class PulseUsageBuffer
{
    public const STATE_CACHE_KEY = 'mimon_kafka_logger:pulse:usage_state';

    public const FLUSH_LOCK_KEY = 'mimon_kafka_logger:pulse:usage_flush_lock';

    /** @var array<string, mixed>|null */
    protected static ?array $memoryState = null;

    protected static bool $bypassCacheEvents = false;

    public static function isInternalOperation(): bool
    {
        return self::$bypassCacheEvents;
    }

    public static function recordRequest(string $userId, ?string $userLabel = null, bool $isSlow = false): void
    {
        self::mutate(function (array &$state) use ($userId, $userLabel, $isSlow): void {
            $state['total_requests']++;

            if ($userId !== '' && $userId !== 'GUEST' && $userId !== 'SYSTEM') {
                if (! isset($state['users'][$userId])) {
                    if (count($state['users']) >= 50) {
                        return;
                    }
                    $state['users'][$userId] = [
                        'user_id' => $userId,
                        'label' => $userLabel ?: $userId,
                        'requests' => 0,
                        'slow_requests' => 0,
                    ];
                }
                $state['users'][$userId]['requests']++;
                if ($isSlow) {
                    $state['users'][$userId]['slow_requests']++;
                }
            }
        });
    }

    public static function recordCache(string $key, bool $hit): void
    {
        $prefix = self::normalizeCacheKeyPrefix($key);
        if ($prefix === null) {
            return;
        }

        self::mutate(function (array &$state) use ($prefix, $hit): void {
            if ($hit) {
                $state['cache_hits']++;
            } else {
                $state['cache_misses']++;
            }

            if (! isset($state['cache_keys'][$prefix])) {
                if (count($state['cache_keys']) >= 50) {
                    return;
                }
                $state['cache_keys'][$prefix] = [
                    'key' => $prefix,
                    'hits' => 0,
                    'misses' => 0,
                ];
            }

            if ($hit) {
                $state['cache_keys'][$prefix]['hits']++;
            } else {
                $state['cache_keys'][$prefix]['misses']++;
            }
        });
    }

    public static function recordJobQueued(): void
    {
        self::mutate(static function (array &$state): void {
            $state['jobs_queued']++;
        });
    }

    public static function recordJobProcessed(): void
    {
        self::mutate(static function (array &$state): void {
            $state['jobs_processed']++;
        });
    }

    public static function recordJobFailed(): void
    {
        self::mutate(static function (array &$state): void {
            $state['jobs_failed']++;
        });
    }

    /**
     * Flush accumulated usage/cache/queue counts at most once per configured interval (default 60s).
     */
    public static function maybeFlush(): ?array
    {
        try {
            if (! self::isEnabled()) {
                return null;
            }

            $interval = max(15, (int) PayloadSanitizer::config('kafka-logger.pulse.usage_rollup.flush_interval_seconds', 60));

            if (! self::acquireFlushLock($interval)) {
                return null;
            }

            return self::flushNow($interval);
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * Drain buffered counters and emit 1 summary PULSE_USAGE_ROLLUP event.
     */
    public static function flushNow(int $windowSeconds = 60): ?array
    {
        $state = self::pullState();

        $hasActivity = ($state['total_requests'] + $state['cache_hits'] + $state['cache_misses']
            + $state['jobs_queued'] + $state['jobs_processed'] + $state['jobs_failed']) > 0;

        if (! $hasActivity) {
            return null;
        }

        $topUsers = array_values($state['users']);
        usort($topUsers, static fn (array $a, array $b): int => $b['requests'] <=> $a['requests']);
        $topUsers = array_slice($topUsers, 0, 10);

        $topCacheKeys = array_values($state['cache_keys']);
        usort($topCacheKeys, static fn (array $a, array $b): int => ($b['hits'] + $b['misses']) <=> ($a['hits'] + $a['misses']));
        $topCacheKeys = array_slice($topCacheKeys, 0, 10);

        $payload = LoggerService::createEnterpriseLog(
            eventType: 'PULSE_USAGE_ROLLUP',
            message: "Pulse usage rollup ({$windowSeconds}s): {$state['total_requests']} reqs, {$state['cache_hits']}/{$state['cache_misses']} cache hit/miss, {$state['jobs_processed']} jobs.",
            level: LogLevel::INFO,
            component: ComponentSubsystem::APPLICATION_LOGIC,
            userId: 'SYSTEM',
            userRole: 'SYSTEM',
            metadata: [
                'window_seconds' => $windowSeconds,
                'total_requests' => (int) $state['total_requests'],
                'cache_hits' => (int) $state['cache_hits'],
                'cache_misses' => (int) $state['cache_misses'],
                'jobs_queued' => (int) $state['jobs_queued'],
                'jobs_processed' => (int) $state['jobs_processed'],
                'jobs_failed' => (int) $state['jobs_failed'],
                'top_users' => $topUsers,
                'top_cache_keys' => $topCacheKeys,
                'host' => gethostname() ?: 'unknown-host',
            ],
        );

        try {
            LoggerService::dispatchEnterpriseLog($payload);
        } catch (Throwable) {
            // Never disrupt client requests if dispatch fails
        }

        return $payload;
    }

    public static function reset(): void
    {
        self::$memoryState = null;

        try {
            self::$bypassCacheEvents = true;
            Cache::forget(self::STATE_CACHE_KEY);
            Cache::forget(self::FLUSH_LOCK_KEY);
        } catch (Throwable) {
        } finally {
            self::$bypassCacheEvents = false;
        }
    }

    public static function normalizeCacheKeyPrefix(string $key): ?string
    {
        $trimmed = trim($key);
        if (
            $trimmed === ''
            || str_contains($trimmed, 'mimon_kafka_logger:')
            || str_contains($trimmed, 'cls:app_registry:')
        ) {
            return null;
        }

        // Strip common numeric/UUID suffixes to group key families cleanly (e.g. "users:123" -> "users:*")
        $normalized = preg_replace('/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i', '*', $trimmed) ?? $trimmed;
        $normalized = preg_replace('/(?<=[:._\/-])\d+(?=$|[:._\/-])/', '*', $normalized) ?? $normalized;

        return substr($normalized, 0, 120);
    }

    protected static function isEnabled(): bool
    {
        return (bool) PayloadSanitizer::config('kafka-logger.enabled', true)
            && (bool) PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
            && (bool) PayloadSanitizer::config('kafka-logger.pulse.usage_rollup.enabled', true);
    }

    protected static function emptyState(): array
    {
        return [
            'total_requests' => 0,
            'cache_hits' => 0,
            'cache_misses' => 0,
            'jobs_queued' => 0,
            'jobs_processed' => 0,
            'jobs_failed' => 0,
            'users' => [],
            'cache_keys' => [],
        ];
    }

    protected static function mutate(callable $callback): void
    {
        try {
            if (! self::isEnabled()) {
                return;
            }

            self::$bypassCacheEvents = true;

            try {
                $state = Cache::get(self::STATE_CACHE_KEY);
                if (! is_array($state)) {
                    $state = self::emptyState();
                }
                $callback($state);
                Cache::put(self::STATE_CACHE_KEY, $state, 300);
            } catch (Throwable) {
                $state = self::$memoryState ?? self::emptyState();
                $callback($state);
                self::$memoryState = $state;
            }
        } catch (Throwable) {
        } finally {
            self::$bypassCacheEvents = false;
        }
    }

    protected static function pullState(): array
    {
        self::$bypassCacheEvents = true;

        try {
            $state = Cache::pull(self::STATE_CACHE_KEY);
            if (is_array($state)) {
                return array_merge(self::emptyState(), $state);
            }
        } catch (Throwable) {
        } finally {
            self::$bypassCacheEvents = false;
        }

        $state = self::$memoryState ?? self::emptyState();
        self::$memoryState = null;

        return array_merge(self::emptyState(), $state);
    }

    protected static function acquireFlushLock(int $intervalSeconds): bool
    {
        self::$bypassCacheEvents = true;

        try {
            return (bool) Cache::add(self::FLUSH_LOCK_KEY, time(), $intervalSeconds);
        } catch (Throwable) {
            static $lastFlushedAt = 0;
            $now = time();
            if (($now - $lastFlushedAt) < $intervalSeconds) {
                return false;
            }
            $lastFlushedAt = $now;

            return true;
        } finally {
            self::$bypassCacheEvents = false;
        }
    }
}
