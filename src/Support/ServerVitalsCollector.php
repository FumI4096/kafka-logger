<?php

namespace Mimon\KafkaLogger\Support;

use Illuminate\Support\Facades\Cache;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Throwable;

class ServerVitalsCollector
{
    public const HEARTBEAT_LOCK_KEY = 'mimon_kafka_logger:pulse:heartbeat_lock';

    /**
     * Emit a PULSE_SERVER_HEARTBEAT at most once per configured interval (default 60s).
     * Piggybacked automatically on normal HTTP requests — no scheduler or cron needed.
     */
    public static function maybeEmitHeartbeat(): ?array
    {
        try {
            if (! self::isEnabled()) {
                return null;
            }

            $interval = max(15, (int) PayloadSanitizer::config('kafka-logger.pulse.server_heartbeat.interval_seconds', 60));

            if (! self::acquireThrottleLock($interval)) {
                return null;
            }

            return self::emitHeartbeat();
        } catch (Throwable) {
            return null;
        }
    }

    /**
     * Collect server vitals and dispatch a PULSE_SERVER_HEARTBEAT payload immediately.
     */
    public static function emitHeartbeat(): array
    {
        $vitals = self::collect();
        $host = (string) ($vitals['host'] ?? 'unknown-host');
        $cpu = $vitals['cpu_percent'];
        $memUsed = $vitals['memory_used_mb'];
        $memTotal = $vitals['memory_total_mb'];

        $payload = LoggerService::createEnterpriseLog(
            eventType: 'PULSE_SERVER_HEARTBEAT',
            message: "Server vitals heartbeat from {$host} (CPU {$cpu}%, RAM {$memUsed}/{$memTotal} MB).",
            level: LogLevel::INFO,
            component: ComponentSubsystem::APPLICATION_LOGIC,
            userId: 'SYSTEM',
            userRole: 'SYSTEM',
            metadata: $vitals,
        );

        try {
            LoggerService::dispatchEnterpriseLog($payload);
        } catch (Throwable) {
            // Never disrupt client app requests if dispatch fails
        }

        return $payload;
    }

    /**
     * Gather portable Linux/container server vitals (CPU, Memory, Disk, Runtime).
     *
     * @return array<string, mixed>
     */
    public static function collect(): array
    {
        $host = gethostname() ?: (php_uname('n') ?: 'unknown-host');
        $cores = self::detectCpuCores();
        $loadAvg1m = self::readLoadAverage1m();
        $cpuPercent = $cores > 0
            ? round(min(100.0, ($loadAvg1m / $cores) * 100.0), 2)
            : 0.0;

        [$memUsedMb, $memTotalMb] = self::readMemoryUsageMb();
        [$diskUsedGb, $diskTotalGb] = self::readDiskUsageGb();

        $laravelVersion = null;
        if (function_exists('app')) {
            try {
                $laravelVersion = app()->version();
            } catch (Throwable) {
                $laravelVersion = null;
            }
        }

        return [
            'host' => $host,
            'cpu_cores' => $cores,
            'load_avg_1m' => round($loadAvg1m, 2),
            'cpu_percent' => $cpuPercent,
            'memory_used_mb' => $memUsedMb,
            'memory_total_mb' => $memTotalMb,
            'disk_used_gb' => $diskUsedGb,
            'disk_total_gb' => $diskTotalGb,
            'php_version' => PHP_VERSION,
            'laravel_version' => $laravelVersion,
            'logger_version' => LoggerService::resolvePackageVersion(),
        ];
    }

    protected static function isEnabled(): bool
    {
        return (bool) PayloadSanitizer::config('kafka-logger.enabled', true)
            && (bool) PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
            && (bool) PayloadSanitizer::config('kafka-logger.pulse.server_heartbeat.enabled', true);
    }

    protected static function acquireThrottleLock(int $intervalSeconds): bool
    {
        try {
            return (bool) Cache::add(self::HEARTBEAT_LOCK_KEY, now()->timestamp, $intervalSeconds);
        } catch (Throwable) {
            // Fallback to process-local static guard if Cache facade is unavailable
            static $lastSentAt = 0;
            $now = time();
            if (($now - $lastSentAt) < $intervalSeconds) {
                return false;
            }
            $lastSentAt = $now;

            return true;
        }
    }

    protected static function detectCpuCores(): int
    {
        if (is_readable('/proc/cpuinfo')) {
            $cpuinfo = @file_get_contents('/proc/cpuinfo');
            if (is_string($cpuinfo)) {
                $count = substr_count($cpuinfo, 'processor');
                if ($count > 0) {
                    return $count;
                }
            }
        }

        return 1;
    }

    protected static function readLoadAverage1m(): float
    {
        if (function_exists('sys_getloadavg')) {
            $load = @sys_getloadavg();
            if (is_array($load) && isset($load[0])) {
                return (float) $load[0];
            }
        }

        if (is_readable('/proc/loadavg')) {
            $raw = @file_get_contents('/proc/loadavg');
            if (is_string($raw)) {
                $parts = preg_split('/\s+/', trim($raw));
                if (isset($parts[0]) && is_numeric($parts[0])) {
                    return (float) $parts[0];
                }
            }
        }

        return 0.0;
    }

    /**
     * @return array{0: int, 1: int} [used_mb, total_mb]
     */
    protected static function readMemoryUsageMb(): array
    {
        if (is_readable('/proc/meminfo')) {
            $raw = @file_get_contents('/proc/meminfo');
            if (is_string($raw)) {
                $totalKb = null;
                $availableKb = null;
                $freeKb = null;

                if (preg_match('/^MemTotal:\s+(\d+)\s+kB/m', $raw, $m)) {
                    $totalKb = (int) $m[1];
                }
                if (preg_match('/^MemAvailable:\s+(\d+)\s+kB/m', $raw, $m)) {
                    $availableKb = (int) $m[1];
                }
                if (preg_match('/^MemFree:\s+(\d+)\s+kB/m', $raw, $m)) {
                    $freeKb = (int) $m[1];
                }

                if ($totalKb !== null && $totalKb > 0) {
                    $avail = $availableKb ?? $freeKb ?? 0;
                    $usedKb = max(0, $totalKb - $avail);

                    return [
                        (int) round($usedKb / 1024),
                        (int) round($totalKb / 1024),
                    ];
                }
            }
        }

        $phpUsedMb = (int) round(memory_get_usage(true) / 1048576);

        return [$phpUsedMb, max($phpUsedMb, 256)];
    }

    /**
     * @return array{0: float, 1: float} [used_gb, total_gb]
     */
    protected static function readDiskUsageGb(): array
    {
        $path = (string) PayloadSanitizer::config('kafka-logger.pulse.server_heartbeat.disk_path', '/');
        if ($path === '' || ! @is_dir($path)) {
            $path = DIRECTORY_SEPARATOR;
        }

        $total = @disk_total_space($path);
        $free = @disk_free_space($path);

        if (! is_numeric($total) || ! is_numeric($free) || $total <= 0) {
            return [0.0, 0.0];
        }

        $used = max(0.0, (float) $total - (float) $free);

        return [
            round($used / 1073741824, 2),
            round((float) $total / 1073741824, 2),
        ];
    }
}
