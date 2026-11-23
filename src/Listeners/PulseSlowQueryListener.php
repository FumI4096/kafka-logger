<?php

namespace Mimon\KafkaLogger\Listeners;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Throwable;

class PulseSlowQueryListener
{
    protected static bool $recording = false;

    public function handle(object $event): ?array
    {
        $sql = isset($event->sql) ? (string) $event->sql : '';
        $timeMs = isset($event->time) ? (float) $event->time : 0.0;
        $connection = isset($event->connectionName) ? (string) $event->connectionName : 'default';

        return $this->recordQuery($sql, $timeMs, $connection);
    }

    public function recordQuery(string $sql, float $timeMs, string $connection = 'default'): ?array
    {
        if (self::$recording) {
            return null;
        }

        try {
            self::$recording = true;

            if (
                ! PayloadSanitizer::config('kafka-logger.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.slow_queries.enabled', true)
            ) {
                return null;
            }

            $thresholdMs = max(1, (int) PayloadSanitizer::config('kafka-logger.pulse.slow_queries.threshold_ms', 500));
            $durationMs = (int) round($timeMs);

            if ($durationMs < $thresholdMs) {
                return null;
            }

            $normalizedSql = self::normalizeSql($sql);
            if ($normalizedSql === '' || self::shouldIgnoreSql($normalizedSql)) {
                return null;
            }

            $maxLen = max(100, (int) PayloadSanitizer::config('kafka-logger.pulse.slow_queries.max_query_length', 1000));
            if (strlen($normalizedSql) > $maxLen) {
                $normalizedSql = substr($normalizedSql, 0, $maxLen) . '...';
            }

            $caller = self::resolveApplicationCaller();

            $payload = LoggerService::createEnterpriseLog(
                eventType: 'PULSE_SLOW_QUERY',
                message: "Slow SQL query on [{$connection}] took {$durationMs}ms.",
                level: $durationMs >= ($thresholdMs * 3) ? LogLevel::WARNING : LogLevel::INFO,
                component: ComponentSubsystem::DATABASE,
                metadata: [
                    'signature_hash' => sha1($connection . ':' . $normalizedSql),
                    'normalized_sql' => $normalizedSql,
                    'connection' => $connection,
                    'duration_ms' => $durationMs,
                    'threshold_ms' => $thresholdMs,
                    'caller_file' => $caller['file'],
                    'caller_line' => $caller['line'],
                    'host' => gethostname() ?: 'unknown-host',
                ],
            );

            try {
                LoggerService::dispatchEnterpriseLog($payload);
            } catch (Throwable) {
                // Never disrupt client app queries if dispatch fails
            }

            return $payload;
        } catch (Throwable) {
            return null;
        } finally {
            self::$recording = false;
        }
    }

    /**
     * Normalize SQL while preserving ? placeholders and never including bound parameter values.
     */
    public static function normalizeSql(string $sql): string
    {
        $collapsed = preg_replace('/\s+/', ' ', trim($sql));

        return is_string($collapsed) ? $collapsed : trim($sql);
    }

    protected static function shouldIgnoreSql(string $sql): bool
    {
        $lower = strtolower($sql);

        // Prevent telemetry feedback loops on CLS hub tables
        return str_contains($lower, 'system_logs')
            || str_contains($lower, 'pulse_slow_events')
            || str_contains($lower, 'pulse_server_heartbeats')
            || str_contains($lower, 'kafka_pipeline_metrics');
    }

    /**
     * Find the first non-vendor application file:line in the stack trace.
     *
     * @return array{file: ?string, line: ?int}
     */
    protected static function resolveApplicationCaller(): array
    {
        $frames = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 30);
        $basePath = function_exists('base_path') ? str_replace('\\', '/', base_path()) : '';

        foreach ($frames as $frame) {
            if (empty($frame['file'])) {
                continue;
            }

            $file = str_replace('\\', '/', (string) $frame['file']);
            if (
                str_contains($file, '/vendor/')
                || str_contains($file, '/packages/mimon/kafka-logger/')
            ) {
                continue;
            }

            if ($basePath !== '' && str_starts_with($file, $basePath . '/')) {
                $file = substr($file, strlen($basePath) + 1);
            }

            return [
                'file' => $file,
                'line' => isset($frame['line']) ? (int) $frame['line'] : null,
            ];
        }

        return ['file' => null, 'line' => null];
    }
}
