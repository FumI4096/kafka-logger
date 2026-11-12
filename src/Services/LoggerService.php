<?php

namespace Mimon\KafkaLogger\Services;

use Illuminate\Support\Carbon;
use Illuminate\Support\Str;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Jobs\SendKafkaLogJob;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Mimon\KafkaLogger\Support\SafeTrace;

class LoggerService
{
    public const CIRCUIT_CACHE_KEY = 'mimon_kafka_logger:circuit_open';

    /**
     * In-memory sliding window samples for producer throughput calculation: [['ts_ms' => int, 'bytes' => int], ...]
     *
     * @var array<int, array{ts_ms: int, bytes: int}>
     */
    protected static array $throughputWindow = [];

    public static function createEnterpriseLog(
        ?string $appCode = null,
        string $eventType = 'SYSTEM_EVENT',
        string $message = '',
        LogLevel|string $level = LogLevel::INFO,
        ComponentSubsystem|string $component = ComponentSubsystem::APPLICATION_LOGIC,
        ?string $topic = null,
        ?string $userId = null,
        ?string $userRole = null,
        ?array $metadata = null,
        ?\Throwable $exception = null,
        Environment|string|null $environment = null,
        ?string $appName = null
    ): array {
        $resolvedAppCode = $appCode ?? $appName ?? (string) PayloadSanitizer::config('kafka-logger.app_code', PayloadSanitizer::config('kafka-logger.app_name', 'HR_APP'));
        $resolvedAppName = $appName ?? $appCode ?? $resolvedAppCode;
        $logId = (string) Str::uuid();
        $traceId = static::resolveTraceId(static::requestHeader('x-trace-id'));
        $spanId = 'sp-' . Str::random(6);

        $resolvedEnv = is_string($environment)
            ? (Environment::tryFrom(strtoupper($environment)) ?? strtoupper($environment))
            : ($environment ?? (Environment::tryFrom(strtoupper((string) PayloadSanitizer::config('kafka-logger.environment', 'LOCAL'))) ?? Environment::LOCAL));

        $resolvedLevel = is_string($level)
            ? (LogLevel::tryFrom(strtoupper($level)) ?? strtoupper($level))
            : $level;

        $resolvedComponent = is_string($component)
            ? (ComponentSubsystem::tryFrom(strtoupper($component)) ?? strtoupper($component))
            : $component;

        $rawTopic = $topic ?? PayloadSanitizer::config('kafka-logger.default_topic', 'YOUR_TOPIC');
        $formattedTopic = substr((string) preg_replace('/[^a-zA-Z0-9._-]/', '_', $rawTopic), 0, 249) ?: 'YOUR_TOPIC';

        $cleanMessage = PayloadSanitizer::redactString($message);
        if (trim($cleanMessage) === '') {
            $cleanMessage = str_replace(['.', '-', '_'], ' ', $eventType) . ' event.';
        }

        $now = Carbon::now();
        $createdAtMs = (int) $now->getPreciseTimestamp(3);

        $errorCode = null;
        $stackTrace = null;

        if ($exception) {
            $errorCode = (string) ($exception->getCode() ?: 'UNKNOWN_ERROR');
            // Never getTraceAsString(): it can include function arguments.
            $stackTrace = SafeTrace::format(
                $exception,
                (int) PayloadSanitizer::config('kafka-logger.max_trace_frames', 25)
            );
        }

        $producedNow = Carbon::now();
        $producedAtMs = max($createdAtMs, (int) $producedNow->getPreciseTimestamp(3));
        $produceLatencyMs = max(0, $producedAtMs - $createdAtMs);

        $payload = [
            'log_id' => $logId,
            'environment' => is_string($resolvedEnv) ? $resolvedEnv : $resolvedEnv->value,
            'application' => [
                'code' => strtoupper(str_replace(['-', ' '], '_', $resolvedAppCode)),
                'name' => strtoupper(str_replace(['-', ' '], '_', $resolvedAppName)),
                'component' => is_string($resolvedComponent) ? $resolvedComponent : $resolvedComponent->value,
                'host_ip' => static::hostIp(),
                'host_name' => gethostname() ?: 'unknown-host',
            ],
            'event' => [
                'type' => strtoupper(str_replace(['.', '-'], '_', $eventType)),
                'level' => is_string($resolvedLevel) ? $resolvedLevel : $resolvedLevel->value,
                'topic' => $formattedTopic,
                'message' => $cleanMessage,
            ],
            'trace' => [
                'trace_id' => $traceId,
                'span_id' => $spanId,
            ],
            'actor' => [
                'user_id' => $userId ?? 'usr_system',
                'user_role' => $userRole ?: 'SYSTEM',
                'client_ip' => static::clientIp(),
            ],
            'timestamps' => [
                'created_at' => $now->toIso8601String(),
                'produced_at' => $producedNow->toIso8601String(),
                'created_at_ms' => $createdAtMs,
                'produced_at_ms' => $producedAtMs,
                'produce_latency_ms' => $produceLatencyMs,
            ],
            'error' => [
                'code' => $errorCode,
                'stack_trace' => $stackTrace,
            ],
            'metadata' => PayloadSanitizer::redactArray($metadata ?? []),
        ];

        $payload = static::enforceSizeLimit(
            $payload,
            (int) PayloadSanitizer::config('kafka-logger.max_payload_bytes', 900000)
        );

        $payload['telemetry'] = static::computeProducerTelemetry($payload, $producedAtMs);

        if (PayloadSanitizer::config('kafka-logger.auto_validate', true)) {
            $errors = PayloadValidator::validate($payload);
            if (! empty($errors)) {
                if ((bool) PayloadSanitizer::config('kafka-logger.strict_validation', false)) {
                    throw new \InvalidArgumentException('Kafka Log Payload validation failed: ' . implode(' ', $errors));
                }

                static::warn('payload dropped, validation failed: ' . implode(' ', $errors));

                return [];
            }
        }

        static::publish($formattedTopic, $payload, [
            'x-trace-id' => $traceId,
            'x-log-id' => $logId,
        ]);

        return $payload;
    }

    /**
     * Publish to Kafka. Never throws, never blocks for long:
     * short producer timeouts + a circuit breaker that skips the broker after a failure.
     */
    public static function publish(string $topic, array $payload, array $headers = [], bool $fromQueue = false): void
    {
        if (! PayloadSanitizer::config('kafka-logger.produce_to_kafka', true)
            || ! class_exists('\Junges\Kafka\Facades\Kafka')) {
            return;
        }

        if (static::circuitIsOpen()) {
            return;
        }

        if (! $fromQueue && PayloadSanitizer::config('kafka-logger.async', false) && function_exists('dispatch')) {
            try {
                dispatch(new SendKafkaLogJob($topic, $payload, $headers));

                return;
            } catch (\Throwable $e) {
                // Queue unavailable: fall back to inline publish below
            }
        }

        try {
            $producer = method_exists('\Junges\Kafka\Facades\Kafka', 'publishOn')
                ? \Junges\Kafka\Facades\Kafka::publishOn($topic)
                : \Junges\Kafka\Facades\Kafka::publish()->onTopic($topic);

            $timeoutMs = max(100, (int) PayloadSanitizer::config('kafka-logger.producer_timeout_ms', 1500));
            if (method_exists($producer, 'withConfigOptions')) {
                $producer = $producer->withConfigOptions([
                    'message.timeout.ms' => (string) $timeoutMs,
                    'request.timeout.ms' => (string) $timeoutMs,
                    'socket.timeout.ms' => (string) min($timeoutMs, 1000),
                ]);
            }

            if (method_exists($producer, 'withFlushRetries')) {
                $producer = $producer->withFlushRetries(0);
            }
            if (method_exists($producer, 'withFlushTimeout')) {
                $producer = $producer->withFlushTimeout($timeoutMs);
            }

            // Refresh produced_at_ms right before broker send (captures queue delay when async=true)
            $publishNow = Carbon::now();
            $producedAtMs = (int) $publishNow->getPreciseTimestamp(3);
            $createdAtMs = (int) ($payload['timestamps']['created_at_ms'] ?? $producedAtMs);
            $payload['timestamps']['produced_at'] = $publishNow->toIso8601String();
            $payload['timestamps']['produced_at_ms'] = $producedAtMs;
            $payload['timestamps']['produce_latency_ms'] = max(0, $producedAtMs - $createdAtMs);

            $producer->withBody($payload)->withHeaders($headers)->send();
        } catch (\Throwable $e) {
            static::openCircuit();
            // WARNING level + prefix: ignored by LogErrorEvents, so this can never loop.
            static::warn('Kafka broker publish skipped: ' . PayloadSanitizer::redactString($e->getMessage()));
        }
    }

    /**
     * Compute producer-side telemetry: payload byte size, rolling events/sec, and rolling MB/sec.
     *
     * @return array{payload_bytes: int, producer_events_per_sec: float, producer_mb_per_sec: float}
     */
    public static function computeProducerTelemetry(array $payload, ?int $nowMs = null): array
    {
        $nowMs = $nowMs ?? (int) Carbon::now()->getPreciseTimestamp(3);
        $payloadBytes = strlen((string) json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_PARTIAL_OUTPUT_ON_ERROR));

        // Prune samples older than 10 seconds (10,000 ms)
        $cutoffMs = $nowMs - 10000;
        static::$throughputWindow = array_values(array_filter(
            static::$throughputWindow,
            static fn (array $sample): bool => $sample['ts_ms'] >= $cutoffMs
        ));

        static::$throughputWindow[] = [
            'ts_ms' => $nowMs,
            'bytes' => $payloadBytes,
        ];

        // Cap memory array to at most 1,000 recent samples
        if (count(static::$throughputWindow) > 1000) {
            static::$throughputWindow = array_slice(static::$throughputWindow, -1000);
        }

        $sampleCount = count(static::$throughputWindow);
        $totalBytes = array_sum(array_column(static::$throughputWindow, 'bytes'));
        $oldestMs = static::$throughputWindow[0]['ts_ms'] ?? $nowMs;
        $windowSec = max(1.0, ($nowMs - $oldestMs) / 1000.0);

        return [
            'payload_bytes' => $payloadBytes,
            'producer_events_per_sec' => round($sampleCount / $windowSec, 2),
            'producer_mb_per_sec' => round(($totalBytes / 1048576) / $windowSec, 4),
        ];
    }

    public static function resetThroughputWindow(): void
    {
        static::$throughputWindow = [];
    }

    /**
     * Keep payloads under Kafka's message.max.bytes (~1 MB default).
     */
    public static function enforceSizeLimit(array $payload, int $maxBytes): array
    {
        $size = static fn (array $p): int => strlen((string) json_encode($p, JSON_UNESCAPED_UNICODE | JSON_PARTIAL_OUTPUT_ON_ERROR));

        if ($size($payload) <= $maxBytes) {
            return $payload;
        }

        $message = (string) ($payload['event']['message'] ?? '');
        if (mb_strlen($message) > 2000) {
            $payload['event']['message'] = mb_substr($message, 0, 2000) . ' ...[truncated]';
        }

        foreach ([10, 3] as $frames) {
            if ($size($payload) <= $maxBytes || empty($payload['error']['stack_trace'])) {
                break;
            }
            $lines = explode("\n", (string) $payload['error']['stack_trace']);
            $payload['error']['stack_trace'] = implode("\n", array_slice($lines, 0, $frames + 1)) . "\n#... truncated";
        }

        if ($size($payload) > $maxBytes) {
            $payload['metadata'] = [
                '_truncated' => true,
                'original_bytes' => strlen((string) json_encode($payload['metadata'] ?? [], JSON_PARTIAL_OUTPUT_ON_ERROR)),
            ];
        }

        if ($size($payload) > $maxBytes) {
            $payload['error']['stack_trace'] = '[truncated]';
            $payload['event']['message'] = mb_substr((string) $payload['event']['message'], 0, 500) . ' ...[truncated]';
        }

        return $payload;
    }

    /**
     * Accept an upstream trace id only if it is a short, safe token; otherwise generate one.
     */
    public static function resolveTraceId(?string $header): string
    {
        if ($header !== null && preg_match('/^[A-Za-z0-9._-]{1,64}$/', $header)) {
            return $header;
        }

        return 'tr-' . Str::random(12);
    }

    protected static function requestHeader(string $name): ?string
    {
        try {
            if (function_exists('request') && request()) {
                $value = request()->header($name);

                return is_string($value) ? $value : null;
            }
        } catch (\Throwable $e) {
            // No request available
        }

        return null;
    }

    /**
     * Ensures that the client IP is always available, even in CLI or queue contexts.
     * 
     */
    protected static function clientIp(): string
    {
        try {
            if (function_exists('request') && request() && ! (function_exists('app') && app()->runningInConsole())) {
                return (string) (request()->ip() ?? '127.0.0.1');
            }
        } catch (\Throwable $e) {
            // ignore
        }

        return '127.0.0.1';
    }

    /**
     * Ensures that the host IP is always available, even in CLI or queue contexts.
     */
    protected static function hostIp(): string
    {
        $ip = $_SERVER['SERVER_ADDR'] ?? null;
        if (! $ip) {
            $resolved = gethostbyname((string) gethostname());
            $ip = $resolved !== gethostname() ? $resolved : null;
        }

        return $ip ?: '0.0.0.0';
    }

    protected static function circuitIsOpen(): bool
    {
        try {
            return function_exists('cache') && (bool) cache()->get(static::CIRCUIT_CACHE_KEY, false);
        } catch (\Throwable $e) {
            return false;
        }
    }

    protected static function openCircuit(): void
    {
        $seconds = (int) PayloadSanitizer::config('kafka-logger.circuit_breaker_seconds', 30);
        if ($seconds <= 0) {
            return;
        }

        try {
            if (function_exists('cache')) {
                cache()->put(static::CIRCUIT_CACHE_KEY, true, $seconds);
            }
        } catch (\Throwable $e) {
            // Cache unavailable: breaker disabled, timeouts still apply
        }
    }

    protected static function warn(string $message): void
    {
        try {
            if (function_exists('logger')) {
                logger()->warning('mimon/kafka-logger: ' . $message);
            }
        } catch (\Throwable $e) {
            // ignore
        }
    }
}
