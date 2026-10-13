<?php

namespace Mimon\KafkaLogger\Services;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;

class LoggerService
{
    public static function createEnterpriseLog(
        string $appName,
        string $eventType,
        string $message,
        LogLevel|string $level = LogLevel::INFO,
        ComponentSubsystem|string $component = ComponentSubsystem::APPLICATION_LOGIC,
        ?string $topic = null,
        ?string $userId = null,
        ?string $userRole = null,
        ?array $metadata = null,
        ?\Throwable $exception = null,
        Environment|string|null $environment = null
    ): array {
        $logId = (string) Str::uuid();
        $traceId = request()->header('x-trace-id') ?? ('tr-' . Str::random(12));
        $spanId = 'sp-' . Str::random(6);

        $resolvedEnv = is_string($environment)
            ? (Environment::tryFrom(strtoupper($environment)) ?? strtoupper($environment))
            : ($environment ?? Environment::PRODUCTION);

        $resolvedLevel = is_string($level)
            ? (LogLevel::tryFrom(strtoupper($level)) ?? strtoupper($level))
            : $level;

        $resolvedComponent = is_string($component)
            ? (ComponentSubsystem::tryFrom(strtoupper($component)) ?? strtoupper($component))
            : $component;

        $resolvedRole = $userRole ?: 'SYSTEM';

        $hostIp = request()->ip() ?? (gethostbyname(gethostname()) ?: '10.200.4.15');
        $hostName = gethostname() ?: 'node-01';
        $formattedTopic = $topic ?? config('kafka-logger.default_topic', 'CLS.LOCAL.logs');

        $now = Carbon::now();
        $createdAtIso = $now->toIso8601String();
        $producedAtIso = $now->copy()->addMilliseconds(rand(2, 12))->toIso8601String();

        $errorCode = null;
        $stackTrace = null;

        if ($exception) {
            $errorCode = (string) ($exception->getCode() ?: 'UNKNOWN_ERROR');
            $stackTrace = $exception->getTraceAsString();
        }

        $envValue = is_string($resolvedEnv) ? $resolvedEnv : $resolvedEnv->value;
        $componentValue = is_string($resolvedComponent) ? $resolvedComponent : $resolvedComponent->value;
        $levelValue = is_string($resolvedLevel) ? $resolvedLevel : $resolvedLevel->value;
        $roleValue = $resolvedRole;

        $payload = [
            'log_id' => $logId,
            'environment' => $envValue,
            'application' => [
                'name' => strtoupper(str_replace('-', '_', $appName)),
                'component' => $componentValue,
                'host_ip' => $hostIp,
                'host_name' => $hostName,
            ],
            'event' => [
                'type' => strtoupper(str_replace(['.', '-'], '_', $eventType)),
                'level' => $levelValue,
                'topic' => $formattedTopic,
                'message' => $message,
            ],
            'trace' => [
                'trace_id' => $traceId,
                'span_id' => $spanId,
            ],
            'actor' => [
                'user_id' => $userId ?? 'usr_system',
                'user_role' => $roleValue,
                'client_ip' => request()->ip() ?? '127.0.0.1',
            ],
            'timestamps' => [
                'created_at' => $createdAtIso,
                'produced_at' => $producedAtIso,
            ],
            'error' => [
                'code' => $errorCode,
                'stack_trace' => $stackTrace,
            ],
            'metadata' => $metadata ?? [],
        ];

        // Attempt Kafka broker publication if Kafka library is present
        if (class_exists('\Junges\Kafka\Facades\Kafka') && config('kafka-logger.produce_to_kafka', true)) {
            try {
                $producer = method_exists('\Junges\Kafka\Facades\Kafka', 'publishOn')
                    ? \Junges\Kafka\Facades\Kafka::publishOn($formattedTopic)
                    : \Junges\Kafka\Facades\Kafka::publish()->onTopic($formattedTopic);
                $producer->withBody($payload)
                    ->withHeaders([
                        'x-trace-id' => $traceId,
                        'x-log-id'   => $logId,
                    ])
                    ->send();
            } catch (\Throwable $e) {
                logger()->warning('mimon/kafka-logger: Kafka broker publish skipped: ' . $e->getMessage());
            }
        }

        return $payload;
    }
}