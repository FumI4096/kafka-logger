<?php

namespace Mimon\KafkaLogger\Services;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Throwable;

class KafkaLoggerService
{
    protected string $appName;

    protected string $environment;

    protected string $defaultTopic;

    protected bool $autoValidate;

    public function __construct()
    {
        $this->appName = config('kafka-logger.app_name', 'HR_APP');
        $this->environment = config('kafka-logger.environment', 'PRODUCTION');
        $this->defaultTopic = config('kafka-logger.default_topic', 'CLS.LOCAL.logs');
        $this->autoValidate = (bool) config('kafka-logger.auto_validate', true);
    }

    public function make(): KafkaLogBuilder
    {
        return new KafkaLogBuilder();
    }

    public function event(string $eventType): KafkaLogBuilder
    {
        return $this->make()->event($eventType);
    }

    public function info(?string $arg1 = null, ?string $arg2 = null, array $options = []): KafkaLogBuilder|array
    {
        if ($arg2 !== null) {
            return $this->legacyLog($arg1, $arg2, LogLevel::INFO, $options);
        }

        $builder = $this->make()->info();
        if ($arg1 !== null) {
            $builder->message($arg1);
        }

        return $builder;
    }

    public function warning(?string $arg1 = null, ?string $arg2 = null, array $options = []): KafkaLogBuilder|array
    {
        if ($arg2 !== null) {
            return $this->legacyLog($arg1, $arg2, LogLevel::WARNING, $options);
        }

        $builder = $this->make()->warning();
        if ($arg1 !== null) {
            $builder->message($arg1);
        }

        return $builder;
    }

    public function debug(?string $arg1 = null, ?string $arg2 = null, array $options = []): KafkaLogBuilder|array
    {
        if ($arg2 !== null) {
            return $this->legacyLog($arg1, $arg2, LogLevel::DEBUG, $options);
        }

        $builder = $this->make()->debug();
        if ($arg1 !== null) {
            $builder->message($arg1);
        }

        return $builder;
    }

    public function error(?string $arg1 = null, ?string $arg2 = null, ?Throwable $exception = null, array $options = []): KafkaLogBuilder|array
    {
        if ($arg2 !== null) {
            if ($exception !== null) {
                $options['exception'] = $exception;
            }
            return $this->legacyLog($arg1, $arg2, LogLevel::ERROR, $options);
        }

        $builder = $this->make()->error();
        if ($arg1 !== null) {
            $builder->message($arg1);
        }
        if ($exception !== null) {
            $builder->withException($exception);
        }

        return $builder;
    }

    public function critical(?string $arg1 = null, ?string $arg2 = null, ?Throwable $exception = null, array $options = []): KafkaLogBuilder|array
    {
        if ($arg2 !== null) {
            if ($exception !== null) {
                $options['exception'] = $exception;
            }
            return $this->legacyLog($arg1, $arg2, LogLevel::CRITICAL, $options);
        }

        $builder = $this->make()->critical();
        if ($arg1 !== null) {
            $builder->message($arg1);
        }
        if ($exception !== null) {
            $builder->withException($exception);
        }

        return $builder;
    }

    protected function legacyLog(string $eventType, string $message, LogLevel|string $level, array $options = []): array
    {
        return $this->directLog($eventType, $message, $level, $options);
    }

    public function directLog(
        string $eventType,
        string $message,
        LogLevel|string $level = LogLevel::INFO,
        array $options = []
    ): array {
        $appName = $options['app_name'] ?? $this->appName;
        $env = $options['environment'] ?? $this->environment;
        $component = $options['component'] ?? ComponentSubsystem::APPLICATION_LOGIC;
        $topic = $options['topic'] ?? $this->defaultTopic;
        $userId = $options['user_id'] ?? null;
        $userRole = $options['user_role'] ?? 'SYSTEM';
        $metadata = $options['metadata'] ?? [];
        $exception = $options['exception'] ?? null;

        if ($this->autoValidate) {
            $envStr = $env instanceof Environment ? $env->value : (string) $env;
            $levelStr = $level instanceof LogLevel ? $level->value : (string) $level;
            $compStr = $component instanceof ComponentSubsystem ? $component->value : (string) $component;
            $roleStr = (string) $userRole;

            $draftPayload = [
                'log_id' => (string) Str::uuid(),
                'environment' => $envStr,
                'application' => [
                    'name' => strtoupper(str_replace('-', '_', $appName)),
                    'component' => $compStr,
                    'host_ip' => request()->ip() ?? (gethostbyname(gethostname()) ?: '10.200.4.15'),
                    'host_name' => gethostname() ?: 'node-01',
                ],
                'event' => [
                    'type' => strtoupper(str_replace(['.', '-'], '_', $eventType)),
                    'level' => $levelStr,
                    'topic' => $topic,
                    'message' => $message,
                ],
                'trace' => [
                    'trace_id' => request()->header('x-trace-id') ?? ('tr-' . Str::random(12)),
                    'span_id' => 'sp-' . Str::random(6),
                ],
                'actor' => [
                    'user_id' => $userId ?? 'usr_system',
                    'user_role' => $roleStr,
                    'client_ip' => request()->ip() ?? '127.0.0.1',
                ],
                'timestamps' => [
                    'created_at' => Carbon::now()->toIso8601String(),
                    'produced_at' => Carbon::now()->toIso8601String(),
                ],
            ];

            $errors = PayloadValidator::validate($draftPayload);
            if (! empty($errors)) {
                throw new InvalidArgumentException('Kafka Log Payload validation failed: ' . implode(' ', $errors));
            }
        }

        return LoggerService::createEnterpriseLog(
            appName: $appName,
            eventType: $eventType,
            message: $message,
            level: $level,
            component: $component,
            topic: $topic,
            userId: $userId,
            userRole: $userRole,
            metadata: $metadata,
            exception: $exception,
            environment: $env
        );
    }
}