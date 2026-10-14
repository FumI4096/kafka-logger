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
        $this->environment = config('kafka-logger.environment', 'LOCAL');
        $this->defaultTopic = config('kafka-logger.default_topic', 'YOUR_TOPIC');
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