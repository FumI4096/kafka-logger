<?php

namespace Mimon\KafkaLogger\Services;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Throwable;

class KafkaLogBuilder
{
    protected ?string $eventType = null;

    protected ?string $logMessage = null;

    protected LogLevel|string $logLevel = LogLevel::INFO;

    protected ?string $customAppName = null;

    protected Environment|string|null $customEnvironment = null;

    protected ComponentSubsystem|string|null $customComponent = null;

    protected ?string $customTopic = null;

    protected ?string $userId = null;

    protected ?string $userRole = null;

    protected array $metadata = [];

    protected ?Throwable $exception = null;

    public function event(string $eventType): self
    {
        $this->eventType = $eventType;

        return $this;
    }

    public function message(string $message): self
    {
        $this->logMessage = $message;

        return $this;
    }

    public function level(LogLevel|string $level): self
    {
        $this->logLevel = $level;

        return $this;
    }

    public function info(?string $message = null): self
    {
        $this->logLevel = LogLevel::INFO;
        if ($message !== null) {
            $this->logMessage = $message;
        }

        return $this;
    }

    public function warning(?string $message = null): self
    {
        $this->logLevel = LogLevel::WARNING;
        if ($message !== null) {
            $this->logMessage = $message;
        }

        return $this;
    }

    public function debug(?string $message = null): self
    {
        $this->logLevel = LogLevel::DEBUG;
        if ($message !== null) {
            $this->logMessage = $message;
        }

        return $this;
    }

    public function error(?string $message = null, ?Throwable $exception = null): self
    {
        $this->logLevel = LogLevel::ERROR;
        if ($message !== null) {
            $this->logMessage = $message;
        }
        if ($exception !== null) {
            $this->exception = $exception;
        }

        return $this;
    }

    public function critical(?string $message = null, ?Throwable $exception = null): self
    {
        $this->logLevel = LogLevel::CRITICAL;
        if ($message !== null) {
            $this->logMessage = $message;
        }
        if ($exception !== null) {
            $this->exception = $exception;
        }

        return $this;
    }

    public function appName(string $appName): self
    {
        $this->customAppName = $appName;

        return $this;
    }

    public function environment(Environment|string $environment): self
    {
        $this->customEnvironment = $environment;

        return $this;
    }

    public function component(ComponentSubsystem|string $component): self
    {
        $this->customComponent = $component;

        return $this;
    }

    public function topic(string $topic): self
    {
        $this->customTopic = $topic;

        return $this;
    }

    public function actor(string $userId, ?string $role = null): self
    {
        $this->userId = $userId;
        if ($role !== null) {
            $this->userRole = $role;
        }

        return $this;
    }

    public function user(string $userId, ?string $role = null): self
    {
        return $this->actor($userId, $role);
    }

    public function withMetadata(array $metadata): self
    {
        $this->metadata = array_merge($this->metadata, $metadata);

        return $this;
    }

    public function withContext(array $context): self
    {
        return $this->withMetadata($context);
    }

    public function withException(Throwable $exception): self
    {
        $this->exception = $exception;

        return $this;
    }

    public function dispatch(): array
    {
        $eventType = $this->eventType ?? 'SYSTEM_EVENT';
        $message = $this->logMessage ?? '';

        $options = [
            'app_name' => $this->customAppName,
            'environment' => $this->customEnvironment,
            'component' => $this->customComponent,
            'topic' => $this->customTopic,
            'user_id' => $this->userId,
            'user_role' => $this->userRole,
            'metadata' => $this->metadata,
            'exception' => $this->exception,
        ];

        $options = array_filter($options, fn ($val) => $val !== null);

        /** @var KafkaLoggerService $service */
        $service = app('kafka-logger');

        return $service->directLog(
            eventType: $eventType,
            message: $message,
            level: $this->logLevel,
            options: $options
        );
    }

    public function send(): array
    {
        return $this->dispatch();
    }

    public function log(): array
    {
        return $this->dispatch();
    }
}