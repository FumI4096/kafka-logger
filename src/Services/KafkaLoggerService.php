<?php

namespace Mimon\KafkaLogger\Services;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Listeners\LogErrorEvents;
use Mimon\KafkaLogger\Support\LogContext;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;
use InvalidArgumentException;
use Throwable;

class KafkaLoggerService
{
    protected string $appCode;

    protected string $appName;

    protected string $environment;

    protected string $defaultTopic;

    protected bool $autoValidate;

    public function __construct()
    {
        $this->appCode = (string) config('kafka-logger.app_code');
        $this->appName = (string) config('kafka-logger.app_name', $this->appCode);
        $this->environment = (string) (config('kafka-logger.environment') ?: 'LOCAL');
        $this->defaultTopic = (string) (config('kafka-logger.default_topic') ?: 'CLS.LIVE.logs');
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

    /**
     * Bind scoped workflow/operation context (module, workflow, operation, service, trace_id).
     *
     * @param  array<string, mixed>  $context
     */
    public function withContext(array $context): void
    {
        LogContext::set($context);
    }

    public function clearContext(): void
    {
        LogContext::clear();
    }

    /**
     * @return array<string, mixed>
     */
    public function getContext(): array
    {
        return LogContext::get();
    }

    /**
     * Execute a unit of work under a shared trace_id and scoped context.
     * Logs a single ERROR event (with duration_ms and root cause) if a Throwable escapes,
     * then rethrows without triggering duplicate LogErrorEvents reporting.
     *
     * @template TReturn
     *
     * @param  callable(): TReturn  $callback
     * @param  array<string, mixed>  $context
     * @return TReturn
     */
    public function transaction(
        callable $callback,
        array $context = [],
        string $event = 'WORKFLOW_FAILED',
        ?string $successEvent = null,
    ): mixed {
        return LogContext::runWith($context, function () use ($callback, $event, $successEvent) {
            $startedAtMs = (int) round(microtime(true) * 1000);

            try {
                $result = $callback();

                if ($successEvent !== null && $successEvent !== '') {
                    $durationMs = max(0, ((int) round(microtime(true) * 1000)) - $startedAtMs);
                    $this->event($successEvent)
                        ->info()
                        ->withMetadata(['duration_ms' => $durationMs])
                        ->send();
                }

                return $result;
            } catch (Throwable $e) {
                $durationMs = max(0, ((int) round(microtime(true) * 1000)) - $startedAtMs);
                LogErrorEvents::markAsLogged($e);

                $this->event($event)
                    ->error($e->getMessage() ?: 'Workflow execution failed.', $e)
                    ->withMetadata([
                        'duration_ms' => $durationMs,
                        'exception_class' => get_class($e),
                        'error_category' => 'WORKFLOW_ERROR',
                    ])
                    ->send();

                throw $e;
            }
        });
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
        $appCode = $options['app_code'] ?? $options['app_name'] ?? $this->appCode;
        $appName = $options['app_name'] ?? $options['app_code'] ?? $this->appName;
        $env = $options['environment'] ?? $this->environment;
        $component = $options['component'] ?? ComponentSubsystem::APPLICATION_LOGIC;
        $topic = $options['topic'] ?? null;
        $traceId = $options['trace_id'] ?? null;
        $userId = $options['user_id'] ?? null;
        $userRole = $options['user_role'] ?? 'SYSTEM';
        $metadata = $options['metadata'] ?? [];
        $exception = $options['exception'] ?? null;

        return LoggerService::createEnterpriseLog(
            appCode: $appCode,
            eventType: $eventType,
            message: $message,
            level: $level,
            component: $component,
            topic: $topic,
            userId: $userId,
            userRole: $userRole,
            metadata: $metadata,
            exception: $exception,
            environment: $env,
            appName: $appName,
            traceId: $traceId,
        );
    }
}