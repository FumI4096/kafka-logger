<?php

namespace Mimon\KafkaLogger\Listeners;

use Illuminate\Auth\Access\AuthorizationException;
use Illuminate\Auth\AuthenticationException;
use Illuminate\Database\QueryException;
use Illuminate\Http\Client\HttpClientException;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Facades\KafkaLogger;
use PDOException;
use Throwable;

class LogErrorEvents
{
    /**
     * Guard against infinite recursion if logging itself triggers a log event.
     */
    protected static bool $isHandling = false;

    /**
     * Track already-dispatched Throwable object IDs within the current request/job lifecycle
     * to prevent duplicate Kafka logs when both MessageLogged and JobFailed fire.
     *
     * @var array<int, bool>
     */
    protected static array $loggedExceptionIds = [];

    /**
     * Handle Laravel's MessageLogged event for error, critical, alert, and emergency levels.
     */
    public function handleMessageLogged(MessageLogged $event): void
    {
        if (self::$isHandling) {
            return;
        }

        $levelName = strtolower($event->level);
        if (! in_array($levelName, ['error', 'critical', 'alert', 'emergency'], true)) {
            return;
        }

        $message = (string) $event->message;
        if (str_starts_with($message, 'mimon/kafka-logger:')) {
            return;
        }

        $exception = ($event->context['exception'] ?? null) instanceof Throwable
            ? $event->context['exception']
            : null;

        if ($exception !== null) {
            $objId = spl_object_id($exception);
            if (isset(self::$loggedExceptionIds[$objId])) {
                return;
            }
            self::$loggedExceptionIds[$objId] = true;
        }

        self::$isHandling = true;

        try {
            $kafkaLevel = in_array($levelName, ['critical', 'alert', 'emergency'], true)
                ? LogLevel::CRITICAL
                : LogLevel::ERROR;

            [$component, $defaultEventType] = $this->resolveComponentAndEventType($exception, $event->context);

            $eventType = ! empty($event->context['event_type']) && is_string($event->context['event_type'])
                ? $event->context['event_type']
                : $defaultEventType;

            [$userId, $userRole] = $this->resolveActor();

            $metadata = $this->buildMetadata($event->context, $exception);

            $builder = KafkaLogger::event($eventType)
                ->message($message !== '' ? $message : ($exception?->getMessage() ?: 'Application error occurred.'))
                ->level($kafkaLevel)
                ->component($component)
                ->actor($userId, $userRole)
                ->withMetadata($metadata);

            if ($exception !== null) {
                $builder->withException($exception);
            }

            $builder->send();
        } catch (Throwable $e) {
            // Silently ignore to avoid breaking application flow
        } finally {
            self::$isHandling = false;
        }
    }

    /**
     * Handle failed background queue jobs.
     */
    public function handleJobFailed(JobFailed $event): void
    {
        if (self::$isHandling) {
            return;
        }

        $exception = $event->exception;
        $objId = spl_object_id($exception);
        if (isset(self::$loggedExceptionIds[$objId])) {
            return;
        }
        self::$loggedExceptionIds[$objId] = true;

        self::$isHandling = true;

        try {
            $jobName = method_exists($event->job, 'resolveName')
                ? $event->job->resolveName()
                : get_class($event->job);

            KafkaLogger::event('QUEUE_JOB_FAILED')
                ->message("Queue job [{$jobName}] failed: {$exception->getMessage()}")
                ->error()
                ->component(ComponentSubsystem::QUEUE_JOB)
                ->actor('usr_queue_worker', 'SYSTEM')
                ->withException($exception)
                ->withMetadata([
                    'job_name'        => $jobName,
                    'connection_name' => $event->connectionName,
                    'queue'           => $event->job->getQueue(),
                    'exception_class' => get_class($exception),
                    'file'            => $exception->getFile(),
                    'line'            => $exception->getLine(),
                ])
                ->send();
        } catch (Throwable $e) {
            // Silently ignore
        } finally {
            self::$isHandling = false;
        }
    }

    /**
     * Automatically classify the ComponentSubsystem and Event Type from the Throwable or context.
     *
     * @return array{0: ComponentSubsystem, 1: string}
     */
    protected function resolveComponentAndEventType(?Throwable $exception, array $context = []): array
    {
        if (! empty($context['component'])) {
            $customComp = $context['component'] instanceof ComponentSubsystem
                ? $context['component']
                : ComponentSubsystem::tryFrom(strtoupper((string) $context['component']));

            if ($customComp) {
                return [$customComp, 'APPLICATION_ERROR'];
            }
        }

        if ($exception instanceof QueryException || $exception instanceof PDOException) {
            return [ComponentSubsystem::DATABASE, 'DATABASE_QUERY_ERROR'];
        }

        if ($exception instanceof HttpClientException) {
            return [ComponentSubsystem::EXTERNAL_API, 'EXTERNAL_API_ERROR'];
        }

        if ($exception instanceof AuthenticationException || $exception instanceof AuthorizationException) {
            return [ComponentSubsystem::AUTHENTICATION, 'AUTH_ACCESS_DENIED'];
        }

        if ($exception !== null) {
            $className = get_class($exception);

            if (str_contains($className, 'Redis') || str_contains($className, 'Cache')) {
                return [ComponentSubsystem::CACHE, 'CACHE_ERROR'];
            }

            if (str_contains($className, 'Filesystem') || str_contains($className, 'League\\Flysystem')) {
                return [ComponentSubsystem::STORAGE, 'STORAGE_ERROR'];
            }

            if (str_contains($className, 'Kafka')) {
                return [ComponentSubsystem::KAFKA_PRODUCER, 'KAFKA_ERROR'];
            }
        }

        return [ComponentSubsystem::APPLICATION_LOGIC, 'APPLICATION_ERROR'];
    }

    /**
     * Resolve the currently authenticated user (e.g. Filament user) or fallback actor.
     *
     * @return array{0: string, 1: string}
     */
    protected function resolveActor(): array
    {
        try {
            $user = auth()->user();
            if ($user) {
                $userId = (string) $user->getAuthIdentifier();
                $role = $this->resolvePrimaryRole($user);

                return [$userId, $role];
            }
        } catch (Throwable $e) {
            // Ignore auth resolution errors
        }

        if (app()->runningInConsole()) {
            return ['usr_system', 'SYSTEM'];
        }

        return ['usr_anonymous', 'GUEST'];
    }

    /**
     * Build rich metadata including PostgreSQL/MySQL details and Filament/Livewire context.
     */
    protected function buildMetadata(array $context, ?Throwable $exception): array
    {
        $metadata = [];

        foreach ($context as $key => $value) {
            if ($key === 'exception' || is_object($value)) {
                continue;
            }
            $metadata[$key] = $value;
        }

        if ($exception !== null) {
            $metadata['exception_class'] = get_class($exception);
            $metadata['file'] = $exception->getFile();
            $metadata['line'] = $exception->getLine();

            if ($exception instanceof QueryException) {
                $metadata['db_connection'] = $exception->getConnectionName();
                $metadata['sql_state'] = $exception->errorInfo[0] ?? (string) $exception->getCode();
            } elseif ($exception instanceof PDOException) {
                $metadata['sql_state'] = $exception->errorInfo[0] ?? (string) $exception->getCode();
            }
        }

        if (! app()->runningInConsole() && function_exists('request') && request()) {
            $req = request();
            $metadata['url'] = $req->fullUrl();
            $metadata['http_method'] = $req->method();
            $metadata['route_name'] = $req->route()?->getName();

            // Enrich Filament / Livewire AJAX requests with the actual page Referer and component name
            if ($req->hasHeader('X-Livewire') || str_contains($req->path(), 'livewire')) {
                $metadata['is_livewire'] = true;
                $metadata['filament_page_url'] = $req->header('Referer');

                $components = $req->input('components');
                if (is_array($components) && isset($components[0]['snapshot'])) {
                    $snapshot = json_decode((string) $components[0]['snapshot'], true);
                    if (! empty($snapshot['memo']['name'])) {
                        $metadata['filament_component'] = $snapshot['memo']['name'];
                    }
                }
            }
        }

        return $metadata;
    }

    /**
     * Resolve the primary role string in UPPER_SNAKE_CASE for the CLS actor contract.
     */
    protected function resolvePrimaryRole(object $user): string
    {
        if (method_exists($user, 'getRoleNames')) {
            $firstRole = $user->getRoleNames()->first();
            if ($firstRole) {
                return strtoupper(str_replace(['-', ' '], '_', $firstRole));
            }
        }

        if (! empty($user->role) && is_string($user->role)) {
            return strtoupper(str_replace(['-', ' '], '_', $user->role));
        }

        return 'AUTHENTICATED_USER';
    }

    /**
     * Reset the deduplication cache (useful in long-running workers or tests).
     */
    public static function flushLoggedExceptions(): void
    {
        self::$loggedExceptionIds = [];
    }
}