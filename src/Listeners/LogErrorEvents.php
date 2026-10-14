<?php

namespace Mimon\KafkaLogger\Listeners;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Facades\KafkaLogger;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use PDOException;
use Throwable;

class LogErrorEvents
{
    /**
     * Guard against infinite recursion if logging itself triggers a log event.
     */
    protected static bool $isHandling = false;

    /**
     * Track already-dispatched Throwable instances using WeakMap to prevent duplicate
     * Kafka logs when both MessageLogged and JobFailed fire for the same exception.
     * WeakMap automatically frees entries when the exception is GC'd, preventing
     * memory leaks and spl_object_id recycling collisions in long-running queue workers or Octane.
     *
     * @var \WeakMap<Throwable, bool>|null
     */
    protected static ?\WeakMap $loggedExceptions = null;

    /**
     * Handle Laravel's MessageLogged event for error, critical, alert, and emergency levels.
     */
    public function handleMessageLogged(object $event): void
    {
        if (self::$isHandling) {
            return;
        }

        $levelName = strtolower((string) ($event->level ?? ''));
        if (! in_array($levelName, ['error', 'critical', 'alert', 'emergency'], true)) {
            return;
        }

        $message = (string) ($event->message ?? '');
        if (str_starts_with($message, 'mimon/kafka-logger:')) {
            return;
        }

        $context = is_array($event->context ?? null) ? $event->context : [];

        $exception = ($context['exception'] ?? null) instanceof Throwable
            ? $context['exception']
            : null;

        if ($exception !== null) {
            self::$loggedExceptions ??= new \WeakMap();
            if (isset(self::$loggedExceptions[$exception])) {
                return;
            }
            self::$loggedExceptions[$exception] = true;
        }

        self::$isHandling = true;

        try {
            $kafkaLevel = in_array($levelName, ['critical', 'alert', 'emergency'], true)
                ? LogLevel::CRITICAL
                : LogLevel::ERROR;

            [$component, $defaultEventType] = $this->resolveComponentAndEventType($exception, $context);

            $eventType = ! empty($context['event_type']) && is_string($context['event_type'])
                ? $context['event_type']
                : $defaultEventType;

            [$userId, $userRole] = $this->resolveActor();

            $metadata = $this->buildMetadata($context, $exception);

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
    public function handleJobFailed(object $event): void
    {
        if (self::$isHandling || ! isset($event->exception) || ! ($event->exception instanceof Throwable)) {
            return;
        }

        $exception = $event->exception;
        self::$loggedExceptions ??= new \WeakMap();
        if (isset(self::$loggedExceptions[$exception])) {
            return;
        }
        self::$loggedExceptions[$exception] = true;

        $jobName = isset($event->job) && is_object($event->job)
            ? (method_exists($event->job, 'resolveName') ? $event->job->resolveName() : get_class($event->job))
            : 'UnknownJob';

        // Guard against infinite loop if our own async publishing job fails
        if (str_contains($jobName, 'SendKafkaLogJob')) {
            return;
        }

        self::$isHandling = true;

        try {
            KafkaLogger::event('QUEUE_JOB_FAILED')
                ->message("Queue job [{$jobName}] failed: {$exception->getMessage()}")
                ->error()
                ->component(ComponentSubsystem::QUEUE_JOB)
                ->actor('usr_queue_worker', 'SYSTEM')
                ->withException($exception)
                ->withMetadata([
                    'job_name'        => $jobName,
                    'connection_name' => $event->connectionName ?? null,
                    'queue'           => (isset($event->job) && method_exists($event->job, 'getQueue')) ? $event->job->getQueue() : null,
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

        if ($exception instanceof PDOException || is_a($exception, 'Illuminate\Database\QueryException')) {
            return [ComponentSubsystem::DATABASE, 'DATABASE_QUERY_ERROR'];
        }

        if (is_a($exception, 'Illuminate\Http\Client\HttpClientException')) {
            return [ComponentSubsystem::EXTERNAL_API, 'EXTERNAL_API_ERROR'];
        }

        if (
            is_a($exception, 'Illuminate\Auth\AuthenticationException')
            || is_a($exception, 'Illuminate\Auth\Access\AuthorizationException')
        ) {
            return [ComponentSubsystem::AUTHENTICATION, 'AUTH_ACCESS_DENIED'];
        }

        if ($exception !== null) {
            $className = get_class($exception);

            if (str_contains($className, 'HttpClient') || str_contains($className, 'GuzzleHttp')) {
                return [ComponentSubsystem::EXTERNAL_API, 'EXTERNAL_API_ERROR'];
            }

            if (str_contains($className, 'AuthenticationException') || str_contains($className, 'AuthorizationException')) {
                return [ComponentSubsystem::AUTHENTICATION, 'AUTH_ACCESS_DENIED'];
            }

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
            if (function_exists('auth') && auth()->check()) {
                $user = auth()->user();
                if ($user) {
                    $userId = (string) $user->getAuthIdentifier();
                    $role = $this->resolvePrimaryRole($user);

                    return [$userId, $role];
                }
            }
        } catch (Throwable $e) {
            // Ignore auth resolution errors
        }

        if (function_exists('app') && app()->runningInConsole()) {
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

        // Context forwarding is controlled by kafka-logger.context_mode.
        // Values are additionally passed through PayloadSanitizer centrally in LoggerService.
        $mode = (string) PayloadSanitizer::config('kafka-logger.context_mode', 'redact');
        $allow = (array) PayloadSanitizer::config('kafka-logger.context_allowlist', ['event_type', 'component']);

        if ($mode !== 'none') {
            foreach ($context as $key => $value) {
                if ($key === 'exception' || is_object($value)) {
                    continue;
                }
                if ($mode === 'allowlist' && ! in_array($key, $allow, true)) {
                    continue;
                }
                $metadata[$key] = $value;
            }
        }

        if ($exception !== null) {
            $metadata['exception_class'] = get_class($exception);
            $metadata['file'] = $exception->getFile();
            $metadata['line'] = $exception->getLine();

            if (is_a($exception, 'Illuminate\Database\QueryException') && method_exists($exception, 'getConnectionName')) {
                $metadata['db_connection'] = $exception->getConnectionName();
                $metadata['sql_state'] = $exception->errorInfo[0] ?? (string) $exception->getCode();
                // Placeholders only ("?"), never bound values.
                if (PayloadSanitizer::config('kafka-logger.include_sql', false) && method_exists($exception, 'getSql')) {
                    $metadata['sql'] = mb_substr((string) $exception->getSql(), 0, 2000);
                }
            } elseif ($exception instanceof PDOException) {
                $metadata['sql_state'] = $exception->errorInfo[0] ?? (string) $exception->getCode();
            }
        }

        if (function_exists('app') && ! app()->runningInConsole() && function_exists('request') && request()) {
            $req = request();
            $metadata['url'] = $req->fullUrl();
            $metadata['http_method'] = $req->method();
            $metadata['route_name'] = $req->route()?->getName();

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
        self::$loggedExceptions = new \WeakMap();
    }
}