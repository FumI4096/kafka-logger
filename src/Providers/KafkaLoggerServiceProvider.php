<?php

namespace Mimon\KafkaLogger\Providers;

use Illuminate\Auth\Events\Failed;
use Illuminate\Auth\Events\Login;
use Illuminate\Auth\Events\Logout;
use Illuminate\Cache\Events\CacheHit;
use Illuminate\Cache\Events\CacheMissed;
use Illuminate\Contracts\Http\Kernel;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Http\Client\Events\ConnectionFailed;
use Illuminate\Http\Client\Events\RequestSending;
use Illuminate\Http\Client\Events\ResponseReceived;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Queue\Events\JobProcessing;
use Illuminate\Queue\Events\JobQueued;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use Mimon\KafkaLogger\Console\Commands\InstallKafkaLoggerCommand;
use Mimon\KafkaLogger\Http\Middleware\PulseSlowRequestMiddleware;
use Mimon\KafkaLogger\Listeners\LogAuthenticationEvents;
use Mimon\KafkaLogger\Listeners\LogErrorEvents;
use Mimon\KafkaLogger\Listeners\PulseCacheListener;
use Mimon\KafkaLogger\Listeners\PulseJobListener;
use Mimon\KafkaLogger\Listeners\PulseOutgoingRequestListener;
use Mimon\KafkaLogger\Listeners\PulseSlowQueryListener;
use Mimon\KafkaLogger\Services\KafkaLoggerService;

class KafkaLoggerServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../../config/kafka-logger.php',
            'kafka-logger'
        );

        $this->app->singleton('kafka-logger', function ($app) {
            return new KafkaLoggerService();
        });

        $this->app->alias('kafka-logger', KafkaLoggerService::class);
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes([
                __DIR__ . '/../../config/kafka-logger.php' => config_path('kafka-logger.php'),
            ], 'kafka-logger-config');

            $this->commands([
                InstallKafkaLoggerCommand::class,
            ]);
        }

        if (config('kafka-logger.auto_log_auth', true)) {
            Event::listen(Login::class, [LogAuthenticationEvents::class, 'handleLogin']);
            Event::listen(Failed::class, [LogAuthenticationEvents::class, 'handleFailed']);
            Event::listen(Logout::class, [LogAuthenticationEvents::class, 'handleLogout']);
        }

        if (config('kafka-logger.auto_log_errors', true)) {
            Event::listen(MessageLogged::class, [LogErrorEvents::class, 'handleMessageLogged']);
            Event::listen(JobFailed::class, [LogErrorEvents::class, 'handleJobFailed']);
        }

        if (config('kafka-logger.enabled', true) && config('kafka-logger.pulse.enabled', true)) {
            if (config('kafka-logger.pulse.slow_queries.enabled', true)) {
                Event::listen(QueryExecuted::class, [PulseSlowQueryListener::class, 'handle']);
            }

            if (config('kafka-logger.pulse.slow_jobs.enabled', true)) {
                Event::listen(JobQueued::class, [PulseJobListener::class, 'handleQueued']);
                Event::listen(JobProcessing::class, [PulseJobListener::class, 'handleProcessing']);
                Event::listen(JobProcessed::class, [PulseJobListener::class, 'handleProcessed']);
                Event::listen(JobFailed::class, [PulseJobListener::class, 'handleFailed']);
            }

            if (config('kafka-logger.pulse.slow_outgoing_requests.enabled', true)) {
                Event::listen(RequestSending::class, [PulseOutgoingRequestListener::class, 'handleSending']);
                Event::listen(ResponseReceived::class, [PulseOutgoingRequestListener::class, 'handleResponse']);
                Event::listen(ConnectionFailed::class, [PulseOutgoingRequestListener::class, 'handleConnectionFailed']);
            }

            if (config('kafka-logger.pulse.usage_rollup.enabled', true)) {
                Event::listen(CacheHit::class, [PulseCacheListener::class, 'handleHit']);
                Event::listen(CacheMissed::class, [PulseCacheListener::class, 'handleMissed']);
            }

            if (
                ! $this->app->runningInConsole()
                && (
                    config('kafka-logger.pulse.slow_requests.enabled', true)
                    || config('kafka-logger.pulse.server_heartbeat.enabled', true)
                    || config('kafka-logger.pulse.usage_rollup.enabled', true)
                )
            ) {
                $kernel = $this->app->make(Kernel::class);
                if (method_exists($kernel, 'pushMiddleware')) {
                    $kernel->pushMiddleware(PulseSlowRequestMiddleware::class);
                }
            }
        }
    }
}
