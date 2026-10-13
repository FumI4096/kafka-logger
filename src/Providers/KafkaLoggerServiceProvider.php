<?php

namespace Mimon\KafkaLogger\Providers;

use Illuminate\Auth\Events\Failed;
use Illuminate\Auth\Events\Login;
use Illuminate\Auth\Events\Logout;
use Illuminate\Log\Events\MessageLogged;
use Illuminate\Queue\Events\JobFailed;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\ServiceProvider;
use Mimon\KafkaLogger\Listeners\LogAuthenticationEvents;
use Mimon\KafkaLogger\Listeners\LogErrorEvents;
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
    }
}