<?php

namespace Mimon\KafkaLogger\Providers;

use Illuminate\Support\ServiceProvider;
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
    }
}