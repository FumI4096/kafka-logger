<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Kafka Brokers URL
    |--------------------------------------------------------------------------
    |
    | Comma-separated list of Kafka broker endpoints.
    |
    */
    'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),

    /*
    |--------------------------------------------------------------------------
    | Default Application Name
    |--------------------------------------------------------------------------
    |
    | Unique identifier for this application/microservice producing logs.
    | Examples: 'HR_APP', 'CASHIER_APP', 'ESTORE', 'REGISTRATION_APP'.
    |
    */
    'app_name' => env('KAFKA_LOGGER_APP_NAME', 'HR_APP'),

    /*
    |--------------------------------------------------------------------------
    | Deployment Environment
    |--------------------------------------------------------------------------
    |
    | Deployment stage emitting telemetry. Must be one of:
    | 'PRODUCTION', 'STAGING', 'DEVELOPMENT', 'LOCAL'.
    |
    */
    'environment' => env('KAFKA_LOGGER_ENVIRONMENT', 'PRODUCTION'),

    /*
    |--------------------------------------------------------------------------
    | Default Kafka Event Topic
    |--------------------------------------------------------------------------
    |
    | Kafka topic where produced log payloads are dispatched.
    |
    */
    'default_topic' => env('KAFKA_LOGGER_DEFAULT_TOPIC', 'CLS.LOCAL.logs'),

    /*
    |--------------------------------------------------------------------------
    | Automatic Contract Schema Validation
    |--------------------------------------------------------------------------
    |
    | When enabled, every log dispatch is validated against the EnterpriseLogPayload
    | schema taxonomy before saving/producing. Throws InvalidArgumentException on breach.
    |
    */
    'auto_validate' => env('KAFKA_LOGGER_AUTO_VALIDATE', true),

    /*
    |--------------------------------------------------------------------------
    | Produce Logs to Kafka Broke
    |--------------------------------------------------------------------------
    |
    | Toggle whether payloads are published to the Kafka broker when
    | mateusjunges/laravel-kafka is installed.
    |
    */
    'produce_to_kafka' => env('KAFKA_LOGGER_PRODUCE', true),

    /*
    |--------------------------------------------------------------------------
    | Automatic Authentication Event Logging
    |--------------------------------------------------------------------------
    |
    | Automatically listen to Laravel's Login, Failed, and Logout events
    | and dispatch standardized AUTHENTICATION logs to Kafka.
    |
    */
    'auto_log_auth' => env('KAFKA_LOGGER_AUTO_LOG_AUTH', true),

    /*
    |--------------------------------------------------------------------------
    | Automatic Error & Exception Tracing
    |--------------------------------------------------------------------------
    |
    | Automatically listen to Laravel's MessageLogged (error/critical/alert/emergency)
    | and JobFailed events, classify the subsystem (DATABASE, EXTERNAL_API, QUEUE_JOB,
    | CACHE, STORAGE, APPLICATION_LOGIC), extract Filament/Livewire & SQL context,
    | and dispatch standardized ERROR/CRITICAL logs to Kafka.
    |
    */
    'auto_log_errors' => env('KAFKA_LOGGER_AUTO_LOG_ERRORS', true),
];