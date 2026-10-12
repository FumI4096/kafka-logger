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
];