<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Kafka Connection
    |--------------------------------------------------------------------------
    |
    | Broker address, TLS and SASL are read from config/kafka.php
    | (mateusjunges/laravel-kafka): KAFKA_BROKERS, KAFKA_SECURITY_PROTOCOL,
    | KAFKA_MECHANISMS, KAFKA_USERNAME, KAFKA_PASSWORD. Use SASL_SSL outside LOCAL.
    |
    */

    'app_name' => env('KAFKA_LOGGER_APP_NAME', 'HR_APP'),

    // PRODUCTION | STAGING | DEVELOPMENT | LOCAL
    'environment' => env('KAFKA_LOGGER_ENVIRONMENT', 'LOCAL'),

    'default_topic' => env('KAFKA_LOGGER_DEFAULT_TOPIC', 'YOUR_TOPIC'),

    'produce_to_kafka' => env('KAFKA_LOGGER_PRODUCE', true),

    'auto_log_auth' => env('KAFKA_LOGGER_AUTO_LOG_AUTH', true),

    'auto_log_errors' => env('KAFKA_LOGGER_AUTO_LOG_ERRORS', true),

    /*
    |--------------------------------------------------------------------------
    | Validation
    |--------------------------------------------------------------------------
    |
    | auto_validate: validate every payload against the CLS contract.
    | strict_validation: throw InvalidArgumentException on invalid payloads (good in
    | local dev). When false, invalid payloads are dropped with a warning and the
    | user's request is never interrupted. Defaults to true only when APP_ENV=local.
    |
    */
    'auto_validate' => env('KAFKA_LOGGER_AUTO_VALIDATE', true),

    'strict_validation' => env('KAFKA_LOGGER_STRICT_VALIDATION', env('APP_ENV') === 'local'),

    /*
    |--------------------------------------------------------------------------
    | Privacy / Redaction
    |--------------------------------------------------------------------------
    |
    | redact_keys: any metadata/context key containing one of these words
    | (word-boundary match, case-insensitive, camelCase aware) is replaced
    | with [REDACTED]. Applies to auto-logged errors AND manual withMetadata().
    |
    | context_mode (auto error listener only):
    |   'redact'    - forward Log::error() context with sensitive keys redacted (default)
    |   'allowlist' - forward only keys listed in context_allowlist
    |   'none'      - never forward Log::error() context
    |
    | include_sql: when true, adds the SQL with "?" placeholders (never bound values).
    |
    */
    'redact_keys' => [
        'password', 'passwd', 'pwd', 'secret', 'token', 'authorization', 'cookie',
        'api_key', 'apikey', 'private_key', 'credit_card', 'card_number', 'cvv', 'cvc',
        'pin', 'otp', 'session', 'ssn', 'x_api_key', 'client_secret',
    ],

    'context_mode' => env('KAFKA_LOGGER_CONTEXT_MODE', 'redact'),

    'context_allowlist' => ['event_type', 'component'],

    'include_sql' => env('KAFKA_LOGGER_INCLUDE_SQL', false),

    /*
    |--------------------------------------------------------------------------
    | Size Limits
    |--------------------------------------------------------------------------
    |
    | Kafka's default message.max.bytes is ~1 MB. Oversized payloads are trimmed:
    | message -> stack trace frames -> metadata.
    |
    */
    'max_payload_bytes' => env('KAFKA_LOGGER_MAX_PAYLOAD_BYTES', 900000),

    'max_trace_frames' => env('KAFKA_LOGGER_MAX_TRACE_FRAMES', 25),

    /*
    |--------------------------------------------------------------------------
    | Failure Handling
    |--------------------------------------------------------------------------
    |
    | producer_timeout_ms: max time a single publish may wait on the broker.
    | circuit_breaker_seconds: after a failed publish, skip Kafka for N seconds
    |   so a broker outage costs one timeout, not one per log line. 0 = disabled.
    | async: dispatch publishing to a queued job (needs a real queue worker,
    |   not QUEUE_CONNECTION=sync).
    |
    */
    'producer_timeout_ms' => env('KAFKA_LOGGER_PRODUCER_TIMEOUT_MS', 1500),

    'circuit_breaker_seconds' => env('KAFKA_LOGGER_CIRCUIT_BREAKER_SECONDS', 30),

    'async' => env('KAFKA_LOGGER_ASYNC', false),
];
