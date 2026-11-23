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

    'app_code' => env('KAFKA_LOGGER_APP_CODE', env('KAFKA_LOGGER_APP_NAME', 'HR_APP')),
    'app_name' => env('KAFKA_LOGGER_APP_NAME', env('KAFKA_LOGGER_APP_CODE', 'HR_APP')),

    // PRODUCTION | STAGING | DEVELOPMENT | LOCAL (defaults to uppercase APP_ENV)
    'environment' => strtoupper((string) (env('KAFKA_LOGGER_ENVIRONMENT') ?: env('APP_ENV', 'production'))),

    'default_topic' => env('KAFKA_LOGGER_DEFAULT_TOPIC', 'CLS.LIVE.logs'),

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

    /*
    |--------------------------------------------------------------------------
    | Error Rate Limiting (Per-Signature Deduplication)
    |--------------------------------------------------------------------------
    |
    | Prevents identical repeated errors from flooding Kafka. After rate_limit_max
    | occurrences within rate_limit_seconds, further duplicates are suppressed
    | until the window resets (0 = disabled).
    |
    */
    'rate_limit_max' => (int) env('KAFKA_LOGGER_RATE_LIMIT_MAX', 5),

    'rate_limit_seconds' => (int) env('KAFKA_LOGGER_RATE_LIMIT_SECONDS', 60),

    'rate_limit_levels' => array_values(array_filter(array_map(
        static fn (string $lvl): string => strtoupper(trim($lvl)),
        explode(',', (string) env('KAFKA_LOGGER_RATE_LIMIT_LEVELS', 'ERROR,CRITICAL'))
    ))),

    /*
    |--------------------------------------------------------------------------
    | Lean Pulse APM Telemetry (Zero-Config)
    |--------------------------------------------------------------------------
    |
    | Automatically ships threshold-triggered performance signals (Slow HTTP
    | Requests & Slow SQL Queries) plus a throttled Server Vitals heartbeat
    | (CPU, RAM, Disk — at most once per interval during normal traffic, no
    | cron or scheduler needed) over your existing CLS Kafka topic.
    |
    */
    'pulse' => [
        'enabled' => env('KAFKA_LOGGER_PULSE_ENABLED', true),

        'slow_requests' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_SLOW_REQUESTS', true),
            'threshold_ms' => (int) env('KAFKA_LOGGER_PULSE_SLOW_REQUEST_MS', 1000),
            'ignore_paths' => ['_debugbar/*', 'horizon/*', 'telescope/*', 'pulse*', 'livewire/update'],
        ],

        'slow_queries' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_SLOW_QUERIES', true),
            'threshold_ms' => (int) env('KAFKA_LOGGER_PULSE_SLOW_QUERY_MS', 500),
            'max_query_length' => 1000,
        ],

        'slow_jobs' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_SLOW_JOBS', true),
            'threshold_ms' => (int) env('KAFKA_LOGGER_PULSE_SLOW_JOB_MS', 1000),
        ],

        'slow_outgoing_requests' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_SLOW_OUTGOING', true),
            'threshold_ms' => (int) env('KAFKA_LOGGER_PULSE_SLOW_OUTGOING_MS', 1000),
        ],

        'usage_rollup' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_USAGE_ROLLUP', true),
            'flush_interval_seconds' => (int) env('KAFKA_LOGGER_PULSE_FLUSH_SECONDS', 60),
        ],

        'server_heartbeat' => [
            'enabled' => env('KAFKA_LOGGER_PULSE_SERVER_HEARTBEAT', true),
            'interval_seconds' => (int) env('KAFKA_LOGGER_PULSE_HEARTBEAT_SECONDS', 60),
            'disk_path' => env('KAFKA_LOGGER_PULSE_DISK_PATH', '/'),
        ],
    ],
];
