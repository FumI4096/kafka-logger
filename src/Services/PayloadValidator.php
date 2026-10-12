<?php

namespace Mimon\KafkaLogger\Services;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Illuminate\Support\Str;

class PayloadValidator
{
    public static function validate(array $payload): array
    {
        $errors = [];

        // 1. log_id
        if (empty($payload['log_id']) || ! Str::isUuid($payload['log_id'])) {
            $errors[] = "Field 'log_id' must be a valid UUID v4.";
        }

        // 2. environment
        if (empty($payload['environment']) || ! Environment::tryFrom(strtoupper((string) $payload['environment']))) {
            $validEnvs = implode(', ', array_column(Environment::cases(), 'value'));
            $errors[] = "Field 'environment' must be one of: {$validEnvs}.";
        }

        // 3. application
        if (empty($payload['application']) || ! is_array($payload['application'])) {
            $errors[] = "Field 'application' object is required.";
        } else {
            if (empty($payload['application']['name'])) {
                $errors[] = "Field 'application.name' is required.";
            }
            $comp = $payload['application']['component'] ?? null;
            if (empty($comp) || ! ComponentSubsystem::tryFrom(strtoupper((string) $comp))) {
                $validComps = implode(', ', array_column(ComponentSubsystem::cases(), 'value'));
                $errors[] = "Field 'application.component' must be one of: {$validComps}.";
            }
            if (empty($payload['application']['host_ip'])) {
                $errors[] = "Field 'application.host_ip' is required.";
            }
        }

        // 4. event
        if (empty($payload['event']) || ! is_array($payload['event'])) {
            $errors[] = "Field 'event' object is required.";
        } else {
            if (empty($payload['event']['type'])) {
                $errors[] = "Field 'event.type' is required.";
            } elseif (! preg_match('/^[A-Z0-9_]+$/', $payload['event']['type'])) {
                $errors[] = "Field 'event.type' must be in UPPER_SNAKE_CASE (e.g. EMPLOYEE_CREATED).";
            }

            $lvl = $payload['event']['level'] ?? null;
            if (empty($lvl) || ! LogLevel::tryFrom(strtoupper((string) $lvl))) {
                $validLvls = implode(', ', array_column(LogLevel::cases(), 'value'));
                $errors[] = "Field 'event.level' must be one of: {$validLvls}.";
            }

            if (empty($payload['event']['topic'])) {
                $errors[] = "Field 'event.topic' is required.";
            }

            if (! isset($payload['event']['message']) || $payload['event']['message'] === '') {
                $errors[] = "Field 'event.message' is required.";
            }
        }

        // 5. trace
        if (empty($payload['trace']) || ! is_array($payload['trace'])) {
            $errors[] = "Field 'trace' object is required.";
        } else {
            if (empty($payload['trace']['trace_id'])) {
                $errors[] = "Field 'trace.trace_id' is required.";
            }
        }

        // 6. actor
        if (empty($payload['actor']) || ! is_array($payload['actor'])) {
            $errors[] = "Field 'actor' object is required.";
        } else {
            if (empty($payload['actor']['user_id'])) {
                $errors[] = "Field 'actor.user_id' is required.";
            }
        }

        // 7. timestamps
        if (empty($payload['timestamps']) || ! is_array($payload['timestamps'])) {
            $errors[] = "Field 'timestamps' object is required.";
        } else {
            if (empty($payload['timestamps']['created_at'])) {
                $errors[] = "Field 'timestamps.created_at' ISO-8601 string is required.";
            }
            if (empty($payload['timestamps']['produced_at'])) {
                $errors[] = "Field 'timestamps.produced_at' ISO-8601 string is required.";
            }
        }

        return $errors;
    }

    public static function isValid(array $payload): bool
    {
        return empty(static::validate($payload));
    }
}