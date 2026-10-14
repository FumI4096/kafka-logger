<?php

namespace Mimon\KafkaLogger\Support;

/**
 * Central privacy filter applied to every payload (listeners and manual calls).
 *
 * - Redacts values whose KEY looks sensitive (password, token, secret, ...).
 * - Masks sensitive VALUE patterns in strings (Bearer tokens, JWTs, key=value secrets).
 * - Strips bound SQL values from QueryException / PDO driver messages.
 */
class PayloadSanitizer
{
    public const REDACTED = '[REDACTED]';

    public const DEFAULT_REDACT_KEYS = [
        'password', 'passwd', 'pwd', 'secret', 'token', 'authorization', 'cookie',
        'api_key', 'apikey', 'private_key', 'credit_card', 'card_number', 'cvv', 'cvc',
        'pin', 'otp', 'session', 'ssn', 'x_api_key', 'client_secret',
    ];

    protected const MAX_DEPTH = 6;

    /**
     * Safe config() accessor that also works when the package is tested standalone.
     */
    public static function config(string $key, mixed $default = null): mixed
    {
        try {
            if (function_exists('app') && function_exists('config') && app()->bound('config')) {
                return config($key, $default);
            }
        } catch (\Throwable $e) {
            // Fall through to default
        }

        return $default;
    }

    /**
     * Recursively redact sensitive keys and mask sensitive string values.
     */
    public static function redactArray(array $data, ?array $redactKeys = null, int $depth = 0): array
    {
        $redactKeys ??= (array) static::config('kafka-logger.redact_keys', static::DEFAULT_REDACT_KEYS);

        if ($depth >= static::MAX_DEPTH) {
            return ['_truncated' => 'max depth reached'];
        }

        $clean = [];
        foreach ($data as $key => $value) {
            if (is_string($key) && static::isSensitiveKey($key, $redactKeys)) {
                $clean[$key] = static::REDACTED;
                continue;
            }

            if (is_array($value)) {
                $clean[$key] = static::redactArray($value, $redactKeys, $depth + 1);
            } elseif (is_string($value)) {
                $clean[$key] = static::redactString($value);
            } elseif (is_scalar($value) || $value === null) {
                $clean[$key] = $value;
            } elseif ($value instanceof \BackedEnum) {
                $clean[$key] = $value->value;
            } elseif ($value instanceof \Stringable) {
                $clean[$key] = static::redactString((string) $value);
            } else {
                // Never serialize arbitrary objects (models, requests, closures...)
                $clean[$key] = '[object ' . get_class($value) . ']';
            }
        }

        return $clean;
    }

    /**
     * Key matching on word boundaries: "user_password", "accessToken", "X-Api-Key" match,
     * while "shipping" or "spinner" do not match "pin".
     */
    public static function isSensitiveKey(string $key, array $redactKeys): bool
    {
        $normalized = strtolower(preg_replace('/([a-z0-9])([A-Z])/', '$1_$2', $key));
        $normalized = '_' . trim(preg_replace('/[^a-z0-9]+/', '_', $normalized), '_') . '_';

        foreach ($redactKeys as $term) {
            $term = trim(preg_replace('/[^a-z0-9]+/', '_', strtolower((string) $term)), '_');
            if ($term !== '' && str_contains($normalized, '_' . $term . '_')) {
                return true;
            }
        }

        return false;
    }

    /**
     * Mask secrets and SQL bound values inside free text (messages, metadata strings).
     */
    public static function redactString(string $value): string
    {
        $patterns = [
            // Laravel QueryException suffix: (Connection: pgsql, SQL: insert ... values (real@email))
            '/\(Connection:\s*([^,]+),\s*SQL:.*\)\s*$/s' => '(Connection: $1, SQL: ' . static::REDACTED . ')',
            // PostgreSQL detail lines leak key values: DETAIL: Key (email)=(a@b.c) already exists.
            '/\s*DETAIL:.*$/s' => '',
            // MySQL duplicate entry leaks the value
            "/Duplicate entry '.*?' for key/s" => "Duplicate entry '" . static::REDACTED . "' for key",
            // Authorization headers / bearer tokens
            '/\bBearer\s+[A-Za-z0-9\-._~+\/]+=*/i' => 'Bearer ' . static::REDACTED,
            // JWTs
            '/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/' => static::REDACTED,
            // key=value secrets in URLs / query strings
            '/\b(password|passwd|pwd|token|access_token|refresh_token|secret|api_key|apikey|client_secret)=([^&\s"\']+)/i' => '$1=' . static::REDACTED,
        ];

        return (string) preg_replace(array_keys($patterns), array_values($patterns), $value);
    }

    /**
     * Keyed one-way hash for correlating identifiers (e.g. failed-login usernames)
     * without storing the raw value.
     */
    public static function hashIdentifier(string $identifier): string
    {
        $key = (string) (static::config('app.key') ?: 'mimon-kafka-logger');

        return hash_hmac('sha256', strtolower(trim($identifier)), $key);
    }

    /**
     * Masked hint for email-shaped identifiers only (j***@uplb.edu.ph). Anything that is not
     * an email returns null because it might be a password typed into the username field.
     */
    public static function identifierHint(string $identifier): ?string
    {
        $identifier = trim($identifier);
        if (filter_var($identifier, FILTER_VALIDATE_EMAIL) === false) {
            return null;
        }

        [$local, $domain] = explode('@', $identifier, 2);

        return mb_substr($local, 0, 1) . '***@' . $domain;
    }
}
