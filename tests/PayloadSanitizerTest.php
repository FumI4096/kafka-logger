<?php

namespace Mimon\KafkaLogger\Tests;

use Exception;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Mimon\KafkaLogger\Support\SafeTrace;
use PHPUnit\Framework\TestCase;

class PayloadSanitizerTest extends TestCase
{
    public function test_redacts_sensitive_keys_recursively(): void
    {
        $input = [
            'username' => 'user',
            'password' => 'secret_pass123',
            'user' => [
                'access_token' => 'xyz-token-abc',
                'nested' => [
                    'credit_card' => '4111-2222-3333-4444',
                    'cvv' => '123',
                    'api_key' => 'live_sk_9999',
                    'public_info' => 'visible',
                ],
            ],
        ];

        $clean = PayloadSanitizer::redactArray($input);

        $this->assertSame('user', $clean['username']);
        $this->assertSame(PayloadSanitizer::REDACTED, $clean['password']);
        $this->assertSame(PayloadSanitizer::REDACTED, $clean['user']['access_token']);
        $this->assertSame(PayloadSanitizer::REDACTED, $clean['user']['nested']['credit_card']);
        $this->assertSame(PayloadSanitizer::REDACTED, $clean['user']['nested']['cvv']);
        $this->assertSame(PayloadSanitizer::REDACTED, $clean['user']['nested']['api_key']);
        $this->assertSame('visible', $clean['user']['nested']['public_info']);
    }

    public function test_redacts_tokens_and_jwt_in_strings(): void
    {
        $rawMessage = 'Request failed with Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.doNotLeakThisToken and password=superSecretPass123';
        $sanitized = PayloadSanitizer::redactString($rawMessage);

        $this->assertStringNotContainsString('superSecretPass123', $sanitized);
        $this->assertStringNotContainsString('doNotLeakThisToken', $sanitized);
        $this->assertStringContainsString('password=' . PayloadSanitizer::REDACTED, $sanitized);
        $this->assertStringContainsString('Bearer ' . PayloadSanitizer::REDACTED, $sanitized);
    }

    public function test_redacts_query_exception_sql_bindings(): void
    {
        $queryError = 'SQLSTATE[23505]: Unique violation: 7 ERROR: duplicate key (Connection: pgsql, SQL: insert into users (email) values (user@secret.com))';
        $sanitized = PayloadSanitizer::redactString($queryError);

        $this->assertStringNotContainsString('user@secret.com', $sanitized);
        $this->assertStringContainsString('(Connection: pgsql, SQL: ' . PayloadSanitizer::REDACTED . ')', $sanitized);
    }

    public function test_hashes_failed_login_identifier(): void
    {
        $rawIdentifier = 'user@uplb.edu.ph';
        $hash = PayloadSanitizer::hashIdentifier($rawIdentifier);

        $this->assertNotSame($rawIdentifier, $hash);
        $this->assertSame(64, strlen($hash)); // sha256 hex string

        // Deterministic for same identifier
        $this->assertSame($hash, PayloadSanitizer::hashIdentifier('  USER@UPLB.EDU.PH '));
    }

    public function test_masks_email_hint_and_hides_non_email(): void
    {
        $this->assertSame('u***@uplb.edu.ph', PayloadSanitizer::identifierHint('user@uplb.edu.ph'));
        $this->assertSame('j***@gmail.com', PayloadSanitizer::identifierHint('john.doe@gmail.com'));

        // If user accidentally typed password into username field, hint MUST be null
        $this->assertNull(PayloadSanitizer::identifierHint('MySecretPassword123!'));
    }

    public function test_safe_trace_omits_function_arguments(): void
    {
        $secretArg = 'super_secret_argument_value_9999';
        $fn = function ($arg) {
            throw new Exception('Trace test exception');
        };

        try {
            $fn($secretArg);
            $this->fail('Exception not thrown');
        } catch (Exception $e) {
            $trace = SafeTrace::format($e, 10);
            $this->assertStringNotContainsString($secretArg, $trace);
            $this->assertStringContainsString('Exception @ ', $trace);
        }
    }

    public function test_enforces_payload_size_limit(): void
    {
        $hugePayload = [
            'event' => [
                'message' => str_repeat('A', 5000),
            ],
            'error' => [
                'stack_trace' => str_repeat("#0 file.php(10): func()\n", 500),
            ],
            'metadata' => [
                'huge_data' => str_repeat('B', 100000),
            ],
        ];

        $capped = LoggerService::enforceSizeLimit($hugePayload, 5000);
        $encoded = json_encode($capped);

        $this->assertLessThanOrEqual(5000, strlen($encoded));
        $this->assertStringContainsString('truncated', $capped['event']['message']);
    }

    public function test_validates_and_sanitizes_trace_id(): void
    {
        $validTrace = 'tr-valid_trace.id-123';
        $this->assertSame($validTrace, LoggerService::resolveTraceId($validTrace));

        $invalidTrace = 'malicious<script>alert(1)</script>';
        $resolved = LoggerService::resolveTraceId($invalidTrace);
        $this->assertStringStartsWith('tr-', $resolved);
        $this->assertStringNotContainsString('script', $resolved);
    }
}
