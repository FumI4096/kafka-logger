<?php

namespace Mimon\KafkaLogger\Tests;

use Mimon\KafkaLogger\Services\PayloadValidator;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use PHPUnit\Framework\TestCase;
use Illuminate\Support\Str;

class PayloadValidatorTest extends TestCase
{
    public function test_valid_payload_passes_validation(): void
    {
        $payload = [
            'log_id' => (string) Str::uuid(),
            'environment' => Environment::PRODUCTION->value,
            'application' => [
                'code' => 'HR_APP',
                'name' => 'HR_APP',
                'component' => ComponentSubsystem::DATABASE->value,
                'host_ip' => '10.200.4.15',
                'host_name' => 'node-01',
            ],
            'event' => [
                'type' => 'EMPLOYEE_CREATED',
                'level' => LogLevel::INFO->value,
                'topic' => 'HR_APP.LIVE.events',
                'message' => 'Registered new employee profile.',
            ],
            'trace' => [
                'trace_id' => 'tr-123456789012',
                'span_id' => 'sp-123456',
            ],
            'actor' => [
                'user_id' => 'usr_admin_101',
                'user_role' => 'HR_ADMIN',
                'client_ip' => '127.0.0.1',
            ],
            'timestamps' => [
                'created_at' => date('c'),
                'produced_at' => date('c'),
            ],
        ];

        $errors = PayloadValidator::validate($payload);
        $this->assertEmpty($errors);
        $this->assertTrue(PayloadValidator::isValid($payload));
    }

    public function test_invalid_payload_fails_validation(): void
    {
        $invalidPayload = [
            'log_id' => 'not-a-uuid',
            'environment' => 'INVALID_ENV',
            'event' => [
                'type' => 'invalid-lowercase-type',
                'level' => 'INVALID_LEVEL',
            ],
        ];

        $errors = PayloadValidator::validate($invalidPayload);
        $this->assertNotEmpty($errors);
        $this->assertFalse(PayloadValidator::isValid($invalidPayload));
    }

    public function test_invalid_app_code_fails_validation(): void
    {
        $payload = [
            'log_id' => (string) Str::uuid(),
            'environment' => Environment::LOCAL->value,
            'application' => [
                'code' => 'invalid-code-lowercase',
                'component' => ComponentSubsystem::APPLICATION_LOGIC->value,
                'host_ip' => '127.0.0.1',
            ],
            'event' => [
                'type' => 'TEST_EVENT',
                'level' => LogLevel::INFO->value,
                'topic' => 'TEST.TOPIC',
                'message' => 'Test message',
            ],
            'trace' => [
                'trace_id' => 'tr-123456',
            ],
            'actor' => [
                'user_id' => 'usr_1',
            ],
            'timestamps' => [
                'created_at' => date('c'),
                'produced_at' => date('c'),
            ],
        ];

        $errors = PayloadValidator::validate($payload);
        $this->assertContains("Field 'application.code' must be in UPPER_SNAKE_CASE (e.g. HR_APP, CFNR_L).", $errors);
        $this->assertFalse(PayloadValidator::isValid($payload));
    }
}
