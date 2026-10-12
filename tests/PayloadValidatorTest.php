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
}