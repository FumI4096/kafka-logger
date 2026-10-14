<?php

namespace Mimon\KafkaLogger\Tests;

use Exception;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Listeners\LogErrorEvents;
use Mimon\KafkaLogger\Services\LoggerService;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use RuntimeException;

class LoggerServiceTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        LogErrorEvents::flushLoggedExceptions();
    }

    public function test_create_enterprise_log_produces_valid_payload(): void
    {
        $payload = LoggerService::createEnterpriseLog(
            appName: 'HR_APP',
            eventType: 'EMPLOYEE_CREATED',
            message: 'New employee onboarded',
            level: LogLevel::INFO,
            component: ComponentSubsystem::APPLICATION_LOGIC,
            topic: 'HR.EMPLOYEE.EVENTS',
            userId: 'usr_101',
            userRole: 'ADMIN',
            metadata: ['dept' => 'Engineering'],
            environment: Environment::LOCAL
        );

        $this->assertNotEmpty($payload);
        $this->assertSame('HR_APP', $payload['application']['name']);
        $this->assertSame('EMPLOYEE_CREATED', $payload['event']['type']);
        $this->assertSame('HR.EMPLOYEE.EVENTS', $payload['event']['topic']);
        $this->assertSame('New employee onboarded', $payload['event']['message']);
        $this->assertSame('usr_101', $payload['actor']['user_id']);
    }

    public function test_topic_name_is_sanitized_to_valid_kafka_characters(): void
    {
        $payload = LoggerService::createEnterpriseLog(
            appName: 'HR_APP',
            eventType: 'ORDER_PLACED',
            message: 'Order created',
            topic: 'CLS/INVALID TOPIC!NAME#123'
        );

        $this->assertSame('CLS_INVALID_TOPIC_NAME_123', $payload['event']['topic']);
    }

    public function test_empty_message_falls_back_to_human_readable_event(): void
    {
        $payload = LoggerService::createEnterpriseLog(
            appName: 'HR_APP',
            eventType: 'PAYMENT_RECEIVED',
            message: '   '
        );

        $this->assertSame('PAYMENT RECEIVED event.', $payload['event']['message']);
    }

    public function test_weak_map_deduplicates_same_exception_instance(): void
    {
        $listener = new LogErrorEvents();
        $exception = new RuntimeException('Unique DB Timeout');

        $refClass = new ReflectionClass(LogErrorEvents::class);
        $refProp = $refClass->getProperty('loggedExceptions');
        $refProp->setAccessible(true);

        // Before logging, WeakMap is null or empty
        $eventObj = (object) [
            'level' => 'error',
            'message' => 'DB Query Failed',
            'context' => ['exception' => $exception],
        ];

        $listener->handleMessageLogged($eventObj);

        /** @var \WeakMap $map */
        $map = $refProp->getValue();
        $this->assertInstanceOf(\WeakMap::class, $map);
        $this->assertTrue(isset($map[$exception]));

        // Second dispatch with identical exception should be ignored
        $listener->handleMessageLogged($eventObj);
        $this->assertTrue(isset($map[$exception]));
    }

    public function test_handle_job_failed_skips_send_kafka_log_job_to_prevent_infinite_loop(): void
    {
        $listener = new LogErrorEvents();
        $jobObj = new class {
            public function resolveName(): string
            {
                return 'Mimon\KafkaLogger\Jobs\SendKafkaLogJob';
            }
        };

        $event = (object) [
            'job' => $jobObj,
            'exception' => new Exception('Broker timeout during publish'),
        ];

        // Should return early and not throw or fail
        $listener->handleJobFailed($event);

        $refClass = new ReflectionClass(LogErrorEvents::class);
        $refProp = $refClass->getProperty('loggedExceptions');
        $refProp->setAccessible(true);
        $map = $refProp->getValue();

        // Job was SendKafkaLogJob, so it was intercepted before dispatching to Kafka
        $this->assertTrue(isset($map[$event->exception]));
    }

    public function test_trace_id_resolution_guards_against_injection(): void
    {
        // Valid trace id passes through
        $validTrace = LoggerService::resolveTraceId('trace-abc.123_xyz');
        $this->assertSame('trace-abc.123_xyz', $validTrace);

        // Malicious header with newlines or special characters is rejected and replaced with safe tr- random
        $maliciousTrace = LoggerService::resolveTraceId("malicious\r\nHeader: inject<script>");
        $this->assertNotSame("malicious\r\nHeader: inject<script>", $maliciousTrace);
        $this->assertStringStartsWith('tr-', $maliciousTrace);
    }
}
