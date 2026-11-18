<?php

namespace Mimon\KafkaLogger\Tests;

use Mimon\KafkaLogger\Listeners\LogErrorEvents;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\LogContext;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use RuntimeException;

class LogContextAndErrorEnrichmentTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        LogContext::clear();
        LogErrorEvents::flushLoggedExceptions();
    }

    protected function tearDown(): void
    {
        LogContext::clear();
        parent::tearDown();
    }

    public function test_log_context_propagates_shared_trace_id_and_metadata_to_enterprise_logs(): void
    {
        LogContext::set([
            'module' => 'TIMEKEEPING',
            'workflow' => 'TIME_IN',
            'operation' => 'TimekeepingRpc@timeIn',
        ]);

        $first = LoggerService::createEnterpriseLog(
            appCode: 'TKS_APP',
            eventType: 'TIME_IN_VALIDATED',
            message: 'Step 1 validated'
        );

        $second = LoggerService::createEnterpriseLog(
            appCode: 'TKS_APP',
            eventType: 'TIME_IN_SAVED',
            message: 'Step 2 saved'
        );

        $this->assertNotEmpty($first['trace']['trace_id']);
        $this->assertSame($first['trace']['trace_id'], $second['trace']['trace_id']);
        $this->assertSame('TIMEKEEPING', $first['metadata']['module']);
        $this->assertSame('TIME_IN', $second['metadata']['workflow']);
        $this->assertSame('TimekeepingRpc@timeIn', $second['metadata']['operation']);
    }

    public function test_log_context_run_with_restores_outer_scope_after_exception(): void
    {
        LogContext::set(['module' => 'OUTER_MODULE', 'trace_id' => 'outer-trace-1']);

        try {
            LogContext::runWith(['module' => 'INNER_MODULE', 'trace_id' => 'inner-trace-2'], function (): void {
                $this->assertSame('inner-trace-2', LogContext::traceId());
                $this->assertSame('INNER_MODULE', LogContext::get()['module']);
                throw new RuntimeException('Boom');
            });
            $this->fail('Expected RuntimeException');
        } catch (RuntimeException) {
            // Expected
        }

        $this->assertSame('outer-trace-1', LogContext::traceId());
        $this->assertSame('OUTER_MODULE', LogContext::get()['module']);
    }

    public function test_log_error_events_enriches_500_metadata_and_previous_exceptions(): void
    {
        $listener = new LogErrorEvents();
        $method = new ReflectionMethod($listener, 'buildMetadata');
        $method->setAccessible(true);

        $root = new PDOException('SQLSTATE[42P01]: Undefined table');
        $wrapped = new RuntimeException('Service failed', 0, $root);

        $metadata = $method->invoke($listener, [], $wrapped);

        $this->assertSame(500, $metadata['http_status']);
        $this->assertSame('INTERNAL_SERVER_ERROR', $metadata['error_category']);
        $this->assertCount(1, $metadata['previous_exceptions']);
        $this->assertSame(PDOException::class, $metadata['previous_exceptions'][0]['class']);
        $this->assertStringContainsString('Undefined table', $metadata['previous_exceptions'][0]['message']);
    }

    public function test_mark_as_logged_prevents_duplicate_handling_in_listener(): void
    {
        $exception = new RuntimeException('Already logged by transaction');
        LogErrorEvents::markAsLogged($exception);

        $listener = new LogErrorEvents();
        // Should return early without throwing or attempting to resolve Laravel container
        $listener->handleMessageLogged((object) [
            'level' => 'error',
            'message' => 'Already logged by transaction',
            'context' => ['exception' => $exception],
        ]);

        $this->assertTrue(true);
    }
}
