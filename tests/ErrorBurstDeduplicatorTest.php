<?php

namespace Mimon\KafkaLogger\Tests;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\Environment;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\ErrorBurstDeduplicator;
use PHPUnit\Framework\TestCase;

class ErrorBurstDeduplicatorTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        ErrorBurstDeduplicator::reset();
        LoggerService::resetThroughputWindow();
    }

    protected function tearDown(): void
    {
        ErrorBurstDeduplicator::reset();
        parent::tearDown();
    }

    public function test_fingerprint_normalizes_dynamic_tokens_and_respects_file_line(): void
    {
        $payloadA = [
            'application' => ['code' => 'CFNR_L', 'component' => 'DATABASE'],
            'event' => [
                'type' => 'DATABASE_QUERY_ERROR',
                'level' => 'ERROR',
                'message' => 'Failed query for user 550e8400-e29b-41d4-a716-446655440000 at 2026-11-27T08:15:00Z row 1024 hash deadbeef1234567890abcdef',
            ],
            'error' => ['code' => '08006'],
            'metadata' => ['file' => '/var/www/app/Services/SyncService.php', 'line' => 88],
        ];

        $payloadB = [
            'application' => ['code' => 'CFNR_L', 'component' => 'DATABASE'],
            'event' => [
                'type' => 'DATABASE_QUERY_ERROR',
                'level' => 'ERROR',
                'message' => 'Failed query for user 11111111-2222-3333-4444-555555555555 at 2026-11-27T08:15:01Z row 9999 hash 0123456789abcdef01234567',
            ],
            'error' => ['code' => '08006'],
            'metadata' => ['file' => '/opt/release/app/Services/SyncService.php', 'line' => 88],
        ];

        $payloadDifferentLine = $payloadB;
        $payloadDifferentLine['metadata']['line'] = 120;

        $this->assertSame(
            ErrorBurstDeduplicator::fingerprint($payloadA, true),
            ErrorBurstDeduplicator::fingerprint($payloadB, true),
        );

        $this->assertNotSame(
            ErrorBurstDeduplicator::fingerprint($payloadA, true),
            ErrorBurstDeduplicator::fingerprint($payloadDifferentLine, true),
        );
    }

    public function test_twenty_identical_errors_within_1500ms_produce_single_instance_with_count(): void
    {
        $nowMs = 1_700_000_000_000;
        ErrorBurstDeduplicator::setClock(static function () use (&$nowMs): int {
            return $nowMs;
        });

        $published = [];
        ErrorBurstDeduplicator::setPublisher(static function (array $topics, array $payload) use (&$published): void {
            $published[] = ['topics' => $topics, 'payload' => $payload];
        });

        $returnedNonEmpty = 0;
        for ($i = 1; $i <= 20; $i++) {
            $nowMs += 50; // 20 * 50ms = 1000ms (< 1500ms)
            $res = LoggerService::createEnterpriseLog(
                appCode: 'CFNR_L',
                eventType: 'DATABASE_QUERY_ERROR',
                message: "SQLSTATE[08006]: Connection refused on row {$i}",
                level: LogLevel::ERROR,
                component: ComponentSubsystem::DATABASE,
                metadata: ['file' => '/app/Services/Repo.php', 'line' => 42],
                environment: Environment::LOCAL,
            );
            if (! empty($res)) {
                $returnedNonEmpty++;
            }
        }

        // Only the 1st error returned a representative payload; #2..#20 were suppressed
        $this->assertSame(1, $returnedNonEmpty);
        $this->assertSame(1, ErrorBurstDeduplicator::getBufferedCount());
        $this->assertCount(0, $published); // Deferred until flush/terminate

        // Flush on request termination -> emits exactly 1 Kafka payload with count = 20
        $flushed = ErrorBurstDeduplicator::flush();

        $this->assertCount(1, $flushed);
        $this->assertCount(1, $published);
        $meta = $published[0]['payload']['metadata'];
        $this->assertSame(20, $meta['_occurrence_count']);
        $this->assertSame(19, $meta['_suppressed_duplicates']);
        $this->assertSame(1500, $meta['_dedup_window_ms']);
        $this->assertNotEmpty($meta['_dedup_fingerprint']);
        $this->assertNotEmpty($meta['_first_seen_at']);
        $this->assertNotEmpty($meta['_last_seen_at']);
    }

    public function test_distinct_errors_within_1500ms_each_emit_one_instance(): void
    {
        $nowMs = 1_700_000_000_000;
        ErrorBurstDeduplicator::setClock(static function () use (&$nowMs): int {
            return $nowMs;
        });

        $published = [];
        ErrorBurstDeduplicator::setPublisher(static function (array $topics, array $payload) use (&$published): void {
            $published[] = $payload;
        });

        // 5 DB errors + 3 External API errors inside 300ms
        for ($i = 1; $i <= 5; $i++) {
            $nowMs += 20;
            LoggerService::createEnterpriseLog(
                appCode: 'CFNR_L',
                eventType: 'DATABASE_QUERY_ERROR',
                message: 'Database connection timed out',
                level: LogLevel::ERROR,
                component: ComponentSubsystem::DATABASE,
                environment: Environment::LOCAL,
            );
        }

        for ($i = 1; $i <= 3; $i++) {
            $nowMs += 20;
            LoggerService::createEnterpriseLog(
                appCode: 'CFNR_L',
                eventType: 'EXTERNAL_API_ERROR',
                message: 'HTTP 502 Bad Gateway from HRIS',
                level: LogLevel::ERROR,
                component: ComponentSubsystem::EXTERNAL_API,
                environment: Environment::LOCAL,
            );
        }

        ErrorBurstDeduplicator::flush();

        $this->assertCount(2, $published);
        $byType = [];
        foreach ($published as $p) {
            $byType[$p['event']['type']] = $p['metadata']['_occurrence_count'];
        }

        $this->assertSame(5, $byType['DATABASE_QUERY_ERROR']);
        $this->assertSame(3, $byType['EXTERNAL_API_ERROR']);
    }

    public function test_error_after_1500ms_flushes_previous_window_and_starts_new_one(): void
    {
        $nowMs = 1_700_000_000_000;
        ErrorBurstDeduplicator::setClock(static function () use (&$nowMs): int {
            return $nowMs;
        });

        $published = [];
        ErrorBurstDeduplicator::setPublisher(static function (array $topics, array $payload) use (&$published): void {
            $published[] = $payload;
        });

        // Burst 1: 4 occurrences within 200ms
        for ($i = 1; $i <= 4; $i++) {
            $nowMs += 50;
            LoggerService::createEnterpriseLog(
                appCode: 'CFNR_L',
                eventType: 'RUNTIME_ERROR',
                message: 'Cache write timeout',
                level: LogLevel::CRITICAL,
                environment: Environment::LOCAL,
            );
        }

        // Advance clock past 1500ms -> next error auto-flushes Burst 1 and opens Burst 2
        $nowMs += 1600;
        LoggerService::createEnterpriseLog(
            appCode: 'CFNR_L',
            eventType: 'RUNTIME_ERROR',
            message: 'Cache write timeout',
            level: LogLevel::CRITICAL,
            environment: Environment::LOCAL,
        );

        $this->assertCount(1, $published);
        $this->assertSame(4, $published[0]['metadata']['_occurrence_count']);
        $this->assertSame(3, $published[0]['metadata']['_suppressed_duplicates']);

        // Flush Burst 2
        ErrorBurstDeduplicator::flush();
        $this->assertCount(2, $published);
        $this->assertSame(1, $published[1]['metadata']['_occurrence_count']);
        $this->assertSame(0, $published[1]['metadata']['_suppressed_duplicates']);
    }

    public function test_info_and_warning_logs_are_never_buffered_by_error_deduplicator(): void
    {
        $resInfo = LoggerService::createEnterpriseLog(
            appCode: 'CFNR_L',
            eventType: 'USER_LOGIN',
            message: 'User signed in',
            level: LogLevel::INFO,
            environment: Environment::LOCAL,
        );

        $resWarn = LoggerService::createEnterpriseLog(
            appCode: 'CFNR_L',
            eventType: 'SLOW_ENDPOINT',
            message: 'Endpoint took 1200ms',
            level: LogLevel::WARNING,
            environment: Environment::LOCAL,
        );

        $this->assertNotEmpty($resInfo);
        $this->assertNotEmpty($resWarn);
        $this->assertSame(0, ErrorBurstDeduplicator::getBufferedCount());
        $this->assertArrayNotHasKey('_occurrence_count', $resInfo['metadata']);
    }
}
