<?php

namespace Mimon\KafkaLogger\Tests;

use Mimon\KafkaLogger\Http\Middleware\PulseSlowRequestMiddleware;
use Mimon\KafkaLogger\Jobs\SendKafkaLogJob;
use Mimon\KafkaLogger\Listeners\PulseCacheListener;
use Mimon\KafkaLogger\Listeners\PulseJobListener;
use Mimon\KafkaLogger\Listeners\PulseOutgoingRequestListener;
use Mimon\KafkaLogger\Listeners\PulseSlowQueryListener;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Services\PayloadValidator;
use Mimon\KafkaLogger\Support\PulseUsageBuffer;
use Mimon\KafkaLogger\Support\ServerVitalsCollector;
use PHPUnit\Framework\TestCase;

class PulseRecordersTest extends TestCase
{
    public function test_package_version_is_bumped_to_1_2_7(): void
    {
        $this->assertSame('1.2.7', LoggerService::PACKAGE_VERSION);
    }

    public function test_server_vitals_collector_produces_schema_valid_heartbeat_payload(): void
    {
        $vitals = ServerVitalsCollector::collect();

        $this->assertNotEmpty($vitals['host']);
        $this->assertGreaterThanOrEqual(1, $vitals['cpu_cores']);
        $this->assertGreaterThanOrEqual(0.0, $vitals['cpu_percent']);
        $this->assertLessThanOrEqual(100.0, $vitals['cpu_percent']);
        $this->assertGreaterThanOrEqual(0, $vitals['memory_used_mb']);
        $this->assertGreaterThan(0, $vitals['memory_total_mb']);
        $this->assertSame(PHP_VERSION, $vitals['php_version']);
        $this->assertSame('1.2.7', $vitals['logger_version']);

        $payload = LoggerService::createEnterpriseLog(
            appCode: 'CFNR_L',
            eventType: 'PULSE_SERVER_HEARTBEAT',
            message: 'Server heartbeat test',
            userId: 'SYSTEM',
            userRole: 'SYSTEM',
            metadata: $vitals,
        );

        $this->assertSame([], PayloadValidator::validate($payload));
        $this->assertSame('PULSE_SERVER_HEARTBEAT', $payload['event']['type']);
    }

    public function test_slow_query_listener_emits_normalized_sql_only_above_threshold_and_never_includes_bindings(): void
    {
        $listener = new PulseSlowQueryListener();

        // Fast query below 500ms default threshold -> ignored
        $fast = (object) [
            'sql' => 'select * from users where email = ?',
            'bindings' => ['secret@up.edu.ph'],
            'time' => 120.5,
            'connectionName' => 'pgsql',
        ];
        $this->assertNull($listener->handle($fast));

        // Internal CLS hub table query -> ignored to prevent feedback loops
        $internal = (object) [
            'sql' => 'insert into "system_logs" ("message") values (?)',
            'bindings' => ['hi'],
            'time' => 900.0,
            'connectionName' => 'pgsql',
        ];
        $this->assertNull($listener->handle($internal));

        // Slow query >= 500ms -> emits normalized SQL without raw binding value
        $slow = (object) [
            'sql' => "select   id, email\n  from cfnr_personnels\n  where email = ?",
            'bindings' => ['private-secret-value@up.edu.ph'],
            'time' => 780.4,
            'connectionName' => 'pgsql',
        ];

        $payload = $listener->handle($slow);
        $this->assertIsArray($payload);
        $this->assertSame([], PayloadValidator::validate($payload));
        $this->assertSame('PULSE_SLOW_QUERY', $payload['event']['type']);
        $this->assertSame('DATABASE', $payload['application']['component']);
        $this->assertSame('select id, email from cfnr_personnels where email = ?', $payload['metadata']['normalized_sql']);
        $this->assertSame(780, $payload['metadata']['duration_ms']);
        $this->assertStringNotContainsString('private-secret-value@up.edu.ph', (string) json_encode($payload));
    }

    public function test_slow_request_middleware_emits_only_above_threshold_and_respects_ignore_paths(): void
    {
        $middleware = new PulseSlowRequestMiddleware();

        // Below 1000ms threshold -> null
        $this->assertNull($middleware->recordRequest('GET', '/personnels', '/personnels', 250, 200));

        // Ignored internal path -> null
        $this->assertNull($middleware->recordRequest('POST', '/livewire/update', '/livewire/update', 2500, 200));

        // Slow request >= 1000ms -> emits schema-valid PULSE_SLOW_REQUEST
        $payload = $middleware->recordRequest('GET', '/personnels', '/personnels', 1850, 200, 'ListPersonnels', '42', 'dev@up.edu.ph');

        $this->assertIsArray($payload);
        $this->assertSame([], PayloadValidator::validate($payload));
        $this->assertSame('PULSE_SLOW_REQUEST', $payload['event']['type']);
        $this->assertSame('GET', $payload['metadata']['method']);
        $this->assertSame('/personnels', $payload['metadata']['uri']);
        $this->assertSame(1850, $payload['metadata']['duration_ms']);
        $this->assertSame(200, $payload['metadata']['status_code']);
    }

    public function test_job_listener_records_slow_jobs_and_ignores_send_kafka_log_job(): void
    {
        $listener = new PulseJobListener();

        // Anti-recursion: SendKafkaLogJob must always be ignored
        $this->assertNull($listener->recordIfSlow(SendKafkaLogJob::class, 'default', 'redis', 5000));

        // Below 1000ms threshold -> null
        $this->assertNull($listener->recordIfSlow('App\\Jobs\\SyncPersonnelJob', 'default', 'redis', 300));

        // Slow job >= 1000ms -> emits schema-valid PULSE_SLOW_JOB
        $payload = $listener->recordIfSlow('App\\Jobs\\SyncPersonnelJob', 'imports', 'redis', 2400, 'processed', 1);

        $this->assertIsArray($payload);
        $this->assertSame([], PayloadValidator::validate($payload));
        $this->assertSame('PULSE_SLOW_JOB', $payload['event']['type']);
        $this->assertSame('QUEUE_JOB', $payload['application']['component']);
        $this->assertSame('App\\Jobs\\SyncPersonnelJob', $payload['metadata']['job_class']);
        $this->assertSame('imports', $payload['metadata']['queue']);
        $this->assertSame(2400, $payload['metadata']['duration_ms']);
    }

    public function test_outgoing_request_listener_records_slow_and_failed_external_calls(): void
    {
        $listener = new PulseOutgoingRequestListener();

        // Fast call -> null
        $this->assertNull($listener->recordOutgoing('GET', 'https://tuklas.uplb.edu.ph/api/reps', 250, 200));

        // Slow call >= 1000ms -> emits schema-valid PULSE_SLOW_OUTGOING_REQUEST
        $slow = $listener->recordOutgoing('POST', 'https://tuklas.uplb.edu.ph/api/workload?token=secret', 1950, 200);
        $this->assertIsArray($slow);
        $this->assertSame([], PayloadValidator::validate($slow));
        $this->assertSame('PULSE_SLOW_OUTGOING_REQUEST', $slow['event']['type']);
        $this->assertSame('EXTERNAL_API', $slow['application']['component']);
        $this->assertSame('tuklas.uplb.edu.ph', $slow['metadata']['host']);
        $this->assertSame('/api/workload', $slow['metadata']['path']);
        $this->assertStringNotContainsString('secret', (string) json_encode($slow));

        // Connection failed even under 1000ms -> always recorded
        $failed = $listener->recordOutgoing('GET', 'https://sso.uplb.edu.ph/oauth/user', 120, 0, failed: true);
        $this->assertIsArray($failed);
        $this->assertTrue($failed['metadata']['connection_failed']);
    }

    public function test_usage_buffer_and_cache_listener_aggregate_and_emit_single_rollup(): void
    {
        PulseUsageBuffer::reset();

        $cacheListener = new PulseCacheListener();
        $cacheListener->handleHit((object) ['key' => 'users:42:permissions']);
        $cacheListener->handleHit((object) ['key' => 'users:99:permissions']);
        $cacheListener->handleMissed((object) ['key' => 'mimon_kafka_logger:pulse:usage_state']); // ignored internal key
        $cacheListener->handleMissed((object) ['key' => 'offices:list']);

        PulseUsageBuffer::recordRequest('7', 'dev@up.edu.ph', false);
        PulseUsageBuffer::recordRequest('7', 'dev@up.edu.ph', true);
        PulseUsageBuffer::recordJobQueued();
        PulseUsageBuffer::recordJobProcessed();

        $payload = PulseUsageBuffer::flushNow(60);

        $this->assertIsArray($payload);
        $this->assertSame([], PayloadValidator::validate($payload));
        $this->assertSame('PULSE_USAGE_ROLLUP', $payload['event']['type']);
        $this->assertSame(2, $payload['metadata']['total_requests']);
        $this->assertSame(2, $payload['metadata']['cache_hits']);
        $this->assertSame(1, $payload['metadata']['cache_misses']);
        $this->assertSame(1, $payload['metadata']['jobs_queued']);
        $this->assertSame(1, $payload['metadata']['jobs_processed']);
        $this->assertSame('users:*:permissions', $payload['metadata']['top_cache_keys'][0]['key']);
        $this->assertSame(2, $payload['metadata']['top_cache_keys'][0]['hits']);
        $this->assertSame('dev@up.edu.ph', $payload['metadata']['top_users'][0]['label']);
        $this->assertSame(2, $payload['metadata']['top_users'][0]['requests']);

        // Second flush with no new activity emits nothing
        $this->assertNull(PulseUsageBuffer::flushNow(60));
    }
}
