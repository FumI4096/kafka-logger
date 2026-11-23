<?php

namespace Mimon\KafkaLogger\Listeners;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Jobs\SendKafkaLogJob;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Mimon\KafkaLogger\Support\PulseUsageBuffer;
use Throwable;

class PulseJobListener
{
    /** @var array<string, int> */
    protected static array $startedAtHr = [];

    public function handleQueued(object $event): void
    {
        try {
            $jobName = $this->resolveJobName($event);
            if ($this->shouldIgnoreJob($jobName)) {
                return;
            }

            PulseUsageBuffer::recordJobQueued();
        } catch (Throwable) {
        }
    }

    public function handleProcessing(object $event): void
    {
        try {
            $jobName = $this->resolveJobName($event);
            if ($this->shouldIgnoreJob($jobName)) {
                return;
            }

            self::$startedAtHr[$this->resolveJobKey($event)] = hrtime(true);
        } catch (Throwable) {
        }
    }

    public function handleProcessed(object $event): ?array
    {
        try {
            $jobName = $this->resolveJobName($event);
            if ($this->shouldIgnoreJob($jobName)) {
                return null;
            }

            PulseUsageBuffer::recordJobProcessed();
            $durationMs = $this->popDurationMs($event);

            $payload = $this->recordIfSlow(
                jobClass: $jobName,
                queue: $this->resolveQueue($event),
                connection: (string) ($event->connectionName ?? 'default'),
                durationMs: $durationMs,
                status: 'processed',
                attempts: $this->resolveAttempts($event),
            );

            PulseUsageBuffer::maybeFlush();

            return $payload;
        } catch (Throwable) {
            return null;
        }
    }

    public function handleFailed(object $event): ?array
    {
        try {
            $jobName = $this->resolveJobName($event);
            if ($this->shouldIgnoreJob($jobName)) {
                return null;
            }

            PulseUsageBuffer::recordJobFailed();
            $durationMs = $this->popDurationMs($event);

            return $this->recordIfSlow(
                jobClass: $jobName,
                queue: $this->resolveQueue($event),
                connection: (string) ($event->connectionName ?? 'default'),
                durationMs: $durationMs,
                status: 'failed',
                attempts: $this->resolveAttempts($event),
            );
        } catch (Throwable) {
            return null;
        }
    }

    public function recordIfSlow(
        string $jobClass,
        string $queue,
        string $connection,
        int $durationMs,
        string $status = 'processed',
        int $attempts = 1,
    ): ?array {
        try {
            if (
                $this->shouldIgnoreJob($jobClass)
                || ! PayloadSanitizer::config('kafka-logger.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.slow_jobs.enabled', true)
            ) {
                return null;
            }

            $thresholdMs = max(1, (int) PayloadSanitizer::config('kafka-logger.pulse.slow_jobs.threshold_ms', 1000));
            if ($durationMs < $thresholdMs) {
                return null;
            }

            $payload = LoggerService::createEnterpriseLog(
                eventType: 'PULSE_SLOW_JOB',
                message: "Slow queue job {$jobClass} on [{$queue}] took {$durationMs}ms ({$status}).",
                level: $status === 'failed' || $durationMs >= ($thresholdMs * 3) ? LogLevel::WARNING : LogLevel::INFO,
                component: ComponentSubsystem::QUEUE_JOB,
                userId: 'SYSTEM',
                userRole: 'SYSTEM',
                metadata: [
                    'signature_hash' => sha1($queue . ':' . $jobClass),
                    'job_class' => $jobClass,
                    'queue' => $queue,
                    'connection' => $connection,
                    'duration_ms' => $durationMs,
                    'threshold_ms' => $thresholdMs,
                    'status' => $status,
                    'attempts' => $attempts,
                    'host' => gethostname() ?: 'unknown-host',
                ],
            );

            try {
                LoggerService::dispatchEnterpriseLog($payload);
            } catch (Throwable) {
            }

            return $payload;
        } catch (Throwable) {
            return null;
        }
    }

    protected function shouldIgnoreJob(string $jobName): bool
    {
        return $jobName === ''
            || $jobName === SendKafkaLogJob::class
            || str_contains($jobName, 'SendKafkaLogJob');
    }

    protected function resolveJobName(object $event): string
    {
        if (isset($event->job) && is_object($event->job)) {
            if (method_exists($event->job, 'resolveName')) {
                return (string) $event->job->resolveName();
            }

            return get_class($event->job);
        }

        return isset($event->job) && is_string($event->job) ? $event->job : '';
    }

    protected function resolveJobKey(object $event): string
    {
        if (isset($event->job) && is_object($event->job)) {
            if (method_exists($event->job, 'uuid') && $event->job->uuid()) {
                return (string) $event->job->uuid();
            }

            return (string) spl_object_id($event->job);
        }

        return (string) spl_object_id($event);
    }

    protected function resolveQueue(object $event): string
    {
        if (isset($event->job) && is_object($event->job) && method_exists($event->job, 'getQueue')) {
            return (string) ($event->job->getQueue() ?: 'default');
        }

        return 'default';
    }

    protected function resolveAttempts(object $event): int
    {
        if (isset($event->job) && is_object($event->job) && method_exists($event->job, 'attempts')) {
            return max(1, (int) $event->job->attempts());
        }

        return 1;
    }

    protected function popDurationMs(object $event): int
    {
        $key = $this->resolveJobKey($event);
        if (! isset(self::$startedAtHr[$key])) {
            return 0;
        }

        $started = self::$startedAtHr[$key];
        unset(self::$startedAtHr[$key]);

        return max(0, (int) round((hrtime(true) - $started) / 1e6));
    }
}
