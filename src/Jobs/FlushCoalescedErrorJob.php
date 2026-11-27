<?php

namespace Mimon\KafkaLogger\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Mimon\KafkaLogger\Support\ErrorBurstDeduplicator;

/**
 * Used when KAFKA_LOGGER_ASYNC=true (or flush_mode=delayed_job).
 * Waits for the 1.5s error burst window to close, then publishes a single
 * Kafka error log carrying the final combined cross-process occurrence count.
 */
class FlushCoalescedErrorJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    public int $tries = 1;

    public function __construct(
        public string $fingerprint,
    ) {
    }

    public function handle(): void
    {
        ErrorBurstDeduplicator::flushDelayedJobEntry($this->fingerprint);
    }
}
