<?php

namespace Mimon\KafkaLogger\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Mimon\KafkaLogger\Services\LoggerService;

/**
 * Used only when KAFKA_LOGGER_ASYNC=true. Moves the broker round-trip out of the user request.
 * Requires a real queue worker (QUEUE_CONNECTION other than "sync").
 */
class SendKafkaLogJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable;

    public int $tries = 1;

    public function __construct(
        public string $topic,
        public array $payload,
        public array $headers = [],
    ) {
    }

    public function handle(): void
    {
        LoggerService::publish($this->topic, $this->payload, $this->headers, fromQueue: true);
    }
}
