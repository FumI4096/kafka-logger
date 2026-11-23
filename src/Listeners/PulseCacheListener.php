<?php

namespace Mimon\KafkaLogger\Listeners;

use Mimon\KafkaLogger\Support\PulseUsageBuffer;
use Throwable;

class PulseCacheListener
{
    public function handleHit(object $event): void
    {
        try {
            if (PulseUsageBuffer::isInternalOperation()) {
                return;
            }

            $key = isset($event->key) ? (string) $event->key : '';
            if ($key !== '') {
                PulseUsageBuffer::recordCache($key, hit: true);
            }
        } catch (Throwable) {
        }
    }

    public function handleMissed(object $event): void
    {
        try {
            if (PulseUsageBuffer::isInternalOperation()) {
                return;
            }

            $key = isset($event->key) ? (string) $event->key : '';
            if ($key !== '') {
                PulseUsageBuffer::recordCache($key, hit: false);
            }
        } catch (Throwable) {
        }
    }
}
