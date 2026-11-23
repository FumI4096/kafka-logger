<?php

namespace Mimon\KafkaLogger\Listeners;

use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Throwable;

class PulseOutgoingRequestListener
{
    /** @var array<int, int> */
    protected static array $startedAtHr = [];

    public function handleSending(object $event): void
    {
        try {
            if (isset($event->request) && is_object($event->request)) {
                self::$startedAtHr[spl_object_id($event->request)] = hrtime(true);
            }
        } catch (Throwable) {
        }
    }

    public function handleResponse(object $event): ?array
    {
        try {
            if (! isset($event->request) || ! is_object($event->request)) {
                return null;
            }

            $durationMs = $this->popDurationMs($event->request);
            $method = method_exists($event->request, 'method') ? strtoupper((string) $event->request->method()) : 'GET';
            $url = method_exists($event->request, 'url') ? (string) $event->request->url() : '';
            $statusCode = isset($event->response) && is_object($event->response) && method_exists($event->response, 'status')
                ? (int) $event->response->status()
                : 200;

            return $this->recordOutgoing($method, $url, $durationMs, $statusCode, failed: false);
        } catch (Throwable) {
            return null;
        }
    }

    public function handleConnectionFailed(object $event): ?array
    {
        try {
            if (! isset($event->request) || ! is_object($event->request)) {
                return null;
            }

            $durationMs = $this->popDurationMs($event->request);
            $method = method_exists($event->request, 'method') ? strtoupper((string) $event->request->method()) : 'GET';
            $url = method_exists($event->request, 'url') ? (string) $event->request->url() : '';

            return $this->recordOutgoing($method, $url, $durationMs, 0, failed: true);
        } catch (Throwable) {
            return null;
        }
    }

    public function recordOutgoing(
        string $method,
        string $url,
        int $durationMs,
        int $statusCode = 200,
        bool $failed = false,
    ): ?array {
        try {
            if (
                $url === ''
                || ! PayloadSanitizer::config('kafka-logger.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.slow_outgoing_requests.enabled', true)
            ) {
                return null;
            }

            $thresholdMs = max(1, (int) PayloadSanitizer::config('kafka-logger.pulse.slow_outgoing_requests.threshold_ms', 1000));
            if (! $failed && $durationMs < $thresholdMs) {
                return null;
            }

            $parsed = parse_url($url);
            $host = (string) ($parsed['host'] ?? 'unknown-host');
            $path = (string) ($parsed['path'] ?? '/');
            $methodUpper = strtoupper($method);
            $label = "{$methodUpper} {$host}{$path}";

            $payload = LoggerService::createEnterpriseLog(
                eventType: 'PULSE_SLOW_OUTGOING_REQUEST',
                message: $failed
                    ? "Outgoing HTTP {$label} failed to connect after {$durationMs}ms."
                    : "Slow outgoing HTTP {$label} took {$durationMs}ms (HTTP {$statusCode}).",
                level: $failed || $statusCode >= 500 || $durationMs >= ($thresholdMs * 3) ? LogLevel::WARNING : LogLevel::INFO,
                component: ComponentSubsystem::EXTERNAL_API,
                metadata: [
                    'signature_hash' => sha1($label),
                    'method' => $methodUpper,
                    'host' => $host,
                    'path' => $path,
                    'status_code' => $statusCode,
                    'duration_ms' => $durationMs,
                    'threshold_ms' => $thresholdMs,
                    'connection_failed' => $failed,
                    'source_host' => gethostname() ?: 'unknown-host',
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

    protected function popDurationMs(object $request): int
    {
        $id = spl_object_id($request);
        if (! isset(self::$startedAtHr[$id])) {
            return 0;
        }

        $started = self::$startedAtHr[$id];
        unset(self::$startedAtHr[$id]);

        return max(0, (int) round((hrtime(true) - $started) / 1e6));
    }
}
