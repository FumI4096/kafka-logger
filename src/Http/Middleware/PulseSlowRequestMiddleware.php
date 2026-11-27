<?php

namespace Mimon\KafkaLogger\Http\Middleware;

use Closure;
use Illuminate\Http\Request;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Services\LoggerService;
use Mimon\KafkaLogger\Support\ErrorBurstDeduplicator;
use Mimon\KafkaLogger\Support\PayloadSanitizer;
use Mimon\KafkaLogger\Support\PulseUsageBuffer;
use Mimon\KafkaLogger\Support\ServerVitalsCollector;
use Symfony\Component\HttpFoundation\Response;
use Throwable;

class PulseSlowRequestMiddleware
{
    public function handle(Request $request, Closure $next): Response
    {
        $startedAtHr = hrtime(true);

        try {
            return $response = $next($request);
        } finally {
            $durationMs = (int) round((hrtime(true) - $startedAtHr) / 1e6);
            $statusCode = isset($response) && $response instanceof Response ? $response->getStatusCode() : 500;

            $slowPayload = $this->recordIfSlow($request, $durationMs, $statusCode);

            try {
                $user = $request->user();
                $userId = $user && isset($user->id) ? (string) $user->id : 'GUEST';
                $userLabel = $user && isset($user->email) ? (string) $user->email : $userId;
                PulseUsageBuffer::recordRequest($userId, $userLabel, $slowPayload !== null);
            } catch (Throwable) {
            }

            // Piggyback server vitals + usage/cache rollup + expired error burst count rollups
            ServerVitalsCollector::maybeEmitHeartbeat();
            PulseUsageBuffer::maybeFlush();
            ErrorBurstDeduplicator::flushExpiredCrossProcessCounts();
        }
    }

    public function recordIfSlow(Request $request, int $durationMs, int $statusCode): ?array
    {
        try {
            $path = ltrim($request->path(), '/');
            $method = strtoupper($request->getMethod());
            $route = $request->route();
            $routeUri = $route && method_exists($route, 'uri') ? '/' . ltrim((string) $route->uri(), '/') : '/' . $path;
            $routeAction = $route && method_exists($route, 'getActionName') ? (string) $route->getActionName() : null;

            $user = null;
            try {
                $user = $request->user();
            } catch (Throwable) {
                $user = null;
            }

            $userId = $user && isset($user->id) ? (string) $user->id : 'GUEST';
            $userLabel = $user && isset($user->email) ? (string) $user->email : $userId;

            return $this->recordRequest($method, $path, $routeUri, $durationMs, $statusCode, $routeAction, $userId, $userLabel);
        } catch (Throwable) {
            return null;
        }
    }

    public function recordRequest(
        string $method,
        string $path,
        string $routeUri,
        int $durationMs,
        int $statusCode,
        ?string $routeAction = null,
        string $userId = 'GUEST',
        ?string $userLabel = null,
    ): ?array {
        try {
            if (
                ! PayloadSanitizer::config('kafka-logger.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.enabled', true)
                || ! PayloadSanitizer::config('kafka-logger.pulse.slow_requests.enabled', true)
            ) {
                return null;
            }

            $thresholdMs = max(1, (int) PayloadSanitizer::config('kafka-logger.pulse.slow_requests.threshold_ms', 1000));
            if ($durationMs < $thresholdMs) {
                return null;
            }

            $cleanPath = ltrim($path, '/');
            if ($this->shouldIgnorePath($cleanPath)) {
                return null;
            }

            $methodUpper = strtoupper($method);
            $normalizedRoute = '/' . ltrim($routeUri, '/');
            $label = "{$methodUpper} {$normalizedRoute}";
            $memoryMb = round(memory_get_peak_usage(true) / 1048576, 2);

            $payload = LoggerService::createEnterpriseLog(
                eventType: 'PULSE_SLOW_REQUEST',
                message: "Slow HTTP request {$label} took {$durationMs}ms (HTTP {$statusCode}).",
                level: $durationMs >= ($thresholdMs * 3) ? LogLevel::WARNING : LogLevel::INFO,
                component: ComponentSubsystem::APPLICATION_LOGIC,
                userId: $userId,
                metadata: [
                    'signature_hash' => sha1($label),
                    'method' => $methodUpper,
                    'route' => $normalizedRoute,
                    'uri' => '/' . $cleanPath,
                    'action' => $routeAction,
                    'status_code' => $statusCode,
                    'duration_ms' => $durationMs,
                    'memory_mb' => $memoryMb,
                    'threshold_ms' => $thresholdMs,
                    'actor_label' => $userLabel ?? $userId,
                    'host' => gethostname() ?: 'unknown-host',
                ],
            );

            try {
                LoggerService::dispatchEnterpriseLog($payload);
            } catch (Throwable) {
                // Never disrupt client app requests if dispatch fails
            }

            return $payload;
        } catch (Throwable) {
            return null;
        }
    }

    protected function shouldIgnorePath(string $path): bool
    {
        $ignorePatterns = (array) PayloadSanitizer::config(
            'kafka-logger.pulse.slow_requests.ignore_paths',
            ['_debugbar/*', 'horizon/*', 'telescope/*', 'pulse*', 'livewire/update']
        );
        foreach ($ignorePatterns as $pattern) {
            if (! is_string($pattern) || $pattern === '') {
                continue;
            }

            $normalized = ltrim($pattern, '/');
            if ($path === $normalized || fnmatch($normalized, $path)) {
                return true;
            }
        }

        return false;
    }
}
