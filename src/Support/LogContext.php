<?php

namespace Mimon\KafkaLogger\Support;

use Illuminate\Support\Str;

class LogContext
{
    /**
     * Scoped workflow/operation context for the active request, RPC call, or queue job.
     *
     * @var array<string, mixed>
     */
    protected static array $context = [];

    protected static ?string $traceId = null;

    /**
     * Bind or merge context fields (e.g., module, workflow, operation, service, trace_id).
     *
     * @param  array<string, mixed>  $context
     */
    public static function set(array $context): void
    {
        if (! empty($context['trace_id']) && is_string($context['trace_id'])) {
            static::$traceId = $context['trace_id'];
        } elseif (static::$traceId === null) {
            static::$traceId = (string) Str::uuid();
        }

        unset($context['trace_id']);

        foreach ($context as $key => $value) {
            if ($value !== null) {
                static::$context[$key] = $value;
            }
        }
    }

    /**
     * @return array<string, mixed>
     */
    public static function get(): array
    {
        return static::$context;
    }

    public static function traceId(): ?string
    {
        return static::$traceId;
    }

    public static function ensureTraceId(): string
    {
        if (static::$traceId === null) {
            static::$traceId = (string) Str::uuid();
        }

        return static::$traceId;
    }

    public static function clear(): void
    {
        static::$context = [];
        static::$traceId = null;
    }

    /**
     * Execute a callback within a scoped context, restoring the prior context afterward.
     *
     * @template TReturn
     *
     * @param  array<string, mixed>  $context
     * @param  callable(): TReturn  $callback
     * @return TReturn
     */
    public static function runWith(array $context, callable $callback): mixed
    {
        $previousContext = static::$context;
        $previousTraceId = static::$traceId;

        try {
            static::set($context);

            return $callback();
        } finally {
            static::$context = $previousContext;
            static::$traceId = $previousTraceId;
        }
    }
}
