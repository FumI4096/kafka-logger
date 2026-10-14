<?php

namespace Mimon\KafkaLogger\Support;

use Throwable;

/**
 * Builds a stack trace from file / line / class / function ONLY.
 * Function arguments are never read, regardless of zend.exception_ignore_args.
 */
class SafeTrace
{
    public static function format(Throwable $e, int $maxFrames = 25): string
    {
        $basePath = null;
        try {
            if (function_exists('base_path') && function_exists('app') && method_exists(app(), 'basePath')) {
                $basePath = rtrim((string) base_path(), '/\\') . DIRECTORY_SEPARATOR;
            }
        } catch (\Throwable $err) {
            // Standalone test environment without Application instance
        }

        $strip = static fn (?string $file) => $file && $basePath && str_starts_with($file, $basePath)
            ? substr($file, strlen($basePath))
            : (string) $file;

        $lines = [];
        $frames = $e->getTrace();
        $total = count($frames);

        foreach (array_slice($frames, 0, max(1, $maxFrames)) as $i => $frame) {
            $location = isset($frame['file']) ? $strip($frame['file']) . '(' . ($frame['line'] ?? '?') . ')' : '[internal function]';
            $call = ($frame['class'] ?? '') . ($frame['type'] ?? '') . ($frame['function'] ?? '') . '()';
            $lines[] = "#{$i} {$location}: {$call}";
        }

        if ($total > $maxFrames) {
            $lines[] = '#... ' . ($total - $maxFrames) . ' more frames omitted';
        }

        array_unshift($lines, get_class($e) . ' @ ' . $strip($e->getFile()) . '(' . $e->getLine() . ')');

        return implode("\n", $lines);
    }
}
