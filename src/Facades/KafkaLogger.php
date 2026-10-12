<?php

namespace Mimon\KafkaLogger\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder make()
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder event(string $eventType)
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder|\App\Models\SystemLog info(?string $arg1 = null, ?string $arg2 = null, array $options = [])
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder|\App\Models\SystemLog warning(?string $arg1 = null, ?string $arg2 = null, array $options = [])
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder|\App\Models\SystemLog debug(?string $arg1 = null, ?string $arg2 = null, array $options = [])
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder|\App\Models\SystemLog error(?string $arg1 = null, ?string $arg2 = null, ?\Throwable $exception = null, array $options = [])
 * @method static \Mimon\KafkaLogger\Services\KafkaLogBuilder|\App\Models\SystemLog critical(?string $arg1 = null, ?string $arg2 = null, ?\Throwable $exception = null, array $options = [])
 * @method static \App\Models\SystemLog directLog(string $eventType, string $message, \Mimon\KafkaLogger\Enums\LogLevel|string $level = \Mimon\KafkaLogger\Enums\LogLevel::INFO, array $options = [])
 *
 * @see \Mimon\KafkaLogger\Services\KafkaLoggerService
 */
class KafkaLogger extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'kafka-logger';
    }
}