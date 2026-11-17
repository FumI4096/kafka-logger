<?php

namespace Mimon\KafkaLogger\Console\Commands;

use Illuminate\Console\Command;

class InstallKafkaLoggerCommand extends Command
{
    protected $signature = 'kafka-logger:install
        {--app-code= : Registered UPLB-CLS application code (e.g. CFNR_L)}
        {--app-name= : Human-readable application name}
        {--env= : Optional override environment (defaults to uppercase APP_ENV)}
        {--topic=CLS.LOCAL.logs : Default Kafka topic}
        {--force : Overwrite existing KAFKA_LOGGER_* keys in .env}';

    protected $description = 'Publish kafka-logger config and append KAFKA_LOGGER_* environment variables to .env and .env.example';

    public function handle(): int
    {
        $this->callSilently('vendor:publish', [
            '--tag' => 'kafka-logger-config',
        ]);

        $appCode = strtoupper(trim((string) ($this->option('app-code') ?: $this->ask('Application Code (must match UPLB-CLS)', 'MY_APP'))));
        $appName = trim((string) ($this->option('app-name') ?: $this->ask('Application Name', $appCode)));
        $environmentOption = $this->option('env');
        $topic = trim((string) ($this->option('topic') ?: 'CLS.LOCAL.logs'));
        $force = (bool) $this->option('force');

        $vars = [
            'KAFKA_LOGGER_APP_CODE' => $appCode,
            'KAFKA_LOGGER_APP_NAME' => '"' . str_replace('"', '\"', $appName) . '"',
            'KAFKA_LOGGER_DEFAULT_TOPIC' => $topic,
            'KAFKA_LOGGER_PRODUCE' => 'true',
            'KAFKA_LOGGER_AUTO_LOG_AUTH' => 'true',
            'KAFKA_LOGGER_AUTO_LOG_ERRORS' => 'true',
        ];

        if ($environmentOption !== null && trim((string) $environmentOption) !== '') {
            $vars['KAFKA_LOGGER_ENVIRONMENT'] = strtoupper(trim((string) $environmentOption));
        }

        $this->writeEnvFile(base_path('.env'), $vars, $force);
        $this->writeEnvFile(base_path('.env.example'), $vars, $force);

        $this->info('Kafka Logger configuration and .env variables installed.');

        return self::SUCCESS;
    }

    /**
     * @param  array<string, string>  $vars
     */
    protected function writeEnvFile(string $path, array $vars, bool $force): void
    {
        if (! file_exists($path)) {
            return;
        }

        $contents = (string) file_get_contents($path);
        $appended = [];

        foreach ($vars as $key => $value) {
            $pattern = '/^' . preg_quote($key, '/') . '=.*$/m';

            if (preg_match($pattern, $contents)) {
                if ($force) {
                    $contents = (string) preg_replace($pattern, "{$key}={$value}", $contents);
                }
            } else {
                $appended[] = "{$key}={$value}";
            }
        }

        if (! empty($appended)) {
            $contents = rtrim($contents) . "\n\n# UPLB-CLS Kafka Logger\n" . implode("\n", $appended) . "\n";
        }

        file_put_contents($path, $contents);
    }
}
