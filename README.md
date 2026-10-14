# mimon/kafka-logger

[![Latest Version on Packagist](https://img.shields.io/packagist/v/mimon/kafka-logger.svg?style=flat-square)](https://packagist.org/packages/mimon/kafka-logger)
[![Total Downloads](https://img.shields.io/packagist/dt/mimon/kafka-logger.svg?style=flat-square)](https://packagist.org/packages/mimon/kafka-logger)
[![PHP Version](https://img.shields.io/packagist/dependency-v/mimon/kafka-logger/php?style=flat-square)](https://packagist.org/packages/mimon/kafka-logger)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE.md)

Standardized Enterprise Kafka Log Payload Builder, Schema Validator & Automatic Event Listener for Laravel + Filament Microservices.

Designed for **Centralized Logging Systems (CLS)** across distributed applications — providing **zero-config automatic logging** for authentication and errors while giving developers a **fluent Builder API** for custom domain and audit logs.

---

## Key Features

- **Built-in Automatic Authentication Logging**: Automatically listens to Laravel's `Login`, `Failed`, and `Logout` events — no manual login/logout logging code required.
- **Built-in Automatic Error & Exception Tracing**: Automatically intercepts unhandled exceptions, failed queue jobs (`JobFailed`), and `Log::error()` / `Log::critical()` calls. Auto-detects database (`pgsql`/`mysql`), external API, cache, and Filament/Livewire context.
- **Developer-Friendly Fluent Builder**: Simple chainable API (`KafkaLogger::event(...)->message(...)->info()->send()`) for custom business events and audit trails.
- **Smart Config Defaults**: `appName`, `environment`, `topic`, `component`, and `actor` automatically fall back to your `.env` / config settings so you only write what you need.
- **Contract Schema Validation**: Validates every payload against the Enterprise CLS JSON contract before publishing to Kafka.

---

## 1. Installation & Setup

### Step 1: Install via Composer

```bash
composer require mimon/kafka-logger
```

*(Laravel Package Auto-Discovery automatically registers `KafkaLoggerServiceProvider` and the `KafkaLogger` facade.)*

### Step 2: Publish the Configuration File

```bash
php artisan vendor:publish --tag=kafka-logger-config
```

## 2. What Is Logged Automatically (Zero Code Required)

Once installed, `mimon/kafka-logger` automatically runs two background listeners out of the box:

### A. Automatic Authentication Logs (`LogAuthenticationEvents`)
Enabled via `KAFKA_LOGGER_AUTO_LOG_AUTH=true` (default). Automatically dispatches `ComponentSubsystem::AUTHENTICATION` logs whenever a user logs in, fails login, or logs out:
- **`USER_LOGIN`** (`INFO`): Captures user ID, email, name, guard, IP address, user agent, Spatie/Shield roles (`getRoleNames()`), and office metadata.
- **`USER_LOGIN_FAILED`** (`WARNING`): Captures attempted email/username, guard, IP address, and user agent.
- **`USER_LOGOUT`** (`INFO`): Captures user ID, email, guard, and IP address.

> **Tip — Custom User Metadata**: Implement `public function toKafkaLogMetadata(): array` on your `User` model to automatically merge extra user fields into every login log.

### B. Automatic Error & Exception Tracing (`LogErrorEvents`)
Enabled via `KAFKA_LOGGER_AUTO_LOG_ERRORS=true` (default). Automatically listens to `Illuminate\Log\Events\MessageLogged` (`error`, `critical`, `alert`, `emergency`) and `Illuminate\Queue\Events\JobFailed`:
- **Unhandled Exceptions**: Any uncaught exception in controllers, Filament resources, Livewire actions, or console commands is automatically formatted with its error code, file, line, and stack trace.
- **Database Errors (PostgreSQL & MySQL)**: Automatically classifies `QueryException` and `PDOException` under `ComponentSubsystem::DATABASE` (`DATABASE_QUERY_ERROR`) and extracts `db_connection` (`pgsql`/`mysql`) and `sql_state`.
- **Filament & Livewire Awareness**: When an error occurs during a Filament action or form submission (`POST /livewire/update`), it automatically captures `filament_page_url` and `filament_component`.
- **Standard `Log::error()` Calls**: Even if you catch an exception inside a Filament action to show a toast notification and call `Log::error('...', ['exception' => $e])`, the listener automatically forwards it to Kafka.

*To disable either automatic listener in a specific app, set `KAFKA_LOGGER_AUTO_LOG_AUTH=false` or `KAFKA_LOGGER_AUTO_LOG_ERRORS=false` in `.env`.*

---

## 3. How to Manually Create Custom Logs (Fluent Builder Guide)

While authentication and general errors are handled automatically, you can manually create **custom audit logs, domain events, or custom error logs** anywhere in your application using the `KafkaLogger` facade.

### Minimal Example (Using Smart Defaults)

Because `appName`, `environment`, `topic`, `component`, and `actor` have defaults configured, **only `event()` and `message()` are required**:

```php
use Mimon\KafkaLogger\Facades\KafkaLogger;

KafkaLogger::event('EMPLOYEE_CREATED')
    ->message('New employee profile registered into HR database.')
    ->info()
    ->send(); // Alias: ->dispatch() or ->log()
```

---

### Full Builder Example (Customizing All Fields)

```php
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Facades\KafkaLogger;

KafkaLogger::event('APPOINTMENT_APPROVED')
    ->message('Employee appointment papers approved by HR Admin.')
    ->info()
    ->component(ComponentSubsystem::APPLICATION_LOGIC)
    ->actor((string) auth()->id(), 'HR_ADMIN')
    ->withMetadata([
        'employee_id'    => $employee->id,
        'appointment_no' => $appointment->reference_no,
        'department'     => 'Institute of Computer Science',
    ])
    ->send();
```

---

### Manual Custom Error & Exception Logging

When you want to catch an exception yourself and log a **specific domain error event** (instead of relying on the generic automatic error listener), use `->error()` or `->critical()` with `->withException($e)`:

```php
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Facades\KafkaLogger;

try {
    $this->payrollService->generateSalaryDocument($employee);
} catch (\Throwable $e) {
    KafkaLogger::event('SALARY_DOCUMENT_GENERATION_FAILED')
        ->message('Failed to generate PDF salary document for employee.')
        ->error()
        ->withException($e) // Automatically extracts error code & stack trace
        ->component(ComponentSubsystem::APPLICATION_LOGIC)
        ->actor((string) auth()->id(), 'HR_ADMIN')
        ->withMetadata([
            'employee_id' => $employee->id,
            'period'      => '2026-10',
        ])
        ->send();
}
```

You can also log business/validation errors **without** a PHP exception:

```php
KafkaLogger::event('VOUCHER_BALANCE_MISMATCH')
    ->message('Disbursement voucher total does not match itemized breakdown.')
    ->error()
    ->component(ComponentSubsystem::APPLICATION_LOGIC)
    ->actor((string) auth()->id(), 'CASHIER')
    ->withMetadata([
        'voucher_id' => $voucher->id,
        'expected'   => 15000.00,
        'actual'     => 14500.00,
    ])
    ->send();
```

---

### Available Builder Methods Reference

| Method | Required? | Description | Default if Omitted |
| :--- | :---: | :--- | :--- |
| `->event(string $eventType)` | **Yes** | Event name in `UPPER_SNAKE_CASE` (e.g. `'EMPLOYEE_CREATED'`). | `'SYSTEM_EVENT'` |
| `->message(string $message)` | **Yes** | Human-readable description of what happened. | — |
| `->debug(?string $msg = null)` | Optional | Sets severity level to `LogLevel::DEBUG`. | — |
| `->info(?string $msg = null)` | Optional | Sets severity level to `LogLevel::INFO` (and optionally sets message). | `LogLevel::INFO` |
| `->warning(?string $msg = null)` | Optional | Sets severity level to `LogLevel::WARNING`. | — |
| `->error(?string $msg = null, ?Throwable $e = null)` | Optional | Sets severity level to `LogLevel::ERROR` (and optionally attaches exception). | — |
| `->critical(?string $msg = null, ?Throwable $e = null)` | Optional | Sets severity level to `LogLevel::CRITICAL` (and optionally attaches exception). | — |
| `->level(LogLevel\|string $level)` | Optional | Explicitly sets the `LogLevel` enum or string value. | `LogLevel::INFO` |
| `->component(ComponentSubsystem\|string $comp)` | Optional | Subsystem classification (see Section 5). | `ComponentSubsystem::APPLICATION_LOGIC` |
| `->actor(string $userId, ?string $role = null)` | Optional | Sets the user ID and role in `UPPER_SNAKE_CASE`. Alias: `->user()`. | `'usr_system'` / `'SYSTEM'` |
| `->withMetadata(array $data)` | Optional | Attaches custom key-value context. Alias: `->withContext()`. | `[]` |
| `->withException(Throwable $e)` | Optional | Extracts `error.code` and `error.stack_trace` from an exception. | `null` |
| `->appName(string $name)` | Optional | Overrides the application name for this log entry. | `config('kafka-logger.app_name')` |
| `->environment(Environment\|string $env)` | Optional | Overrides environment (`LOCAL`, `DEVELOPMENT`, `STAGING`, `PRODUCTION`). | `config('kafka-logger.environment')` |
| `->topic(string $topic)` | Optional | Overrides the target Kafka topic for this log entry. | `config('kafka-logger.default_topic')` |
| `->send()` / `->dispatch()` / `->log()` | **Yes** | Validates the payload and dispatches it to Kafka. | — |

---

## 4. Log Levels & Severity Methods (`Mimon\KafkaLogger\Enums\LogLevel`)

You can set the log severity either by calling the dedicated helper method (`->info()`, `->warning()`, `->error()`, `->critical()`, `->debug()`) or by passing the `LogLevel` enum to `->level()`:

```php
use Mimon\KafkaLogger\Enums\LogLevel;
use Mimon\KafkaLogger\Facades\KafkaLogger;

// Option A: Using fluent severity methods (Recommended)
KafkaLogger::event('CACHE_MISS')->message('Cache key expired')->debug()->send();
KafkaLogger::event('EMPLOYEE_CREATED')->message('Profile created')->info()->send();
KafkaLogger::event('RATE_LIMIT_NEAR')->message('80% quota used')->warning()->send();
KafkaLogger::event('QUERY_FAILED')->message('Insert failed')->error(exception: $e)->send();
KafkaLogger::event('BROKER_DOWN')->message('Service unreachable')->critical(exception: $e)->send();

// Option B: Starting directly from the facade severity method
KafkaLogger::info('Profile created')->event('EMPLOYEE_CREATED')->send();
KafkaLogger::warning('80% quota used')->event('RATE_LIMIT_NEAR')->send();
KafkaLogger::error('Insert failed', $e)->event('QUERY_FAILED')->send();

// Option C: Passing the LogLevel Enum explicitly
KafkaLogger::event('EMPLOYEE_CREATED')
    ->message('Profile created')
    ->level(LogLevel::INFO)
    ->send();
```

| Enum Case | String Value | Fluent Builder Method | Recommended Use Case |
| :--- | :--- | :--- | :--- |
| `LogLevel::DEBUG` | `'DEBUG'` | `->debug()` | Diagnostic details, payload inspections, cache hits/misses during development |
| `LogLevel::INFO` | `'INFO'` | `->info()` | Standard business events, audit logs, logins, successful CRUD actions *(Default)* |
| `LogLevel::WARNING` | `'WARNING'` | `->warning()` | Failed login attempts, deprecated usage, retries, non-breaking anomalies |
| `LogLevel::ERROR` | `'ERROR'` | `->error()` | Handled/unhandled runtime exceptions, failed queries, failed queue jobs |
| `LogLevel::CRITICAL` | `'CRITICAL'` | `->critical()` | System-wide outages, database/broker unreachability, data corruption risks |

---

## 5. Subsystem Component Taxonomy (`Mimon\KafkaLogger\Enums\ComponentSubsystem`)

| Enum Case | Value | Recommended Use Case |
| :--- | :--- | :--- |
| `ComponentSubsystem::APPLICATION_LOGIC` | `'APPLICATION_LOGIC'` | Business logic, Filament resources/actions, controllers, services |
| `ComponentSubsystem::DATABASE` | `'DATABASE'` | Eloquent models, PostgreSQL/MySQL queries, transactions |
| `ComponentSubsystem::KAFKA_PRODUCER` | `'KAFKA_PRODUCER'` | Outbound Kafka event publishing |
| `ComponentSubsystem::KAFKA_CONSUMER` | `'KAFKA_CONSUMER'` | Inbound Kafka event processing |
| `ComponentSubsystem::QUEUE_JOB` | `'QUEUE_JOB'` | Asynchronous Laravel queue workers and background jobs |
| `ComponentSubsystem::STORAGE` | `'STORAGE'` | File uploads, PDF generation, S3, or local disk operations |
| `ComponentSubsystem::EXTERNAL_API` | `'EXTERNAL_API'` | HTTP client (`Http::get/post`) calls, third-party webhooks |
| `ComponentSubsystem::AUTHENTICATION` | `'AUTHENTICATION'` | Login, logout, failed auth, permission/role checks |
| `ComponentSubsystem::CACHE` | `'CACHE'` | Redis or cache read/write operations |

---

## 6. Standardized JSON Payload Output

Every message emitted to Kafka strictly follows this enterprise schema:

```json
{
  "log_id": "550e8400-e29b-41d4-a716-446655440000",
  "environment": "LOCAL",
  "application": {
    "name": "HR_APP",
    "component": "APPLICATION_LOGIC",
    "host_ip": "127.0.0.1",
    "host_name": "hr-app-node-01"
  },
  "event": {
    "type": "EMPLOYEE_CREATED",
    "level": "INFO",
    "topic": "CLS.LOCAL.logs",
    "message": "New employee profile registered into HR database."
  },
  "trace": {
    "trace_id": "tr-98124uplb202",
    "span_id": "sp-0012ab"
  },
  "actor": {
    "user_id": "usr_admin_10293",
    "user_role": "HR_ADMIN",
    "client_ip": "192.168.1.55"
  },
  "timestamps": {
    "created_at": "2026-10-13T09:00:20+08:00",
    "produced_at": "2026-10-13T09:00:20+08:00"
  },
  "error": {
    "code": null,
    "stack_trace": null
  },
  "metadata": {
    "employee_id": 101,
    "department": "Institute of Computer Science"
  }
}
```

## 7. Environment Configuration Variables

| Key | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_LOGGER_APP_NAME` | `'HR_APP'` | Enterprise identifier for the microservice |
| `KAFKA_LOGGER_ENVIRONMENT` | `'LOCAL'` | `LOCAL`, `DEVELOPMENT`, `STAGING`, or `PRODUCTION` |
| `KAFKA_LOGGER_DEFAULT_TOPIC` | `'YOUR_TOPIC'` | Fallback Kafka topic |
| `KAFKA_LOGGER_PRODUCE` | `true` | Set to `false` in local testing to disable Kafka transmission |
| `KAFKA_LOGGER_AUTO_LOG_AUTH` | `true` | Automatically listen to Laravel authentication events |
| `KAFKA_LOGGER_AUTO_LOG_ERRORS` | `true` | Automatically listen to error logs and failed jobs |
| `KAFKA_LOGGER_CONTEXT_MODE` | `'redact'` | `'redact'` (sanitize keys), `'allowlist'`, or `'none'` |
| `KAFKA_LOGGER_INCLUDE_SQL` | `false` | Whether to include sanitized SQL queries in DB error metadata |
| `KAFKA_LOGGER_PRODUCER_TIMEOUT_MS`| `1500` | Max broker wait time in milliseconds |
| `KAFKA_LOGGER_CIRCUIT_BREAKER_SECONDS`| `30` | Seconds to bypass Kafka after a broker error |
| `KAFKA_LOGGER_ASYNC` | `false` | Asynchronously publish via queued `SendKafkaLogJob` |
| `KAFKA_LOGGER_MAX_PAYLOAD_BYTES` | `900000` | Max payload size threshold before progressive truncation |

---

## Testing

Run the package unit test suite via PHPUnit:

```bash
vendor/bin/phpunit
```

---

## License

This package is open-sourced software licensed under the [MIT license](LICENSE.md).