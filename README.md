# mimon/kafka-logger

[![Latest Version on Packagist](https://img.shields.io/packagist/v/mimon/kafka-logger.svg?style=flat-square)](https://packagist.org/packages/mimon/kafka-logger)
[![Total Downloads](https://img.shields.io/packagist/dt/mimon/kafka-logger.svg?style=flat-square)](https://packagist.org/packages/mimon/kafka-logger)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE.md)

Standardized Enterprise Kafka Log Payload Builder, Schema Validator & Facade for Laravel Microservices and Distributed Logging Systems.

Designed specifically for **Centralized Logging Systems (CLS)** in decoupled microservices, event-driven architectures, and distributed systems.

---

## Features

- **Fluent Builder Pattern**: Clean, expressive syntax (`KafkaLogger::event(...)->info()->send()`).
- **Standardized Enterprise Schema**: Auto-injects UUID `log_id`, distributed `trace_id`, client IP, host IP, and high-precision timestamps.
- **Latency & APM Ready**: Emits `timestamps.created_at` and `timestamps.produced_at` for sub-millisecond network latency tracking.
- **Auto-Environment Detection**: Automatically infers `LOCAL`, `STAGING`, or `PRODUCTION` from Laravel's environment.
- **Automatic Exception Parsing**: Formats exception codes and stack traces automatically for `ERROR` and `CRITICAL` levels.
- **Resilient Fallback**: Automatically logs to the local database even if the Kafka broker is temporarily unreachable.

---

## Installation

Install the package via Composer:

```bash
composer require mimon/kafka-logger
```

Publish the configuration file:

```bash
php artisan vendor:publish --tag=kafka-logger-config
```

---

## Configuration

In your application's `.env` file, specify your Kafka broker and centralized logging topic:

```env
# Kafka Broker connection
KAFKA_BROKERS=localhost:9092

# Logging configurations
KAFKA_LOGGER_DEFAULT_TOPIC=CLS.LOCAL.logs
KAFKA_LOGGER_APP_NAME=HR_APP
KAFKA_LOGGER_ENVIRONMENT=LOCAL
```

---

## Usage

### 1. Standard Audit / Event Log

```php
use App\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Facades\KafkaLogger;

KafkaLogger::event('EMPLOYEE_CREATED')
    ->message('Successfully registered new employee profile.')
    ->info()
    ->component(ComponentSubsystem::APPLICATION_LOGIC)
    ->actor($request->user()->id, $request->user()->role) // e.g., 'usr_101', 'HR_ADMIN'
    ->withMetadata([
        'employee_id' => $employee->id,
        'department'  => 'Institute of Computer Science',
        'position'    => 'Associate Professor',
    ])
    ->send();
```

---

### 2. Error & Exception Logging

Pass an exception directly to `withException()` or `error()`. The builder automatically extracts the error code and stack trace:

```php
use App\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Facades\KafkaLogger;

try {
    // Database or external operation...
} catch (\Throwable $e) {
    KafkaLogger::event('DATABASE_TRANSACTION_FAILED')
        ->message('Failed to persist employee record to PostgreSQL.')
        ->error(exception: $e)
        ->component(ComponentSubsystem::DATABASE)
        ->actor(auth()->id(), 'SYSTEM')
        ->withMetadata(['record_id' => $id])
        ->send();

    throw $e;
}
```

---

### 3. File Storage & Queue Workers

Track background operations and file transactions with dedicated subsystem taxonomies:

```php
// Background queue job
KafkaLogger::event('SALARY_PDF_GENERATED')
    ->message('Payroll report rendered successfully.')
    ->info()
    ->component(ComponentSubsystem::QUEUE_JOB)
    ->withMetadata(['job_id' => $this->job->getJobId()])
    ->send();

// S3 / Drive upload
KafkaLogger::event('DOCUMENT_UPLOADED')
    ->message('Applicant resume stored in S3 bucket.')
    ->info()
    ->component(ComponentSubsystem::STORAGE)
    ->send();
```

---

### 4. Quick One-Liners

```php
// Quick info
KafkaLogger::info('User session initiated.')
    ->event('USER_LOGIN')
    ->actor(auth()->id(), 'STUDENT')
    ->send();

// Quick warning
KafkaLogger::warning('Cache miss encountered for appointments index.')
    ->component(ComponentSubsystem::CACHE)
    ->send();
```

---

## Subsystem Component Taxonomy

| Component Value | Recommended Use Case |
| :--- | :--- |
| `APPLICATION_LOGIC` | Business logic, controller actions, service classes |
| `DATABASE` | Eloquent models, SQL queries, database transactions |
| `KAFKA_PRODUCER` | Outbound domain event publishing |
| `KAFKA_CONSUMER` | Inbound domain event processing |
| `QUEUE_JOB` | Asynchronous Laravel queue workers |
| `STORAGE` | File uploads, downloads, S3, Google Drive, or MinIO operations |
| `EXTERNAL_API` | HTTP clients (Guzzle), third-party API webhooks |
| `AUTHENTICATION` | Logins, token verification, permission denials |
| `CACHE` | Redis, Memcached read/write operations |

---

## Log Payload Structure

Every message emitted to Kafka adheres strictly to the following enterprise schema:

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
    "message": "Successfully registered new employee profile."
  },
  "trace": {
    "trace_id": "tr-20261012-98124",
    "span_id": "sp-0012"
  },
  "actor": {
    "user_id": "usr_admin_10293",
    "user_role": "HR_ADMIN",
    "client_ip": "192.168.1.55"
  },
  "timestamps": {
    "created_at": "2026-10-12T14:16:20.100Z",
    "produced_at": "2026-10-12T14:16:20.105Z"
  },
  "error": {
    "code": null,
    "stack_trace": null
  },
  "metadata": {
    "employee_id": 101,
    "position": "Associate Professor"
  }
}
```

---

## Testing

Run unit tests via PHPUnit:

```bash
composer test
```

---

## License

This package is open-sourced software licensed under the [MIT license](LICENSE.md).