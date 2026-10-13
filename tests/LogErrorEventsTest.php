<?php

namespace Mimon\KafkaLogger\Tests;

use Illuminate\Auth\AuthenticationException;
use Illuminate\Http\Client\HttpClientException;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Listeners\LogErrorEvents;
use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use RuntimeException;

class LogErrorEventsTest extends TestCase
{
    public function test_classifies_database_exceptions_correctly(): void
    {
        $listener = new LogErrorEvents();
        $method = new ReflectionMethod($listener, 'resolveComponentAndEventType');
        $method->setAccessible(true);

        [$component, $eventType] = $method->invoke($listener, new PDOException('SQLSTATE[23505]: Unique violation'));

        $this->assertSame(ComponentSubsystem::DATABASE, $component);
        $this->assertSame('DATABASE_QUERY_ERROR', $eventType);
    }

    public function test_classifies_external_api_exceptions_correctly(): void
    {
        $listener = new LogErrorEvents();
        $method = new ReflectionMethod($listener, 'resolveComponentAndEventType');
        $method->setAccessible(true);

        [$component, $eventType] = $method->invoke($listener, new HttpClientException('Connection timed out'));

        $this->assertSame(ComponentSubsystem::EXTERNAL_API, $component);
        $this->assertSame('EXTERNAL_API_ERROR', $eventType);
    }

    public function test_classifies_authentication_exceptions_correctly(): void
    {
        $listener = new LogErrorEvents();
        $method = new ReflectionMethod($listener, 'resolveComponentAndEventType');
        $method->setAccessible(true);

        [$component, $eventType] = $method->invoke($listener, new AuthenticationException('Unauthenticated.'));

        $this->assertSame(ComponentSubsystem::AUTHENTICATION, $component);
        $this->assertSame('AUTH_ACCESS_DENIED', $eventType);
    }

    public function test_classifies_general_exceptions_as_application_logic(): void
    {
        $listener = new LogErrorEvents();
        $method = new ReflectionMethod($listener, 'resolveComponentAndEventType');
        $method->setAccessible(true);

        [$component, $eventType] = $method->invoke($listener, new RuntimeException('Unexpected error in Filament action'));

        $this->assertSame(ComponentSubsystem::APPLICATION_LOGIC, $component);
        $this->assertSame('APPLICATION_ERROR', $eventType);
    }
}