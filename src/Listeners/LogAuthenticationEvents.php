<?php

namespace Mimon\KafkaLogger\Listeners;

use Illuminate\Auth\Events\Failed;
use Illuminate\Auth\Events\Login;
use Illuminate\Auth\Events\Logout;
use Mimon\KafkaLogger\Enums\ComponentSubsystem;
use Mimon\KafkaLogger\Facades\KafkaLogger;
use Mimon\KafkaLogger\Support\PayloadSanitizer;

class LogAuthenticationEvents
{
    /**
     * Handle successful user login events.
     */
    public function handleLogin(Login $event): void
    {
        try {
            $user = $event->user;
            $userId = (string) $user->getAuthIdentifier();
            $email = $user->email ?? $userId;
            $role = $this->resolvePrimaryRole($user);
            $appName = config('kafka-logger.app_name', 'APPLICATION');

            $metadata = [
                'user_id'    => $user->getAuthIdentifier(),
                'email'      => $user->email ?? null,
                'name'       => $user->name ?? null,
                'guard'      => $event->guard,
                'remember'   => $event->remember,
                'ip_address' => request()?->ip(),
                'user_agent' => request()?->userAgent(),
            ];

            if (method_exists($user, 'getRoleNames')) {
                $metadata['roles'] = $user->getRoleNames()->values()->all();
            }

            if (method_exists($user, 'office') && isset($user->office)) {
                $metadata['office'] = $user->office?->code ?? $user->office?->name ?? null;
            }

            if (method_exists($user, 'toKafkaLogMetadata')) {
                $metadata = array_merge($metadata, (array) $user->toKafkaLogMetadata());
            }

            KafkaLogger::event('USER_LOGIN')
                ->message("User {$email} logged into {$appName}.")
                ->info()
                ->component(ComponentSubsystem::AUTHENTICATION)
                ->actor($userId, $role)
                ->withMetadata($metadata)
                ->send();
        } catch (\Throwable $e) {
            logger()->warning('mimon/kafka-logger: Failed to log Login event: ' . $e->getMessage());
        }
    }

    /**
     * Handle failed authentication attempts.
     */
    public function handleFailed(Failed $event): void
    {
        try {
            // ensure that the identifier is a string and not null
            $identifier = (string) ($event->credentials['email']
                ?? $event->credentials['username']
                ?? $event->credentials['login']
                ?? '');

            $userId = $event->user ? (string) $event->user->getAuthIdentifier() : 'usr_anonymous';

            KafkaLogger::event('USER_LOGIN_FAILED')
                ->message('Failed login attempt.')
                ->warning()
                ->component(ComponentSubsystem::AUTHENTICATION)
                ->actor($userId, 'GUEST')
                ->withMetadata([
                    'attempted_identifier_hash' => $identifier !== '' ? PayloadSanitizer::hashIdentifier($identifier) : null,
                    'identifier_hint'           => $identifier !== '' ? PayloadSanitizer::identifierHint($identifier) : null,
                    'guard'                     => $event->guard,
                    'ip_address'                => request()?->ip(),
                    'user_agent'                => mb_substr((string) request()?->userAgent(), 0, 255),
                ])
                ->send();
        } catch (\Throwable $e) {
            logger()->warning('mimon/kafka-logger: Failed to log Failed login event: ' . $e->getMessage());
        }
    }

    /**
     * Handle user logout events.
     */
    public function handleLogout(Logout $event): void
    {
        if (! $event->user) {
            return;
        }

        try {
            $user = $event->user;
            $userId = (string) $user->getAuthIdentifier();
            $email = $user->email ?? $userId;
            $role = $this->resolvePrimaryRole($user);
            $appName = config('kafka-logger.app_name', 'APPLICATION');

            KafkaLogger::event('USER_LOGOUT')
                ->message("User {$email} logged out of {$appName}.")
                ->info()
                ->component(ComponentSubsystem::AUTHENTICATION)
                ->actor($userId, $role)
                ->withMetadata([
                    'user_id'    => $user->getAuthIdentifier(),
                    'email'      => $user->email ?? null,
                    'guard'      => $event->guard,
                    'ip_address' => request()?->ip(),
                ])
                ->send();
        } catch (\Throwable $e) {
            logger()->warning('mimon/kafka-logger: Failed to log Logout event: ' . $e->getMessage());
        }
    }

    /**
     * Resolve the primary role string in UPPER_SNAKE_CASE for the CLS actor contract.
     */
    protected function resolvePrimaryRole(object $user): string
    {
        if (method_exists($user, 'getRoleNames')) {
            $firstRole = $user->getRoleNames()->first();
            if ($firstRole) {
                return strtoupper(str_replace(['-', ' '], '_', $firstRole));
            }
        }

        if (! empty($user->role) && is_string($user->role)) {
            return strtoupper(str_replace(['-', ' '], '_', $user->role));
        }

        return 'AUTHENTICATED_USER';
    }
}
