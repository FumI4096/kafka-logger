<?php

namespace Mimon\KafkaLogger\Enums;

enum ComponentSubsystem: string
{
    case APPLICATION_LOGIC = 'APPLICATION_LOGIC';
    case DATABASE = 'DATABASE';
    case KAFKA_PRODUCER = 'KAFKA_PRODUCER';
    case KAFKA_CONSUMER = 'KAFKA_CONSUMER';
    case QUEUE_JOB = 'QUEUE_JOB';
    case STORAGE = 'STORAGE';
    case EXTERNAL_API = 'EXTERNAL_API';
    case AUTHENTICATION = 'AUTHENTICATION';
    case CACHE = 'CACHE';
}