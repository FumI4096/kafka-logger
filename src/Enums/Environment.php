<?php

namespace Mimon\KafkaLogger\Enums;

enum Environment: string
{
    case PRODUCTION = 'PRODUCTION';
    case STAGING = 'STAGING';
    case DEVELOPMENT = 'DEVELOPMENT';
    case LOCAL = 'LOCAL';
}