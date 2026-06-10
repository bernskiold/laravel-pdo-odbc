<?php

namespace Bernskiold\LaravelSnowflake;

use Illuminate\Container\Container;

/**
 * Reads the package configuration when a Laravel container is available,
 * falling back to the given default so the grammars keep working when
 * illuminate/database is used standalone.
 */
final class PackageConfig
{
    public static function get(string $key, mixed $default = null): mixed
    {
        $container = Container::getInstance();

        if ($container->bound('config')) {
            return $container->make('config')->get('snowflake.'.$key, $default);
        }

        return $default;
    }
}
