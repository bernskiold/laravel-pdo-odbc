<?php

namespace Bernskiold\LaravelSnowflake\Tests;

use Bernskiold\LaravelSnowflake\SnowflakeConnection;
use Bernskiold\LaravelSnowflake\SnowflakeServiceProvider;
use Orchestra\Testbench\TestCase as BaseTestCase;
use PDO;

abstract class TestCase extends BaseTestCase
{
    protected function getPackageProviders($app)
    {
        return [
            SnowflakeServiceProvider::class,
        ];
    }

    /**
     * Create a Snowflake connection backed by an in-memory SQLite PDO,
     * which is enough to exercise grammars, processors and bindings
     * without a real Snowflake server.
     */
    protected function makeConnection(array $config = []): SnowflakeConnection
    {
        $pdo = new PDO('sqlite::memory:');

        return new SnowflakeConnection($pdo, 'TESTDB', '', array_merge([
            'name' => 'snowflake',
            'driver' => 'snowflake',
        ], $config));
    }

    /**
     * Toggle the SNOWFLAKE_COLUMNS_CASE_SENSITIVE environment variable.
     */
    protected function setColumnsCaseSensitive(?bool $value): void
    {
        if ($value === null) {
            putenv('SNOWFLAKE_COLUMNS_CASE_SENSITIVE');
            unset($_ENV['SNOWFLAKE_COLUMNS_CASE_SENSITIVE'], $_SERVER['SNOWFLAKE_COLUMNS_CASE_SENSITIVE']);

            return;
        }

        $stringValue = $value ? 'true' : 'false';

        putenv('SNOWFLAKE_COLUMNS_CASE_SENSITIVE='.$stringValue);
        $_ENV['SNOWFLAKE_COLUMNS_CASE_SENSITIVE'] = $stringValue;
        $_SERVER['SNOWFLAKE_COLUMNS_CASE_SENSITIVE'] = $stringValue;
    }

    protected function tearDown(): void
    {
        $this->setColumnsCaseSensitive(null);

        parent::tearDown();
    }
}
