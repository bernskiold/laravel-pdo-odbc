<?php

namespace Bernskiold\LaravelSnowflake;

use Bernskiold\LaravelSnowflake\Odbc\OdbcConnector;
use Illuminate\Database\Connection;
use Illuminate\Support\ServiceProvider;

class SnowflakeServiceProvider extends ServiceProvider
{
    /**
     * Register the Snowflake database drivers.
     *
     * @return void
     */
    public function register()
    {
        Connection::resolverFor('snowflake', SnowflakeConnector::registerDriver());
        Connection::resolverFor('snowflake_native', SnowflakeConnector::registerDriver());
        Connection::resolverFor('odbc', OdbcConnector::registerDriver());
    }
}
