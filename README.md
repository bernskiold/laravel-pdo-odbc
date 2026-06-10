# Laravel Snowflake

A Snowflake database driver for Laravel, with full support for the query
builder, Eloquent and schema migrations. It connects to Snowflake either
through the native [`pdo_snowflake`](https://github.com/snowflakedb/pdo_snowflake)
PHP extension (recommended) or through ODBC, and also works as a generic
PDO/ODBC driver for other ODBC data sources.

## Requirements

- PHP 8.2+
- Laravel 12+
- The [`pdo_snowflake`](https://github.com/snowflakedb/pdo_snowflake) extension
  (for native connections) **or** the
  [Snowflake ODBC driver](https://docs.snowflake.com/en/developer-guide/odbc/odbc)
  together with PHP's `odbc` and `pdo_odbc` extensions

## Installation

```bash
composer require bernskiold/laravel-snowflake
```

The service provider is registered automatically through package discovery.
If you have disabled discovery, register it manually:

```php
'providers' => [
    // ...
    Bernskiold\LaravelSnowflake\SnowflakeServiceProvider::class,
],
```

## Configuration

Add a connection to `config/database.php`. The package registers three
drivers:

| Driver | Description |
| --- | --- |
| `snowflake_native` | Snowflake through the `pdo_snowflake` extension (recommended) |
| `snowflake` | Snowflake through the Snowflake ODBC driver |
| `odbc` | Generic ODBC connection for any other data source |

### Native Snowflake (recommended)

```php
'snowflake' => [
    'driver' => 'snowflake_native',
    'account' => '{account_name}.eu-west-1',
    'username' => env('SNOWFLAKE_USERNAME'),
    'password' => env('SNOWFLAKE_PASSWORD'),
    'database' => env('SNOWFLAKE_DATABASE'),
    'warehouse' => env('SNOWFLAKE_WAREHOUSE'),
    'schema' => 'PUBLIC',
],
```

### Snowflake via ODBC

```php
'snowflake' => [
    'driver' => 'snowflake',
    // Absolute path to the driver file, or the name registered in odbcinst.ini.
    'odbc_driver' => '/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib',
    'server' => '{account_name}.snowflakecomputing.com',
    'username' => env('SNOWFLAKE_USERNAME'),
    'password' => env('SNOWFLAKE_PASSWORD'),
    'database' => env('SNOWFLAKE_DATABASE'),
    'warehouse' => env('SNOWFLAKE_WAREHOUSE'),
    'schema' => 'PUBLIC',
    'options' => [
        // Required for Snowflake ODBC usage.
        \PDO::ODBC_ATTR_USE_CURSOR_LIBRARY => \PDO::ODBC_SQL_USE_DRIVER,
    ],
],
```

All configuration fields except `driver`, `odbc_driver`, `options`,
`username`, `password`, `name` and `prefix` are appended to the DSN
connection string, so any extra Snowflake connection parameter can simply be
added to the connection configuration.

### Key-pair authentication

Both drivers support [key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth).
Set the `authenticator` to `key_pair` and provide the private key either as a
path or inline:

```php
'snowflake' => [
    'driver' => 'snowflake_native',
    'account' => '{account_name}.eu-west-1',
    'username' => env('SNOWFLAKE_USERNAME'),
    'authenticator' => 'key_pair',

    // Either a path to a PEM-encoded private key...
    'private_key_path' => env('SNOWFLAKE_PRIVATE_KEY_PATH'),

    // ...or the key itself. It is written to a temporary file (0600) that is
    // removed as soon as the connection has been established.
    'private_key' => env('SNOWFLAKE_PRIVATE_KEY'),

    // Optional, when the private key is encrypted.
    'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),

    'database' => env('SNOWFLAKE_DATABASE'),
    'warehouse' => env('SNOWFLAKE_WAREHOUSE'),
    'schema' => 'PUBLIC',
],
```

### Generic ODBC connections

```php
'odbc-connection-name' => [
    'driver' => 'odbc',
    // Either reference a DSN configured in your ODBC manager...
    'dsn' => 'OdbcConnectionName',
    // ...or provide a full connection string:
    // 'dsn' => 'Driver={Driver Name};Server=server.example.com;Port=443;Database={DatabaseName}',
    'username' => env('DB_USERNAME'),
    'password' => env('DB_PASSWORD'),
],
```

## Options

### Column case sensitivity

Snowflake uppercases unquoted identifiers. By default this package follows
that convention and uppercases all column and table names. To use quoted,
case-sensitive identifiers instead, set:

```ini
SNOWFLAKE_COLUMNS_CASE_SENSITIVE=true
```

### Case-insensitive LIKE

The query grammar compiles `LIKE` clauses to `ILIKE` (case-insensitive) by
default, which matches typical Laravel expectations. Disable it per
connection if you want Snowflake's case-sensitive `LIKE` behaviour:

```php
'snowflake' => [
    // ...
    'options' => [
        'use_ilike' => false,
    ],
],
```

### Quoted identifier session behaviour

On connect, the package executes
`ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = false` so quoted
identifiers keep their case. Set the
`SNOWFLAKE_DISABLE_FORCE_QUOTED_IDENTIFIER` environment variable to skip this.

## Usage

Use the query builder and Eloquent as you would with any other connection:

```php
use Illuminate\Support\Facades\DB;

$books = DB::connection('snowflake')
    ->table('books')
    ->where('author', 'Abram Andrea')
    ->get();

$books = Book::where('author', 'Abram Andrea')->get();
```

## Testing

The package ships with a [Pest](https://pestphp.com) test suite:

```bash
composer test
```

## Further documentation

- [Snowflake ODBC notes](docs/snowflake-odbc.md)
- [Snowflake ODBC troubleshooting](docs/snowflake-odbc-troubleshooting.md)
- [Custom `getLastInsertId()` behaviour](docs/custom-last-insert-id.md)
- [Custom processor and grammars](docs/custom-grammars.md)

## Credits & Thanks

This package started as a fork of
[`yoramdelangen/laravel-pdo-odbc`](https://github.com/yoramdelangen/laravel-pdo-odbc)
by Yoram de Langen. Many thanks to Yoram and all of the original
contributors for laying the groundwork for Snowflake and ODBC support in
Laravel.

## License

[MIT](LICENSE.md)
