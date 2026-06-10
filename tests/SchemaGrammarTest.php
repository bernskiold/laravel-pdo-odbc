<?php

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Fluent;

function schemaConnection()
{
    $connection = test()->makeConnection();
    $connection->useDefaultSchemaGrammar();

    return $connection;
}

function blueprint(string $table, ?Closure $callback = null): Blueprint
{
    return new Blueprint(schemaConnection(), $table, $callback);
}

it('compiles a create table statement', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->create();
        $table->increments('id');
        $table->string('name');
    })->toSql();

    expect($statements)->toBe([
        'create table USERS (ID int not null autoincrement primary key, NAME varchar(255) not null)',
    ]);
});

it('compiles a temporary table', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->create();
        $table->temporary();
        $table->string('name');
    })->toSql();

    expect($statements[0])->toStartWith('create temporary table USERS');
});

it('compiles an add column statement', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->string('email')->nullable();
    })->toSql();

    expect($statements)->toBe(['alter table USERS add column EMAIL varchar(255)']);
});

it('compiles one statement per added column', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->boolean('active')->default(true);
        $table->integer('logins')->default(0);
        $table->string('role')->default('user');
    })->toSql();

    expect($statements)->toBe([
        'alter table USERS add column ACTIVE boolean not null default TRUE',
        'alter table USERS add column LOGINS int not null default 0',
        "alter table USERS add column ROLE varchar(255) not null default 'user'",
    ]);
});

it('compiles change column statements', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->string('name', 100)->nullable()->change();
    })->toSql();

    expect($statements)->toBe([
        'alter table USERS modify column NAME varchar(100)',
        'alter table USERS modify column NAME drop not null',
    ]);
});

it('compiles drop table statements', function () {
    $grammar = schemaConnection()->getSchemaGrammar();
    $table = blueprint('users');

    expect($grammar->compileDrop($table, new Fluent()))->toBe('drop table USERS')
        ->and($grammar->compileDropIfExists($table, new Fluent()))->toBe('drop table if exists USERS');
});

it('compiles a rename table statement', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->rename('accounts');
    })->toSql();

    expect($statements)->toBe(['alter table USERS rename to ACCOUNTS']);
});

it('quotes the arguments of the table exists query', function () {
    $grammar = schemaConnection()->getSchemaGrammar();

    expect($grammar->compileTableExists('TESTDB', 'USERS'))->toBe(
        "select * from information_schema.tables where table_catalog = 'TESTDB' and table_name = 'USERS' and table_type = 'BASE TABLE'"
    );
});

it('compiles json columns to the object type', function () {
    $statements = blueprint('users', function (Blueprint $table) {
        $table->json('settings');
    })->toSql();

    expect($statements)->toBe(['alter table USERS add column SETTINGS object not null']);
});

it('quotes identifiers when case sensitivity is enabled', function () {
    $this->setColumnsCaseSensitive(true);

    $statements = blueprint('users', function (Blueprint $table) {
        $table->string('name');
    })->toSql();

    expect($statements)->toBe(['alter table "users" add column "name" varchar(255) not null']);
});
