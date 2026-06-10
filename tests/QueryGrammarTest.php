<?php

it('uppercases identifiers by default', function () {
    $sql = $this->makeConnection()->query()->from('users')->where('name', 'John')->toSql();

    expect($sql)->toBe('select * from USERS where NAME = ?');
});

it('quotes identifiers when case sensitivity is enabled', function () {
    $this->setColumnsCaseSensitive(true);

    $sql = $this->makeConnection()->query()->from('users')->where('name', 'John')->toSql();

    expect($sql)->toBe('select * from "users" where "name" = ?');
});

it('compiles like to ilike by default', function () {
    $sql = $this->makeConnection()->query()->from('users')->where('name', 'like', '%john%')->toSql();

    expect($sql)->toBe('select * from USERS where NAME ilike ?');
});

it('compiles not like to not ilike by default', function () {
    $sql = $this->makeConnection()->query()->from('users')->where('name', 'not like', '%john%')->toSql();

    expect($sql)->toBe('select * from USERS where NAME not ilike ?');
});

it('can disable ilike per connection', function () {
    $sql = $this->makeConnection(['options' => ['use_ilike' => false]])
        ->query()
        ->from('users')
        ->where('name', 'like', '%john%')
        ->toSql();

    expect($sql)->toBe('select * from USERS where NAME like ?');
});

it('respects the case sensitivity flag of whereLike', function () {
    $insensitive = $this->makeConnection()->query()->from('users')->whereLike('name', '%john%')->toSql();
    $sensitive = $this->makeConnection()->query()->from('users')->whereLike('name', '%john%', true)->toSql();

    expect($insensitive)->toBe('select * from USERS where NAME ilike ?')
        ->and($sensitive)->toBe('select * from USERS where NAME like ?');
});

it('uses a native date cast for whereDate', function () {
    $sql = $this->makeConnection()->query()->from('orders')->whereDate('created_at', '2026-01-01')->toSql();

    expect($sql)->toBe('select * from ORDERS where CREATED_AT = ?::DATE');
});

it('uses TO_VARCHAR for whereYear', function () {
    $sql = $this->makeConnection()->query()->from('orders')->whereYear('created_at', 2026)->toSql();

    expect($sql)->toBe("select * from ORDERS where TO_VARCHAR(CREATED_AT, 'YYYY') = ?");
});

it('compiles truncate to truncate table', function () {
    $connection = $this->makeConnection();
    $query = $connection->query()->from('users');

    expect($connection->getQueryGrammar()->compileTruncate($query))->toBe('truncate table USERS');
});

it('aliases aggregates as aggregate', function () {
    $query = $this->makeConnection()->query()->from('users');
    $query->aggregate = ['function' => 'count', 'columns' => ['*']];

    expect($query->toSql())->toBe('select count(*) as "aggregate" from USERS');
});

it('compiles locks to nothing as Snowflake does not support them', function () {
    $sql = $this->makeConnection()->query()->from('users')->lockForUpdate()->toSql();

    expect($sql)->toBe('select * from USERS');
});

it('compiles limit and offset', function () {
    $sql = $this->makeConnection()->query()->from('users')->limit(10)->offset(5)->toSql();

    expect($sql)->toBe('select * from USERS limit 10 offset 5');
});
