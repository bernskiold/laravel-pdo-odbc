<?php

use Bernskiold\LaravelSnowflake\PDO\Statement;

function statementPdo(): PDO
{
    $pdo = new PDO('sqlite::memory:');
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(PDO::ATTR_STATEMENT_CLASS, [Statement::class, [$pdo]]);

    return $pdo;
}

it('interpolates bound values into the query', function () {
    $pdo = statementPdo();

    $statement = $pdo->prepare('select ? as value');
    $statement->bindValue(1, 'hello');
    $statement->execute();

    expect($statement->fetchColumn())->toBe('hello');
});

it('casts integer bindings', function () {
    $pdo = statementPdo();

    $statement = $pdo->prepare('select ? as value');
    $statement->bindValue(1, '42', PDO::PARAM_INT);
    $statement->execute();

    expect($statement->fetchColumn())->toBe(42);
});

it('executes DDL statements directly', function () {
    $pdo = statementPdo();

    $statement = $pdo->prepare('CREATE TABLE demo (id int)');
    $statement->execute();

    $check = $pdo->prepare("select name from sqlite_master where type = 'table' and name = 'demo'");
    $check->execute();

    expect($check->fetchColumn())->toBe('demo');
});
