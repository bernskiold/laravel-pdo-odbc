<?php

use Bernskiold\LaravelSnowflake\Tests\Fixtures\BindingCapturingStatement;

it('formats date bindings using the grammar date format', function () {
    $bindings = $this->makeConnection()->prepareBindings([new DateTime('2026-01-02 03:04:05')]);

    expect($bindings)->toBe(['2026-01-02 03:04:05']);
});

it('casts numeric bindings', function () {
    $bindings = $this->makeConnection()->prepareBindings(['42', 7, 1.5, true, false, 'text']);

    expect($bindings)->toBe([42, 7, 1.5, true, false, 'text']);
});

it('preserves numeric strings with leading zeros', function () {
    $bindings = $this->makeConnection()->prepareBindings(['00123', '0', '0500']);

    expect($bindings)->toBe(['00123', 0, '0500']);
});

it('binds booleans as TRUE/FALSE strings and detects parameter types', function () {
    $statement = new BindingCapturingStatement();

    $this->makeConnection()->bindValues($statement, [true, '00123', '42', 'text']);

    expect($statement->bound)->toBe([
        1 => ['TRUE', PDO::PARAM_STR],
        2 => ['00123', PDO::PARAM_STR],
        3 => ['42', PDO::PARAM_INT],
        4 => ['text', PDO::PARAM_STR],
    ]);
});

it('keeps named parameters when binding values', function () {
    $statement = new BindingCapturingStatement();

    $this->makeConnection()->bindValues($statement, ['name' => 'John']);

    expect($statement->bound)->toBe(['name' => ['John', PDO::PARAM_STR]]);
});
