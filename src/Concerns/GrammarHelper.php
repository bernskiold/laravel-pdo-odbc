<?php

namespace Bernskiold\LaravelSnowflake\Concerns;

use Illuminate\Support\Str;

/**
 * Shared identifier handling for the Snowflake query and schema grammars.
 *
 * Snowflake folds unquoted identifiers to uppercase. By default this package
 * follows that convention: identifiers are uppercased and left unquoted, so
 * they match what Snowflake stores. When case sensitivity is enabled (via the
 * "options.case_sensitive" connection option, or the
 * SNOWFLAKE_COLUMNS_CASE_SENSITIVE environment variable), identifiers are
 * wrapped in double quotes and keep the casing used in the query.
 */
trait GrammarHelper
{
    /**
     * Determine if identifiers should be treated as case-sensitive.
     */
    public function isCaseSensitive(): bool
    {
        $configured = $this->connection?->getConfig('options.case_sensitive');

        if (null !== $configured) {
            return (bool) $configured;
        }

        return (bool) env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false);
    }

    /**
     * Fold an identifier name the way Snowflake stores it, for comparisons
     * against information_schema and SHOW output.
     */
    public function caseFoldName(string $name): string
    {
        return $this->isCaseSensitive() ? $name : Str::upper($name);
    }

    /**
     * Wrap a single identifier segment in keyword identifiers.
     *
     * @param string $value
     *
     * @return string
     */
    protected function wrapValue($value)
    {
        if ('*' === $value) {
            return $value;
        }

        if (! $this->isCaseSensitive()) {
            return Str::upper(str_replace('"', '', $value));
        }

        return '"'.str_replace('"', '""', $value).'"';
    }
}
