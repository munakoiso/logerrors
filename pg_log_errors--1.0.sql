\echo Use "CREATE EXTENSION pg_log_errors" to load this file. \quit

CREATE FUNCTION pg_show_log_errors(
    OUT time_interval integer,
    OUT type text,
    OUT message text,
    OUT count integer
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_show_log_errors'
    LANGUAGE C STRICT;
