\echo Use "CREATE EXTENSION logerrors" to load this file. \quit

CREATE FUNCTION pg_log_errors_stats(
    OUT time_interval integer,
    OUT type text,
    OUT message text,
    OUT count integer,
    OUT username text,
    OUT database text,
    OUT sqlstate text
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_log_errors_stats'
    LANGUAGE C STRICT;

CREATE FUNCTION pg_log_errors_reset()
    RETURNS void
AS 'MODULE_PATHNAME', 'pg_log_errors_reset'
    LANGUAGE C STRICT;
