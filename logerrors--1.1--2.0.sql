ALTER EXTENSION logerrors DROP FUNCTION pg_log_errors_stats();
DROP FUNCTION IF EXISTS pg_log_errors_stats();

CREATE FUNCTION pg_log_errors_stats(
    OUT time_interval integer,
    OUT type text,
    OUT message text,
    OUT count integer,
    OUT username text,
    OUT database text
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_log_errors_stats'
    LANGUAGE C STRICT;
