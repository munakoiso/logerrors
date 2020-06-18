CREATE FUNCTION pg_slow_log_stats(
    OUT slow_count integer,
    OUT reset_time timestamp
)
    RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_slow_log_stats'
    LANGUAGE C STRICT;
GRANT ALL ON FUNCTION pg_slow_log_stats() TO public;
