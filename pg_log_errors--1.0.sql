/* contrib/pg_log_errors/pg_log_errors--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_log_errors" to load this file. \quit

--
-- pg_show_log_errors()
--

CREATE FUNCTION pg_show_log_errors(
    OUT time_interval integer,
    OUT type text,
    OUT message text,
    OUT count integer
)
    RETURNS SETOF record
AS 'pg_log_errors', 'pg_show_log_errors'

    LANGUAGE C STRICT;
