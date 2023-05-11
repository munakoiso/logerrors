Extension for PostgreSQL for collecting statistics about messages in logfile.

Configuration variables:
* `logerrors.interval` - Time between writing statistic to buffer (ms). Default of **5s**, max of **60s**;
* `logerrors.intervals_count` - Count of intervals in buffer. Default of **120**, max of **360**. During this count of intervals messages doesn't dropping from statistic;
* `logerrors.excluded_errcodes` - Excluded error codes separated by "**,**".

## Install

The extension must be loaded via `shared_preload_libraries`.

Run psql command:

    $ CREATE EXTENSION logerrors;

## Tests

The extension uses standard pgxs regression tests. Run `make installcheck` to run all psql scripts defined in `sql` directory. Output of each is then evaluated by `diff` with corresponding expected output stored in the `expected` directory. 

```
    $ make installcheck
    +++ regress install-check in  +++
    ============== creating temporary instance            ==============
    ============== initializing database system           ==============
    ============== starting postmaster                    ==============
    running on port 51698 with PID 472134
    ============== creating database "contrib_regression" ==============
    CREATE DATABASE
    ALTER DATABASE
    ALTER DATABASE
    ALTER DATABASE
    ALTER DATABASE
    ALTER DATABASE
    ALTER DATABASE
    ============== installing logerrors                   ==============
    CREATE EXTENSION
    ============== creating role "postgres"               ==============
    CREATE ROLE
    GRANT
    ============== running regression test queries        ==============
    test logerrors                    ... ok        15017 ms
    ============== shutting down postmaster               ==============
    ============== removing temporary instance            ==============

=====================
 All 1 tests passed. 
=====================

```

## Usage

   After creating extension you can call `pg_log_errors_stats()` function in psql (without any arguments).

```
    postgres=# select * from pg_log_errors_stats();
     time_interval |  type   |       message        | count | username | database | sqlstate 
    ---------------+---------+----------------------+-------+----------+----------+----------
                   | WARNING | TOTAL                |     0 |          |          | 
                   | ERROR   | TOTAL                |     1 |          |          | 
                   | FATAL   | TOTAL                |     0 |          |          | 
                 5 | ERROR   | ERRCODE_SYNTAX_ERROR |     1 | postgres | postgres | 42601
               600 | ERROR   | ERRCODE_SYNTAX_ERROR |     1 | postgres | postgres | 42601
```
In output you can see 7 columns:

    time_interval: how long (in seconds) has statistics been collected.
    type: postgresql type of message (now supports only these: warning, error, fatal).
    message: code of message from log_hook. (or 'TOTAL' for total count of that type messages)
    count: count of messages of this type at this time_interval in log.
    username: effective role causing the message
    database: database where the message comes from
    sqlstate: code of the message transformed to the form of sqlstate

To get number of lines in slow log call `pg_slow_log_stats()`:

```
    postgres=# select * from pg_slow_log_stats();
     slow_count |         reset_time
    ------------+----------------------------
              1 | 2020-06-13 00:19:31.084923
    (1 row)
```

To reset all statistics use
```
    postgres=# select pg_log_errors_reset();
```
