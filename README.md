Extension for PostgreSQL for collecting statistics about messages in logfile

## Install

Run psql command:

    $ CREATE EXTENSION logerrors;

## Usage

   After creating extension you can call pg_show_log_errors function in psql(without any arguments)

```
    postgres=# select * from pg_show_log_errors();
     time_interval |  type   |       message        | count
    ---------------+---------+----------------------+-------
                   | WARNING | TOTAL                |     0
                   | ERROR   | TOTAL                |     3
               600 | ERROR   | ERRCODE_SYNTAX_ERROR |     3
                 5 | ERROR   | ERRCODE_SYNTAX_ERROR |     2
                   | FATAL   | TOTAL                |     0
```
In output you can see 4 columns:

    time_interval: how long (in seconds) has statistics been collected.
    type: postgresql type of message (now supports only these: warning, error, fatal).
    message: code of message from log_hook. (or 'TOTAL' for total count of that type messages)
    count: count of massages of this type at this time_interval in log.
