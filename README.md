## Install

Do the following commands where PATH for PostgreSQL commands of the target
server is set.

    $ cd pg_log_errors
    $ make
    $ sudo make install

## Usage

postgresql.conf:
	shared_preload_libraries = 'pg_log_errors'
