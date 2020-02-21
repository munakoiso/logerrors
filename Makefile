EXTENSION = pg_log_errors
MODULE_big	= pg_log_errors
DATA = pg_log_errors--1.0.sql
OBJS = pg_log_errors.o
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
