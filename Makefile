EXTENSION = logerrors
MODULE_big	= logerrors
DATA = logerrors--1.0.sql logerrors--1.0--1.1.sql logerrors--1.1--2.0.sql
OBJS = logerrors.o
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
