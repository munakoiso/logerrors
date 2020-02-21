# contrib/pg_log_errors/Makefile

MODULE_big	= pg_log_errors
OBJS = \
	$(WIN32RES) \
	pg_log_errors.o

EXTENSION = pg_log_errors
DATA = pg_log_errors--1.0.sql
PGFILEDESC = "lalalalalala"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_log_errors
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endi