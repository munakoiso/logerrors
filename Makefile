MODULES = pg_log_errors

PG_CPPFLAGS = -I$(libpq_srcdir) -L$(libdir)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
