/* Some general headers for custom bgworker facility */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"

#include "utils/memutils.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include "funcapi.h"

#include "constants.c"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);
void _PG_fini(void);

/* Shared memory init */
static void pgss_shmem_startup(void);

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void logerrors_load_params(void);
/* GUC variables */
/* One interval in buffer to count messages (ms) */
static int interval;
/* While that count of intervals messages doesn't dropping from statistic */
static int intervals_count;

/* Worker name */
static char *worker_name = "logerrors";

static emit_log_hook_type prev_emit_log_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* counters of messages (depends on message_types_count) */
static int total_messages_at_last_interval[3];
static int total_messages_at_buffer[3];

typedef struct hashkey {
    int num;
} ErrorCode;

/* Depends on message_types_count, max_number_of_intervals */
typedef struct message_info {
    ErrorCode key;
    pg_atomic_uint32 message_count[3];
    /* Sum in buffer at previous interval */
    int sum_in_buffer[3];
    pg_atomic_uint32 intervals[3][360];
    char *name;
} MessageInfo;

/* Depends on message_types_count */
typedef struct global_info {
    int interval;
    int intervals_count;
    /* index of current interval in buffer */
    pg_atomic_uint32 current_interval_index;
    pg_atomic_uint32 total_count[3];
} GlobalInfo;

static GlobalInfo *global_variables = NULL;

static HTAB *messages_info_hashtable = NULL;

static void
logerrors_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}


static void
logerrors_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}
void logerrors_main(Datum) pg_attribute_noreturn();


static void
logerrors_init()
{
    logerrors_load_params();
    global_variables->intervals_count = intervals_count;
    global_variables->interval = interval;
    pg_atomic_init_u32(&global_variables->current_interval_index, 0);
    MemSet(&global_variables->total_count, 0, message_types_count);
    MemSet(&total_messages_at_last_interval, 0, message_types_count);
    MemSet(&total_messages_at_buffer, 0, message_types_count);
}

static void
logerrors_update_info()
{
    ErrorCode key;
    MessageInfo *info;
    bool found;
    int message_count;

    if (messages_info_hashtable == NULL || global_variables == NULL) {
        return;
    }

    for (int j = 0; j < message_types_count; ++j)
    {
        total_messages_at_last_interval[j] = 0;
        total_messages_at_buffer[j] = 0;
        for (int i = 0; i < error_types_count; ++i)
        {
            key.num = error_codes[i];
            info = hash_search(messages_info_hashtable, (void *)&key, HASH_FIND, &found);
            if (!found) {
                return;
            }
            message_count = pg_atomic_read_u32(&info->message_count[j]);
            info->sum_in_buffer[j] = info->sum_in_buffer[j] -
                                     pg_atomic_read_u32(&info->intervals[j][pg_atomic_read_u32(&global_variables->current_interval_index)]) +
                                     message_count;
            total_messages_at_buffer[j] += info->sum_in_buffer[j];
            total_messages_at_last_interval[j] += message_count;
            pg_atomic_write_u32(&info->intervals[j][pg_atomic_read_u32(&global_variables->current_interval_index)],
                                message_count);
            pg_atomic_write_u32(&info->message_count[j], 0);

        }
    }
    pg_atomic_write_u32(&global_variables->current_interval_index,
                        (pg_atomic_read_u32(&global_variables->current_interval_index) + 1) % global_variables->intervals_count);
}

void
logerrors_main(Datum main_arg)
{
    /* Register functions for SIGTERM/SIGHUP management */
    pqsignal(SIGHUP, logerrors_sighup);
    pqsignal(SIGTERM, logerrors_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to a database */
#if PG_VERSION_NUM < 110000
    BackgroundWorkerInitializeConnection("postgres", NULL);
#else
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif

    /* Creating table if it does not exist */
    logerrors_init();

    while (!got_sigterm)
    {
        int rc;
        /* Wait necessary amount of time */
        rc = WaitLatch(&MyProc->procLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, interval, PG_WAIT_EXTENSION);

        ResetLatch(&MyProc->procLatch);
        /* Emergency bailout if postmaster has died */
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        /* Process signals */
        if (got_sighup)
        {
            /* Process config file */
            ProcessConfigFile(PGC_SIGHUP);
            got_sighup = false;
            ereport(DEBUG1, (errmsg("bgworker logerrors signal: processed SIGHUP")));
            /* Recreate table if needed */
            logerrors_init();
        }

        if (got_sigterm)
        {
            /* Simply exit */
            ereport(DEBUG1, (errmsg("bgworker logerrors signal: processed SIGTERM")));
            proc_exit(0);
        }

        /* Main work happens here */
        logerrors_update_info();
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

/* Log hook */
void
emit_log_hook_impl(ErrorData *edata)
{
    MessageInfo *elem;
    ErrorCode key;
    bool found;
    /* Only if hashtable already inited */
    if (messages_info_hashtable != NULL && global_variables != NULL) {
        for (int j = 0; j < message_types_count; ++j)
        {
            /* Only current message type */
            if (edata->elevel != message_types_codes[j]) {
                continue;
            }
            pg_atomic_fetch_add_u32(&global_variables->total_count[j], 1);
            key.num = edata->sqlerrcode;
            elem = hash_search(messages_info_hashtable, (void *) &key, HASH_FIND, &found);

            if (!found) {
                key.num = not_known_error_code;
                elem = hash_search(messages_info_hashtable, (void *) &key, HASH_FIND, &found);
            }
            pg_atomic_fetch_add_u32(&elem->message_count[j], 1);
        }
    }

    if (prev_emit_log_hook) {
        prev_emit_log_hook(edata);
    }
}

static void
logerrors_load_params(void)
{
    DefineCustomIntVariable("logerrors.interval",
                            "Time between writing stat to buffer (ms).",
                            "Default of 5s, max of 60s",
                            &interval,
                            5000,
                            1000,
                            60000,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL);
    DefineCustomIntVariable("logerrors.intervals_count",
                            "Count of intervals in buffer",
                            "Default of 120, max of 360",
                            &intervals_count,
                            120,
                            2,
                            360,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL);
}
/*
 * Entry point for worker loading
 */
void
_PG_init(void)
{
    if (! process_shared_preload_libraries_in_progress) {
        return;
    }
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pgss_shmem_startup;
    prev_emit_log_hook = emit_log_hook;
    emit_log_hook = emit_log_hook_impl;
    RequestAddinShmemSpace(sizeof(MessageInfo) * error_types_count);
    BackgroundWorker worker;
    /* Worker parameter and registration */
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
                       BGWORKER_BACKEND_DATABASE_CONNECTION;
    /* Start only on master hosts after finishing crash recovery */
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
    sprintf(worker.bgw_library_name, "logerrors");
    sprintf(worker.bgw_function_name, "logerrors_main");
    /* Wait 10 seconds for restart after crash */
    worker.bgw_restart_time = 10;
    worker.bgw_main_arg = (Datum) 0;
    worker.bgw_notify_pid = 0;
    RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
    emit_log_hook = prev_emit_log_hook;
    shmem_startup_hook = prev_shmem_startup_hook;
}

static void
pgss_shmem_startup(void) {

    bool found;
    ErrorCode key;
    MessageInfo *elem;
    HASHCTL ctl;
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    messages_info_hashtable = NULL;
    global_variables = NULL;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(ErrorCode);
    ctl.entrysize = sizeof(MessageInfo);

    messages_info_hashtable = ShmemInitHash("logerrors hash",
                                            error_types_count, error_types_count,
                                            &ctl,
                                            HASH_ELEM | HASH_BLOBS);
    global_variables = ShmemInitStruct("logerrors global_variables",
                                       sizeof(GlobalInfo),
                                       &found);
    for (int i = 0; i < error_types_count; ++i) {
        key.num = error_codes[i];
        elem = hash_search(messages_info_hashtable, (void *) &key, HASH_ENTER, &found);
        for (int j = 0; j < message_types_count; ++j) {
            pg_atomic_init_u32(&elem->message_count[j], 0);
            elem->name = error_names[i];
            MemSet(&(elem->intervals[j]), 0, max_number_of_intervals);
            elem->sum_in_buffer[j] = 0;
        }
    }
    return;
}

PG_FUNCTION_INFO_V1(pg_show_log_errors);

Datum
pg_show_log_errors(PG_FUNCTION_ARGS)
{
#define logerrors_COLS	4
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc	tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    ErrorCode key;
    MessageInfo *info;

    Datum long_interval_values[logerrors_COLS];
    Datum short_interval_values[logerrors_COLS];

    bool long_interval_nulls[logerrors_COLS];
    bool short_interval_nulls[logerrors_COLS];
    bool found;
    int short_interval;
    int long_interval;
    int prev_interval_index;
    int errors_in_long_interval;
    int errors_in_short_interval;
    /* Shmem structs not ready yet */
    if (messages_info_hashtable == NULL) {
        return (Datum) 0;
    }
    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("return type must be a row type")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    for (int lvl_i = 0; lvl_i < message_types_count; ++lvl_i) {

        /* Add total count to result */
        MemSet(long_interval_values, 0, sizeof(long_interval_values));
        MemSet(long_interval_nulls, 0, sizeof(long_interval_nulls));
        for (int j = 0; j < logerrors_COLS; ++j) {
            long_interval_nulls[j] = false;
        }
        /* Time interval */
        long_interval_nulls[0] = true;
        /* Type */
        long_interval_values[1] = CStringGetTextDatum(message_type_names[lvl_i]);
        /* Message */
        long_interval_values[2] = CStringGetTextDatum("TOTAL");
        /* Count */
        long_interval_values[3] = DatumGetInt32(pg_atomic_read_u32(&global_variables->total_count[lvl_i]));
        tuplestore_putvalues(tupstore, tupdesc, long_interval_values, long_interval_nulls);

        /* Add specific error count */
        for (int i = 0; i < error_types_count; ++i) {
            MemSet(long_interval_values, 0, sizeof(long_interval_values));
            MemSet(short_interval_values, 0, sizeof(short_interval_values));
            MemSet(long_interval_nulls, 0, sizeof(long_interval_nulls));
            MemSet(short_interval_nulls, 0, sizeof(short_interval_nulls));
            for (int j = 0; j < logerrors_COLS; ++j) {
                long_interval_nulls[j] = false;
                short_interval_nulls[j] = false;
            }
            key.num = error_codes[i];
            info = hash_search(messages_info_hashtable, (void *) &key, HASH_FIND, &found);
            if (!found) {
                continue;
            }

            short_interval = global_variables->interval / 1000;
            long_interval = short_interval * global_variables->intervals_count;

            /* Time interval */
            long_interval_values[0] = DatumGetInt32(long_interval);
            short_interval_values[0] = DatumGetInt32(short_interval);

            /* Type */
            long_interval_values[1] = CStringGetTextDatum(message_type_names[lvl_i]);
            short_interval_values[1] = CStringGetTextDatum(message_type_names[lvl_i]);
            /* Message */
            long_interval_values[2] = CStringGetTextDatum(info->name);
            short_interval_values[2] = CStringGetTextDatum(info->name);

            /* Count */
            prev_interval_index = (pg_atomic_read_u32(&global_variables->current_interval_index) - 1 + global_variables->intervals_count)
                                  % global_variables->intervals_count;

            errors_in_long_interval = info->sum_in_buffer[lvl_i];
            errors_in_short_interval = pg_atomic_read_u32(&info->intervals[lvl_i][prev_interval_index]);
            long_interval_values[3] = DatumGetInt32(errors_in_long_interval);
            short_interval_values[3] = DatumGetInt32(errors_in_short_interval);

            if (errors_in_long_interval > 0) {
                tuplestore_putvalues(tupstore, tupdesc, long_interval_values, long_interval_nulls);
            }
            if (errors_in_short_interval > 0) {
                tuplestore_putvalues(tupstore, tupdesc, short_interval_values, short_interval_nulls);
            }
        }
    }

    /* return the tuplestore */
    tuplestore_donestoring(tupstore);

    return (Datum) 0;
}
