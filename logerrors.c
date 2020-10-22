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

#include "libpq/libpq.h"
#include "libpq/pqformat.h"

#include "catalog/pg_authid.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "commands/dbcommands.h"

#include "constants.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);
void _PG_fini(void);

/* Shared memory init */
static void pgss_shmem_startup(void);

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

static void logerrors_load_params(void);
/* GUC variables */
/* One interval in buffer to count messages (ms) */
static int interval = 5000;
/* While that count of intervals messages doesn't dropping from statistic */
static int intervals_count = 120;

/* Worker name */
static char *worker_name = "logerrors";

static emit_log_hook_type prev_emit_log_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

typedef struct error_code {
    int num;
} ErrorCode;

/* Depends on message_types_count, max_number_of_intervals */
typedef struct message_info {
    int error_code;
    Oid db_oid;
    Oid user_oid;
    int message_type_index;
} MessageInfo;

typedef struct error_name {
    ErrorCode code;
    char* name;
} ErrorName;

typedef struct slow_log_info {
    pg_atomic_uint32 count;
    pg_atomic_uint64 reset_time;
} SlowLogInfo;

typedef struct messages_buffer {
    LWLock lock;
    int current_interval_index;
    pg_atomic_uint32 current_message_index;
    /* depends on messages per interval and max intervals count */
    MessageInfo buffer[messages_per_interval * max_actual_intervals_count];
} MessagesBuffer;

/* Depends on message_types_count */
typedef struct global_info {
    int interval;
    int intervals_count;
    /* Actual count of intervals in MessagesBuffer */
    int actual_intervals_count;
    /* index of current interval in buffer */
    pg_atomic_uint32 total_count[3];
    SlowLogInfo slow_log_info;
    MessagesBuffer messagesBuffer;
} GlobalInfo;

typedef struct counter_hashelem {
    MessageInfo key;
    int counter;
} CounterHashElem;

static GlobalInfo *global_variables = NULL;

static HTAB *error_names_hashtable = NULL;

void logerrors_emit_log_hook(ErrorData *edata);

static void
logerrors_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

void logerrors_main(Datum) pg_attribute_noreturn();

static void
global_variables_init()
{
    global_variables->intervals_count = intervals_count;
    /* +5 because we don't want take lock on MessagesBuffer while pg_log_errors_stats is running */
    global_variables->actual_intervals_count = intervals_count + 5;
    global_variables->interval = interval;
}

static void
slow_log_info_init()
{
    pg_atomic_init_u32(&global_variables->slow_log_info.count, 0);
    pg_atomic_init_u64(&global_variables->slow_log_info.reset_time, GetCurrentTimestamp());
}

static void
add_message(int errCode, Oid db_oid, Oid user_oid, int message_type_index) {
    int index_to_write;
    int current_message;
    if (global_variables == NULL)
        return;
    LWLockAcquire(&global_variables->messagesBuffer.lock, LW_EXCLUSIVE);
    current_message = pg_atomic_read_u32(&global_variables->messagesBuffer.current_message_index);
    index_to_write = global_variables->messagesBuffer.current_interval_index * messages_per_interval
            + current_message;
    if (current_message >= messages_per_interval) {
        /* too many messages per one interval, save current instead of random message in interval */
        srand(time(0));
        index_to_write = rand() % messages_per_interval;
    }

    global_variables->messagesBuffer.buffer[index_to_write].db_oid = db_oid;
    global_variables->messagesBuffer.buffer[index_to_write].user_oid = user_oid;
    global_variables->messagesBuffer.buffer[index_to_write].error_code = errCode;
    global_variables->messagesBuffer.buffer[index_to_write].message_type_index = message_type_index;
    pg_atomic_write_u32(&global_variables->messagesBuffer.current_message_index, current_message + 1);
    LWLockRelease(&global_variables->messagesBuffer.lock);
}

static char*
get_user_by_oid(Oid user_oid)
{
    HeapTuple role_tuple;
    char* result;
    role_tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(user_oid));
    if (HeapTupleIsValid(role_tuple))
    {
        result = pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(role_tuple))->rolname));
        ReleaseSysCache(role_tuple);
    }
    else
        result = "unknown";
    
    return result;
}


static void
logerrors_init()
{
    bool found;
    ErrorCode key;
    ErrorName* err_name;
    int i;
    for (i = 0; i < error_codes_count; ++i) {
        key.num = error_codes[i];
        err_name = hash_search(error_names_hashtable, (void *) &key, HASH_ENTER, &found);
        err_name->name = (char*)error_names[i];
    }
    pg_atomic_init_u32(&global_variables->messagesBuffer.current_message_index, 0);
    MemSet(&global_variables->total_count, 0, message_types_count);
    for (i = 0; i < message_types_count; ++i) {
        pg_atomic_init_u32(&global_variables->total_count[i], 0);
    }
    for (i = 0; i < messages_per_interval * global_variables->actual_intervals_count; ++i) {
        global_variables->messagesBuffer.buffer[i].error_code = -1;
        global_variables->messagesBuffer.buffer[i].db_oid = -1;
        global_variables->messagesBuffer.buffer[i].user_oid = -1;
        global_variables->messagesBuffer.buffer[i].message_type_index = -1;
    }
    slow_log_info_init();
}

static void
logerrors_update_info()
{
    int i;
    int current_index;
    int prev_index;
    if (global_variables == NULL) {
        return;
    }
    LWLockAcquire(&global_variables->messagesBuffer.lock, LW_EXCLUSIVE);
    prev_index = global_variables->messagesBuffer.current_interval_index;
    global_variables->messagesBuffer.current_interval_index = (prev_index + 1)
            % global_variables->actual_intervals_count;
    current_index = global_variables->messagesBuffer.current_interval_index;
    for (i = 0; i < messages_per_interval; ++i) {
        global_variables->messagesBuffer.buffer[i + current_index * messages_per_interval].error_code = -1;
        global_variables->messagesBuffer.buffer[i + current_index * messages_per_interval].db_oid = -1;
        global_variables->messagesBuffer.buffer[i + current_index * messages_per_interval].user_oid = -1;
        global_variables->messagesBuffer.buffer[i + current_index * messages_per_interval].message_type_index = -1;
    }
    pg_atomic_write_u32(&global_variables->messagesBuffer.current_message_index, 0);
    LWLockRelease(&global_variables->messagesBuffer.lock);
}

void
logerrors_main(Datum main_arg)
{
    /* Register functions for SIGTERM management */
    pqsignal(SIGTERM, logerrors_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

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

        if (got_sigterm)
        {
            /* Simply exit */
            elog(DEBUG1, "bgworker logerrors signal: processed SIGTERM");
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
logerrors_emit_log_hook(ErrorData *edata)
{
    int lvl_i;
    Oid user_oid;
    Oid db_oid;
    /* Only if hashtable already inited */
    if (global_variables != NULL && MyProc != NULL && !proc_exit_inprogress) {
        for (lvl_i = 0; lvl_i < message_types_count; ++lvl_i)
        {
            /* Only current message type */
            if (edata->elevel != message_types_codes[lvl_i]) {
                continue;
            }
            if (MyProcPort) {
                user_oid = get_role_oid(MyProcPort->user_name, true);
                db_oid = get_database_oid(MyProcPort->database_name, true);
            }
            else {
                user_oid = -1;
                db_oid = -1;
            }
            add_message(edata->sqlerrcode, db_oid, user_oid, lvl_i);
            pg_atomic_fetch_add_u32(&global_variables->total_count[lvl_i], 1);
        }
        if (edata && edata->message && strstr(edata->message, "duration:"))
        {
            pg_atomic_fetch_add_u32(&global_variables->slow_log_info.count, 1);
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
                            PGC_SUSET,
                            GUC_UNIT_MS | GUC_NO_RESET_ALL,
                            NULL,
                            NULL,
                            NULL);
    DefineCustomIntVariable("logerrors.intervals_count",
                            "Count of intervals in buffer",
                            "Default of 120, max of 360",
                            &intervals_count,
                            120,
                            2,
                            max_intervals_count,
                            PGC_SUSET,
                            GUC_NO_RESET_ALL,
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
    BackgroundWorker worker;
    if (!process_shared_preload_libraries_in_progress) {
        return;
    }
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = pgss_shmem_startup;
    prev_emit_log_hook = emit_log_hook;
    emit_log_hook = logerrors_emit_log_hook;
    RequestAddinShmemSpace((sizeof(ErrorCode) + sizeof(ErrorName)) * error_codes_count + sizeof(GlobalInfo));
    /* Worker parameter and registration */
    MemSet(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
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
    logerrors_load_params();
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
    HASHCTL ctl;
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    error_names_hashtable = NULL;
    global_variables = NULL;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(ErrorCode);
    ctl.entrysize = sizeof(ErrorName);
    error_names_hashtable = ShmemInitHash("logerrors hash",
                                            error_codes_count, error_codes_count,
                                            &ctl,
                                            HASH_ELEM | HASH_BLOBS);
    global_variables = ShmemInitStruct("logerrors global_variables",
                                       sizeof(GlobalInfo),
                                       &found);
    if (!IsUnderPostmaster) {
        global_variables_init();
        logerrors_init();
    }
    return;
}

PG_FUNCTION_INFO_V1(pg_log_errors_stats);

static void
count_up_errors(int duration_in_intervals, int current_interval, HTAB* counters_hashtable) {
    bool found;
    int i;
    int j;
    int interval_index;
    int message_index;
    MessageInfo key;
    CounterHashElem* elem;
    if (global_variables == NULL || counters_hashtable == NULL){
        return;
    }
    /* put all messages to hashtable */
    for (i = duration_in_intervals; i > 0; --i) {
        interval_index = (current_interval - i + global_variables->actual_intervals_count)
                % global_variables->actual_intervals_count;
        for (j = 0; j < messages_per_interval; ++j) {
            message_index = interval_index * messages_per_interval + j;
            if (global_variables->messagesBuffer.buffer[message_index].error_code == -1)
                continue;
            key.db_oid = global_variables->messagesBuffer.buffer[message_index].db_oid;
            key.user_oid = global_variables->messagesBuffer.buffer[message_index].user_oid;
            key.error_code = global_variables->messagesBuffer.buffer[message_index].error_code;
            key.message_type_index = global_variables->messagesBuffer.buffer[message_index].message_type_index;
            elem = hash_search(counters_hashtable, (void *) &key, HASH_FIND, &found);
            if (!found) {
                elem = hash_search(counters_hashtable, (void *) &key, HASH_ENTER, &found);
                elem->counter = 0;
            }
            elem->counter++;
        }
    }
}

static void
put_values_to_tuple(
        int current_interval_index,
        int duration_in_intervals,
        HTAB* counters_hashtable,
        TupleDesc tupdesc,
        Tuplestorestate *tupstore){
#define logerrors_COLS	6
    Datum long_interval_values[logerrors_COLS];
    bool long_interval_nulls[logerrors_COLS];
    bool found;
    int message_index;
    int interval_index;
    int i;
    int j;
    char* db_name;
    ErrorName* err_name;
    MessageInfo key;
    ErrorCode err_code;
    CounterHashElem *elem;
    if (global_variables == NULL || counters_hashtable == NULL){
        return;
    }
    count_up_errors(duration_in_intervals, current_interval_index, counters_hashtable);
    for (i = duration_in_intervals; i > 0 ; --i) {
        interval_index = (current_interval_index - i + global_variables->actual_intervals_count)
                % global_variables->actual_intervals_count;
        for (j = 0; j < messages_per_interval; ++j) {
            message_index = interval_index * messages_per_interval + j;
            if (global_variables->messagesBuffer.buffer[message_index].error_code == -1)
                continue;
            key.db_oid = global_variables->messagesBuffer.buffer[message_index].db_oid;
            key.user_oid = global_variables->messagesBuffer.buffer[message_index].user_oid;
            key.error_code = global_variables->messagesBuffer.buffer[message_index].error_code;
            key.message_type_index = global_variables->messagesBuffer.buffer[message_index].message_type_index;
            elem = hash_search(counters_hashtable, (void *) &key, HASH_FIND, &found);
            if (!found) {
                /* we already put this king of message to output */
                continue;
            }

            MemSet(long_interval_values, 0, sizeof(long_interval_values));
            MemSet(long_interval_nulls, 0, sizeof(long_interval_nulls));
            for (j = 0; j < logerrors_COLS; ++j) {
                long_interval_nulls[j] = false;
            }
            /* Time interval */
            long_interval_values[0] = DatumGetInt32(global_variables->interval * duration_in_intervals / 1000);
            /* Type */
            long_interval_values[1] = CStringGetTextDatum(message_type_names[key.message_type_index]);
            /* Message */
            err_code.num = key.error_code;
            err_name = hash_search(error_names_hashtable, (void *) &err_code, HASH_FIND, &found);
            long_interval_values[2] = CStringGetTextDatum(err_name->name);
            /* Count */
            long_interval_values[3] = DatumGetInt32(elem->counter);
            /* Username */
            long_interval_values[4] = CStringGetTextDatum(get_user_by_oid(key.user_oid));
            /* Database name */
            db_name = get_database_name(key.db_oid);
            if (db_name == NULL)
                long_interval_values[5] = CStringGetTextDatum("unknown");
            else
                long_interval_values[5] = CStringGetTextDatum(db_name);

            if (elem->counter > 0) {
                tuplestore_putvalues(tupstore, tupdesc, long_interval_values, long_interval_nulls);
            }
            /* Now remove key from hashtable */
            elem = hash_search(counters_hashtable, (void *) &key, HASH_REMOVE, &found);
        }
    }
}


Datum
pg_log_errors_stats(PG_FUNCTION_ARGS)
{
#define logerrors_COLS	6
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc	tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    HASHCTL ctl;
    HTAB* counters_hashtable;
    Datum long_interval_values[logerrors_COLS];

    bool long_interval_nulls[logerrors_COLS];
    int current_interval_index;
    int lvl_i;
    int j;
    /* Shmem structs not ready yet */
    if (error_names_hashtable == NULL || global_variables == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("logerrors must be loaded via shared_preload_libraries")));
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

    counters_hashtable = NULL;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(MessageInfo);
    ctl.entrysize = sizeof(CounterHashElem);
    /* an unshared hashtable can be expanded on-the-fly */
    counters_hashtable = hash_create("counters hashtable", 1, &ctl, HASH_ELEM | HASH_BLOBS);

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    LWLockAcquire(&global_variables->messagesBuffer.lock, LW_EXCLUSIVE);
    current_interval_index = global_variables->messagesBuffer.current_interval_index;
    LWLockRelease(&global_variables->messagesBuffer.lock);
    /* 'TOTAL' counters */
    for (lvl_i = 0; lvl_i < message_types_count; ++lvl_i) {

        /* Add total count to result */
        MemSet(long_interval_values, 0, sizeof(long_interval_values));
        MemSet(long_interval_nulls, 0, sizeof(long_interval_nulls));
        for (j = 0; j < logerrors_COLS; ++j) {
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
        /* Username */
        long_interval_nulls[4] = true;
        /* Database name */
        long_interval_nulls[5] = true;
        tuplestore_putvalues(tupstore, tupdesc, long_interval_values, long_interval_nulls);
    }
    /* short interval counters */
    put_values_to_tuple(current_interval_index, 1, counters_hashtable, tupdesc, tupstore);
    /* long interval counters */
    put_values_to_tuple(current_interval_index, global_variables->intervals_count, counters_hashtable, tupdesc,
            tupstore);
    /* return the tuplestore */
    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pg_log_errors_reset);

Datum
pg_log_errors_reset(PG_FUNCTION_ARGS) {

    if (error_names_hashtable == NULL || global_variables == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("logerrors must be loaded via shared_preload_libraries")));
    }

    logerrors_init();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_slow_log_stats);

Datum
pg_slow_log_stats(PG_FUNCTION_ARGS)
{
#define SLOW_LOG_COLS 2
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    int i;

    Datum result_values[SLOW_LOG_COLS];
    bool result_nulls[SLOW_LOG_COLS];

    /* Shmem structs not ready yet */
    if (global_variables == NULL) {
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

    MemSet(result_values, 0, sizeof(result_values));
    MemSet(result_nulls, 0, sizeof(result_nulls));
    for (i = 0; i < SLOW_LOG_COLS; i++) {
        result_nulls[i] = false;
    }
    result_values[0] = DatumGetInt32(pg_atomic_read_u32(&global_variables->slow_log_info.count));
    result_values[1] = DatumGetTimestamp(pg_atomic_read_u64(&global_variables->slow_log_info.reset_time));

    tuplestore_putvalues(tupstore, tupdesc, result_values, result_nulls);
    /* return the tuplestore */
    tuplestore_donestoring(tupstore);
    return (Datum) 0;
}
