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
// for work with shmem
#include "utils/memutils.h"
// for work with htab
#include "utils/hsearch.h"

/* Allow load of this module in shared libs */
PG_MODULE_MAGIC;

/* Entry point of library loading */
void _PG_init(void);
void _PG_fini(void);

/* Shared memory init */
static void pgss_shmem_startup(void);

static void pg_log_errors_init_intervals(void);

void pg_log_errors_main(Datum) pg_attribute_noreturn();
/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void pg_log_errors_load_params(void);
/* GUC variables */
// one interval in buffer to count messages (ms)
static int interval = 5000;
// while that count of intervals messages doesn't dropping from statistic
static int intervals_count = 120;

/* Worker name */
static char *worker_name = "pg_log_errors";

static emit_log_hook_type prev_emit_log_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

const int error_types_count = 240;
// Error types to show in output
const char error_names[][100] = {"NOT_KNOWN_ERROR", "ERRCODE_SUCCESSFUL_COMPLETION", "ERRCODE_WARNING", "ERRCODE_WARNING_DYNAMIC_RESULT_SETS_RETURNED", "ERRCODE_WARNING_IMPLICIT_ZERO_BIT_PADDING", "ERRCODE_WARNING_NULL_VALUE_ELIMINATED_IN_SET_FUNCTION", "ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED", "ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED", "ERRCODE_WARNING_STRING_DATA_RIGHT_TRUNCATION", "ERRCODE_WARNING_DEPRECATED_FEATURE", "ERRCODE_NO_DATA", "ERRCODE_NO_ADDITIONAL_DYNAMIC_RESULT_SETS_RETURNED", "ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE", "ERRCODE_CONNECTION_EXCEPTION", "ERRCODE_CONNECTION_DOES_NOT_EXIST", "ERRCODE_CONNECTION_FAILURE", "ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION", "ERRCODE_SQLSERVER_REJECTED_ESTABLISHMENT_OF_SQLCONNECTION", "ERRCODE_TRANSACTION_RESOLUTION_UNKNOWN", "ERRCODE_PROTOCOL_VIOLATION", "ERRCODE_TRIGGERED_ACTION_EXCEPTION", "ERRCODE_FEATURE_NOT_SUPPORTED", "ERRCODE_INVALID_TRANSACTION_INITIATION", "ERRCODE_LOCATOR_EXCEPTION", "ERRCODE_L_E_INVALID_SPECIFICATION", "ERRCODE_INVALID_GRANTOR", "ERRCODE_INVALID_GRANT_OPERATION", "ERRCODE_INVALID_ROLE_SPECIFICATION", "ERRCODE_DIAGNOSTICS_EXCEPTION", "ERRCODE_STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER", "ERRCODE_CASE_NOT_FOUND", "ERRCODE_CARDINALITY_VIOLATION", "ERRCODE_DATA_EXCEPTION", "ERRCODE_ARRAY_ELEMENT_ERROR", "ERRCODE_CHARACTER_NOT_IN_REPERTOIRE", "ERRCODE_DATETIME_FIELD_OVERFLOW", "ERRCODE_DIVISION_BY_ZERO", "ERRCODE_ERROR_IN_ASSIGNMENT", "ERRCODE_ESCAPE_CHARACTER_CONFLICT", "ERRCODE_INDICATOR_OVERFLOW", "ERRCODE_INTERVAL_FIELD_OVERFLOW", "ERRCODE_INVALID_ARGUMENT_FOR_LOG", "ERRCODE_INVALID_ARGUMENT_FOR_NTILE", "ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE", "ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION", "ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION", "ERRCODE_INVALID_CHARACTER_VALUE_FOR_CAST", "ERRCODE_INVALID_DATETIME_FORMAT", "ERRCODE_INVALID_ESCAPE_CHARACTER", "ERRCODE_INVALID_ESCAPE_OCTET", "ERRCODE_INVALID_ESCAPE_SEQUENCE", "ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER", "ERRCODE_INVALID_INDICATOR_PARAMETER_VALUE", "ERRCODE_INVALID_PARAMETER_VALUE", "ERRCODE_INVALID_REGULAR_EXPRESSION", "ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE", "ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE", "ERRCODE_INVALID_TABLESAMPLE_ARGUMENT", "ERRCODE_INVALID_TABLESAMPLE_REPEAT", "ERRCODE_INVALID_TIME_ZONE_DISPLACEMENT_VALUE", "ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER", "ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH", "ERRCODE_NULL_VALUE_NOT_ALLOWED", "ERRCODE_NULL_VALUE_NO_INDICATOR_PARAMETER", "ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE", "ERRCODE_SEQUENCE_GENERATOR_LIMIT_EXCEEDED", "ERRCODE_STRING_DATA_LENGTH_MISMATCH", "ERRCODE_STRING_DATA_RIGHT_TRUNCATION", "ERRCODE_SUBSTRING_ERROR", "ERRCODE_TRIM_ERROR", "ERRCODE_UNTERMINATED_C_STRING", "ERRCODE_ZERO_LENGTH_CHARACTER_STRING", "ERRCODE_FLOATING_POINT_EXCEPTION", "ERRCODE_INVALID_TEXT_REPRESENTATION", "ERRCODE_INVALID_BINARY_REPRESENTATION", "ERRCODE_BAD_COPY_FILE_FORMAT", "ERRCODE_UNTRANSLATABLE_CHARACTER", "ERRCODE_NOT_AN_XML_DOCUMENT", "ERRCODE_INVALID_XML_DOCUMENT", "ERRCODE_INVALID_XML_CONTENT", "ERRCODE_INVALID_XML_COMMENT", "ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION", "ERRCODE_RESTRICT_VIOLATION", "ERRCODE_NOT_NULL_VIOLATION", "ERRCODE_FOREIGN_KEY_VIOLATION", "ERRCODE_UNIQUE_VIOLATION", "ERRCODE_CHECK_VIOLATION", "ERRCODE_EXCLUSION_VIOLATION", "ERRCODE_INVALID_CURSOR_STATE", "ERRCODE_INVALID_TRANSACTION_STATE", "ERRCODE_ACTIVE_SQL_TRANSACTION", "ERRCODE_BRANCH_TRANSACTION_ALREADY_ACTIVE", "ERRCODE_HELD_CURSOR_REQUIRES_SAME_ISOLATION_LEVEL", "ERRCODE_INAPPROPRIATE_ACCESS_MODE_FOR_BRANCH_TRANSACTION", "ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION", "ERRCODE_NO_ACTIVE_SQL_TRANSACTION_FOR_BRANCH_TRANSACTION", "ERRCODE_READ_ONLY_SQL_TRANSACTION", "ERRCODE_SCHEMA_AND_DATA_STATEMENT_MIXING_NOT_SUPPORTED", "ERRCODE_NO_ACTIVE_SQL_TRANSACTION", "ERRCODE_IN_FAILED_SQL_TRANSACTION", "ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT", "ERRCODE_INVALID_SQL_STATEMENT_NAME", "ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION", "ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION", "ERRCODE_INVALID_PASSWORD", "ERRCODE_DEPENDENT_PRIVILEGE_DESCRIPTORS_STILL_EXIST", "ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST", "ERRCODE_INVALID_TRANSACTION_TERMINATION", "ERRCODE_SQL_ROUTINE_EXCEPTION", "ERRCODE_S_R_E_FUNCTION_EXECUTED_NO_RETURN_STATEMENT", "ERRCODE_S_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED", "ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED", "ERRCODE_S_R_E_READING_SQL_DATA_NOT_PERMITTED", "ERRCODE_INVALID_CURSOR_NAME", "ERRCODE_EXTERNAL_ROUTINE_EXCEPTION", "ERRCODE_E_R_E_CONTAINING_SQL_NOT_PERMITTED", "ERRCODE_E_R_E_MODIFYING_SQL_DATA_NOT_PERMITTED", "ERRCODE_E_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED", "ERRCODE_E_R_E_READING_SQL_DATA_NOT_PERMITTED", "ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION", "ERRCODE_E_R_I_E_INVALID_SQLSTATE_RETURNED", "ERRCODE_E_R_I_E_NULL_VALUE_NOT_ALLOWED", "ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED", "ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED", "ERRCODE_E_R_I_E_EVENT_TRIGGER_PROTOCOL_VIOLATED", "ERRCODE_SAVEPOINT_EXCEPTION", "ERRCODE_S_E_INVALID_SPECIFICATION", "ERRCODE_INVALID_CATALOG_NAME", "ERRCODE_INVALID_SCHEMA_NAME", "ERRCODE_TRANSACTION_ROLLBACK", "ERRCODE_T_R_INTEGRITY_CONSTRAINT_VIOLATION", "ERRCODE_T_R_SERIALIZATION_FAILURE", "ERRCODE_T_R_STATEMENT_COMPLETION_UNKNOWN", "ERRCODE_T_R_DEADLOCK_DETECTED", "ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION", "ERRCODE_SYNTAX_ERROR", "ERRCODE_INSUFFICIENT_PRIVILEGE", "ERRCODE_CANNOT_COERCE", "ERRCODE_GROUPING_ERROR", "ERRCODE_WINDOWING_ERROR", "ERRCODE_INVALID_RECURSION", "ERRCODE_INVALID_FOREIGN_KEY", "ERRCODE_INVALID_NAME", "ERRCODE_NAME_TOO_LONG", "ERRCODE_RESERVED_NAME", "ERRCODE_DATATYPE_MISMATCH", "ERRCODE_INDETERMINATE_DATATYPE", "ERRCODE_COLLATION_MISMATCH", "ERRCODE_INDETERMINATE_COLLATION", "ERRCODE_WRONG_OBJECT_TYPE", "ERRCODE_GENERATED_ALWAYS", "ERRCODE_UNDEFINED_COLUMN", "ERRCODE_UNDEFINED_FUNCTION", "ERRCODE_UNDEFINED_TABLE", "ERRCODE_UNDEFINED_PARAMETER", "ERRCODE_UNDEFINED_OBJECT", "ERRCODE_DUPLICATE_COLUMN", "ERRCODE_DUPLICATE_CURSOR", "ERRCODE_DUPLICATE_DATABASE", "ERRCODE_DUPLICATE_FUNCTION", "ERRCODE_DUPLICATE_PSTATEMENT", "ERRCODE_DUPLICATE_SCHEMA", "ERRCODE_DUPLICATE_TABLE", "ERRCODE_DUPLICATE_ALIAS", "ERRCODE_DUPLICATE_OBJECT", "ERRCODE_AMBIGUOUS_COLUMN", "ERRCODE_AMBIGUOUS_FUNCTION", "ERRCODE_AMBIGUOUS_PARAMETER", "ERRCODE_AMBIGUOUS_ALIAS", "ERRCODE_INVALID_COLUMN_REFERENCE", "ERRCODE_INVALID_COLUMN_DEFINITION", "ERRCODE_INVALID_CURSOR_DEFINITION", "ERRCODE_INVALID_DATABASE_DEFINITION", "ERRCODE_INVALID_FUNCTION_DEFINITION", "ERRCODE_INVALID_PSTATEMENT_DEFINITION", "ERRCODE_INVALID_SCHEMA_DEFINITION", "ERRCODE_INVALID_TABLE_DEFINITION", "ERRCODE_INVALID_OBJECT_DEFINITION", "ERRCODE_WITH_CHECK_OPTION_VIOLATION", "ERRCODE_INSUFFICIENT_RESOURCES", "ERRCODE_DISK_FULL", "ERRCODE_OUT_OF_MEMORY", "ERRCODE_TOO_MANY_CONNECTIONS", "ERRCODE_CONFIGURATION_LIMIT_EXCEEDED", "ERRCODE_PROGRAM_LIMIT_EXCEEDED", "ERRCODE_STATEMENT_TOO_COMPLEX", "ERRCODE_TOO_MANY_COLUMNS", "ERRCODE_TOO_MANY_ARGUMENTS", "ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE", "ERRCODE_OBJECT_IN_USE", "ERRCODE_CANT_CHANGE_RUNTIME_PARAM", "ERRCODE_LOCK_NOT_AVAILABLE", "ERRCODE_OPERATOR_INTERVENTION", "ERRCODE_QUERY_CANCELED", "ERRCODE_ADMIN_SHUTDOWN", "ERRCODE_CRASH_SHUTDOWN", "ERRCODE_CANNOT_CONNECT_NOW", "ERRCODE_DATABASE_DROPPED", "ERRCODE_SYSTEM_ERROR", "ERRCODE_IO_ERROR", "ERRCODE_UNDEFINED_FILE", "ERRCODE_DUPLICATE_FILE", "ERRCODE_SNAPSHOT_TOO_OLD", "ERRCODE_CONFIG_FILE_ERROR", "ERRCODE_LOCK_FILE_EXISTS", "ERRCODE_FDW_ERROR", "ERRCODE_FDW_COLUMN_NAME_NOT_FOUND", "ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED", "ERRCODE_FDW_FUNCTION_SEQUENCE_ERROR", "ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION", "ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE", "ERRCODE_FDW_INVALID_COLUMN_NAME", "ERRCODE_FDW_INVALID_COLUMN_NUMBER", "ERRCODE_FDW_INVALID_DATA_TYPE", "ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS", "ERRCODE_FDW_INVALID_DESCRIPTOR_FIELD_IDENTIFIER", "ERRCODE_FDW_INVALID_HANDLE", "ERRCODE_FDW_INVALID_OPTION_INDEX", "ERRCODE_FDW_INVALID_OPTION_NAME", "ERRCODE_FDW_INVALID_STRING_LENGTH_OR_BUFFER_LENGTH", "ERRCODE_FDW_INVALID_STRING_FORMAT", "ERRCODE_FDW_INVALID_USE_OF_NULL_POINTER", "ERRCODE_FDW_TOO_MANY_HANDLES", "ERRCODE_FDW_OUT_OF_MEMORY", "ERRCODE_FDW_NO_SCHEMAS", "ERRCODE_FDW_OPTION_NAME_NOT_FOUND", "ERRCODE_FDW_REPLY_HANDLE", "ERRCODE_FDW_SCHEMA_NOT_FOUND", "ERRCODE_FDW_TABLE_NOT_FOUND", "ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION", "ERRCODE_FDW_UNABLE_TO_CREATE_REPLY", "ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION", "ERRCODE_PLPGSQL_ERROR", "ERRCODE_RAISE_EXCEPTION", "ERRCODE_NO_DATA_FOUND", "ERRCODE_TOO_MANY_ROWS", "ERRCODE_ASSERT_FAILURE", "ERRCODE_INTERNAL_ERROR", "ERRCODE_DATA_CORRUPTED", "ERRCODE_INDEX_CORRUPTED"};
// Error codes in same order
const int error_codes[] = {3, 0, 64, 318767168, 134217792, 50331712, 117440576, 100663360, 67108928, 16908352, 128, 16777344, 192, 512, 50332160, 100663808, 16777728, 67109376, 117441024, 16908800, 576, 1088, 1152, 1408, 16778624, 1792, 16910080, 2048, 2688, 33557120, 2, 66, 130, 352845954, 17301634, 134217858, 33816706, 83886210, 301990018, 34078850, 84148354, 352583810, 67371138, 100925570, 369361026, 386138242, 134480002, 117440642, 151257218, 335544450, 84410498, 100794498, 262274, 50856066, 302252162, 654573698, 671350914, 403177602, 386400386, 150995074, 318767234, 385876098, 67108994, 33554562, 50331778, 402653314, 101187714, 16777346, 17039490, 117964930, 67633282, 369098882, 16908418, 33685634, 50462850, 67240066, 84017282, 469762178, 486539394, 503316610, 587202690, 603979906, 16777410, 33575106, 50352322, 83906754, 67391682, 16908482, 258, 322, 16777538, 33554754, 134218050, 50331970, 67109186, 83886402, 100663618, 117440834, 16908610, 33685826, 50463042, 386, 450, 514, 16908802, 1154, 16909442, 1282, 1410, 83887490, 33555842, 50333058, 67110274, 259, 515, 16777731, 33554947, 50332163, 67109379, 579, 16777795, 67109443, 16908867, 33686083, 50463299, 1155, 16778371, 1283, 1411, 4, 33554436, 16777220, 50331652, 16908292, 132, 16801924, 16797828, 101744772, 50364548, 655492, 151388292, 819332, 33579140, 34103428, 151818372, 67141764, 134611076, 17432708, 34209924, 151027844, 156008580, 50360452, 52461700, 16908420, 33685636, 67137668, 16806020, 50462852, 67240068, 50884740, 84017284, 100794500, 117571716, 33845380, 290948, 33583236, 84439172, 134348932, 151126148, 393348, 17064068, 17170564, 33947780, 50724996, 67502212, 84279428, 101056644, 117833860, 260, 197, 4293, 8389, 12485, 16581, 261, 16777477, 17039621, 50856197, 325, 100663621, 33685829, 50463045, 453, 67371461, 16908741, 33685957, 50463173, 67240389, 517, 786949, 16908805, 33686021, 135, 22, 16777238, 2456, 83888536, 33556888, 264600, 17303960, 67635608, 117442968, 134220184, 67111320, 100665752, 19138968, 301992344, 318769560, 335546776, 2361752, 285215128, 150997400, 67373464, 16779672, 536873368, 436210072, 452987288, 553650584, 570427800, 469764504, 486541720, 503318936, 32, 16777248, 33554464, 50331680, 67108896, 2600, 16779816, 33557032};
// error_names[0], error_codes[0]
const int not_known_error_code = 3;

// types of messages to save stat
const int message_types_count = 3;
const char message_type_names[][10] = {"WARNING", "ERROR", "FATAL"};
const int message_types_codes[] = {WARNING, ERROR, FATAL};

// counters of messages (depends on message_types_count)
static int total_messages_at_last_interval[3];
static int total_messages_at_buffer[3];

//number of time intervals to save in buffer
const int max_number_of_intervals = 360;

// index of current interval in buffer
static int current_interval_index;

const char *file_name = "/var/log/pg_log_errors";
const char *temp_prefix = "_temp";
const int max_length_of_filename = 200;

typedef struct hashkey {
    int num;
} ErrorCode;

// depends on message_types_count, max_number_of_intervals
typedef struct message_info {
    ErrorCode key;
    pg_atomic_uint32 message_count[3];
    // sum in buffer at previous interval
    int sum_in_buffer[3];
    int intervals[3][120];
    char *name;
} MessageInfo;

static HTAB *messages_info_hashtable = NULL;

static void
pg_log_errors_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}

static void
pg_log_errors_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    if (MyProc)
        SetLatch(&MyProc->procLatch);
    errno = save_errno;
}


static void
pg_log_errors_init()
{
    FILE *file = NULL;
    pg_log_errors_load_params();
    // create file if not exist to prevent error in update step (while deleting file)
    file = fopen(file_name, "w");
    if (file == NULL) {
        ereport(LOG,
                (errcode_for_file_access(),
                        errmsg("could not write file \"%s\": %m",
                               file_name)));
    }

    fclose(file);
    if (!current_interval_index) {
        current_interval_index = 0;
    }
    MemSet(&total_messages_at_last_interval, 0, message_types_count);
    MemSet(&total_messages_at_buffer, 0, message_types_count);
}

static void
pg_log_errors_update_info()
{
    ErrorCode key;
    MessageInfo *info;
    bool found;
    FILE *file = NULL;
    int message_count;

    if (messages_info_hashtable == NULL) {
        return;
    }
    char temp_filename[max_length_of_filename];

    strcpy(temp_filename,file_name);
    strcat(temp_filename,temp_prefix);

    file = fopen(temp_filename, "w");

    if (file == NULL) {
        ereport(LOG,
                (errcode_for_file_access(),
                        errmsg("could not write file \"%s\": %m",
                               file_name)));
        return;
    }
    fprintf(file, "{\n\t\"ERRORS_LIST\": {");
    bool first_time = true;
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
                    info->intervals[j][current_interval_index] +
                    message_count;
            total_messages_at_buffer[j] += info->sum_in_buffer[j];
            total_messages_at_last_interval[j] += message_count;
            info->intervals[j][current_interval_index] = message_count;
            pg_atomic_write_u32(&info->message_count[j], 0);
            if (info->sum_in_buffer[j] > 0) {
                if (!first_time){
                    fprintf(file, ",");
                }
                first_time = false;
                fprintf(file, "\n\t\t\"%s__%s\": %d",
                        message_type_names[j],
                        info->name,
                        info->sum_in_buffer[j]);
            }
        }

    }
    fprintf(file, "\n\t}");
    for (int j = 0; j < message_types_count; ++j)
    {
        fprintf(file, ",\n\t\"%sS\": [{\"interval\": %d, \"count\": %d}, {\"interval\": %d, \"count\": %d}]",
                message_type_names[j],
                interval / 1000 * intervals_count,
                total_messages_at_buffer[j],
                interval / 1000,
                total_messages_at_last_interval[j]
                );
    }
    current_interval_index = (current_interval_index + 1) % intervals_count;

    fprintf(file, "\n}\n");
    fclose(file);

    remove(file_name);
    rename(temp_filename, file_name);
}

void
pg_log_errors_main(Datum main_arg)
{
    /* Register functions for SIGTERM/SIGHUP management */
    pqsignal(SIGHUP, pg_log_errors_sighup);
    pqsignal(SIGTERM, pg_log_errors_sigterm);

    /* We're now ready to receive signals */
    BackgroundWorkerUnblockSignals();

    /* Connect to a database */
#if PG_VERSION_NUM < 110000
    BackgroundWorkerInitializeConnection("postgres", NULL);
#else
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
#endif

    /* Creating table if it does not exist */
    pg_log_errors_init();

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
            ereport(DEBUG1, (errmsg("bgworker pg_log_errors signal: processed SIGHUP")));
            /* Recreate table if needed */
            pg_log_errors_init();
        }

        if (got_sigterm)
        {
            /* Simply exit */
            ereport(DEBUG1, (errmsg("bgworker pg_log_errors signal: processed SIGTERM")));
            proc_exit(0);
        }

        /* Main work happens here */
        pg_log_errors_update_info();
    }

    /* No problems, so clean exit */
    proc_exit(0);
}

// Log hook
void
emit_log_hook_impl(ErrorData *edata)
{
    // only if hashtable already inited
    if (messages_info_hashtable && edata->elevel >= WARNING) {
        for (int j = 0; j < message_types_count; ++j)
        {
            // Only current message type
            if (edata->elevel != message_types_codes[j]) {
                continue;
            }
            MessageInfo *elem;
            ErrorCode key;
            bool found;
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
pg_log_errors_load_params(void)
{
    DefineCustomIntVariable("pg_log_errors.interval",
                            "Time between writing stat to file (ms).",
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
    DefineCustomIntVariable("pg_log_errors.intervals_count",
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
    DefineCustomStringVariable("pg_log_errors.filename",
                               "Name of the output file to write stat",
                               "Default is /var/log/pg_log_errors",
                               &file_name,
                               "/var/log/pg_log_errors",
                               PGC_SIGHUP,
                               0,
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
    sprintf(worker.bgw_library_name, "pg_log_errors");
    sprintf(worker.bgw_function_name, "pg_log_errors_main");
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

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    bool found;
    ErrorCode key;
    MessageInfo *elem;
    HASHCTL ctl;

    messages_info_hashtable = NULL;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(ErrorCode);
    ctl.entrysize = sizeof(MessageInfo);

    messages_info_hashtable = ShmemInitHash("pg_stat_statements hash",
                              error_types_count, error_types_count,
                              &ctl,
                              HASH_ELEM | HASH_BLOBS);


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
