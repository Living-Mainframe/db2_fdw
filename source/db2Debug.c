#include <postgres.h>
#include <miscadmin.h>
#include <string.h>
#include <stdlib.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <utils/guc.h>
#include "db2_fdw.h"

_Thread_local static int debug_depth = 0;

/* get a PostgreSQL error code from an db2error */
#define to_sqlstate(x) \
  (x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
  (x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
  (x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
  (x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
  (x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
  (x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : ERRCODE_FDW_ERROR))))))

/** local prototype */
int  isLogLevel(int level);
void db2Error  (db2error sqlstate, const char* message);
void db2Error_d(db2error sqlstate, const char* message, const char* detail, ...) __attribute__ ((format (gnu_printf, 2, 0)));
void db2Entry  (int level, const char* message, ...)__attribute__ ((format (gnu_printf, 2, 0)));
void db2Exit   (int level, const char* message, ...)__attribute__ ((format (gnu_printf, 2, 0)));
void db2Debug  (int level, const char* message, ...)__attribute__ ((format (gnu_printf, 2, 0)));

/** db2Error_d
 *    Report a PostgreSQL error with a detail message.
 */
void db2Error_d (db2error sqlstate, const char *message, const char *detail, ...) {
  char    cBuffer [4000];
  va_list arg_marker;
  /* if the backend was terminated, report that rather than the DB2 error */
  CHECK_FOR_INTERRUPTS ();
  va_start(arg_marker, detail);
  vsnprintf(cBuffer, sizeof(cBuffer), detail, arg_marker);
  ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message), errdetail ("%s", cBuffer)));
  va_end  (arg_marker);
}

/** db2error
 *   Report a PostgreSQL error without detail message.
 */
void db2Error (db2error sqlstate, const char *message) {
  /* use errcode_for_file_access() if the message contains %m */
  if (strstr(message, "%m")) {
    ereport (ERROR, (errcode_for_file_access (), errmsg (message, "")));
  } else {
    ereport (ERROR, (errcode (to_sqlstate (sqlstate)), errmsg ("%s", message)));
  }
}

int isLogLevel(int level) {
  int dLevel = 0;
  switch(level){
    case 1:
    dLevel = DEBUG1;
    break;
    case 2:
    dLevel = DEBUG2;
    break;
    case 3:
    dLevel = DEBUG3;
    break;
    case 4:
    dLevel = DEBUG4;
    break;
    case 5:
    default:
    dLevel = DEBUG5;
    break;
  }
  dLevel = (dLevel >= log_min_messages);
  return dLevel;
}

void db2Entry(int level, const char* message, ...) {
  if (isLogLevel(level)) {
    char    cBuffer [4000];
    va_list arg_marker;
    va_start (arg_marker, message);

    vsnprintf (cBuffer, sizeof(cBuffer), message, arg_marker);
    db2Debug(level, cBuffer);
    ++debug_depth;
  }
}

void db2Exit(int level, const char* message, ...) {
  if (isLogLevel(level)) {
    char    cBuffer [4000];
    va_list arg_marker;
    va_start (arg_marker, message);
    vsnprintf (cBuffer, sizeof(cBuffer), message, arg_marker);

    --debug_depth;
    db2Debug(level, cBuffer);
  }
}

void db2Debug(int level, const char* message, ...) {
  if (isLogLevel(level)) {
    char    cBuffer [4000];
    char*   sIndent = (char*)palloc0(2*debug_depth+1);
    int     dLevel  = DEBUG5;
    va_list arg_marker;

    memset(sIndent, ' ', 2 * debug_depth);

    va_start (arg_marker, message);
    vsnprintf (cBuffer, sizeof(cBuffer), message, arg_marker);
    switch(level){
      case 1:
      dLevel = DEBUG1;
      break;
      case 2:
      dLevel = DEBUG2;
      break;
      case 3:
      dLevel = DEBUG3;
      break;
      case 4:
      dLevel = DEBUG4;
      break;
      case 5:
      default:
      dLevel = DEBUG5;
      break;
    }
    elog (dLevel, "%s%s", sIndent, cBuffer);
    va_end   (arg_marker);
    pfree(sIndent);
  }
}
