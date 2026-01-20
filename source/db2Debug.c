#include <postgres.h>
#include <miscadmin.h>
#include <string.h>
#include <stdlib.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/* get a PostgreSQL error code from an db2error */
#define to_sqlstate(x) \
  (x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
  (x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
  (x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
  (x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
  (x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
  (x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : ERRCODE_FDW_ERROR))))))

/** local prototype */
void db2Error  (db2error sqlstate, const char* message);
void db2Error_d(db2error sqlstate, const char* message, const char* detail, ...) __attribute__ ((format (gnu_printf, 2, 0)));
void db2Debug1 (const char* message, ...)__attribute__ ((format (gnu_printf, 1, 0)));
void db2Debug2 (const char* message, ...)__attribute__ ((format (gnu_printf, 1, 0)));
void db2Debug3 (const char* message, ...)__attribute__ ((format (gnu_printf, 1, 0)));
void db2Debug4 (const char* message, ...)__attribute__ ((format (gnu_printf, 1, 0)));
void db2Debug5 (const char* message, ...)__attribute__ ((format (gnu_printf, 1, 0)));

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

/** db2Debug1
 *  Rendering a single DEBUG1 output line to the pg log file.
 */
void db2Debug1(const char* message, ...) {
  char cBuffer [4000];
  va_list arg_marker;
  va_start (arg_marker, message);
  vsnprintf (cBuffer, sizeof(cBuffer),  message, arg_marker);
  elog (DEBUG1, "%s", cBuffer);
  va_end   (arg_marker);
}
/** db2Debug2
 *  Rendering a single DEBUG2 output line to the pg log file.
 */
void db2Debug2(const char* message, ...) {
  char cBuffer [4000];
  va_list arg_marker;
  va_start (arg_marker, message);
  vsnprintf (cBuffer, sizeof(cBuffer),  message, arg_marker);
  elog (DEBUG2, "%s", cBuffer);
  va_end   (arg_marker);
}
/** db2Debug3
 *  Rendering a single DEBUG3 output line to the pg log file.
 */
void db2Debug3(const char* message, ...) {
  char cBuffer [4000];
  va_list arg_marker;
  va_start (arg_marker, message);
  vsnprintf (cBuffer, sizeof(cBuffer),  message, arg_marker);
  elog (DEBUG3, "%s", cBuffer);
  va_end   (arg_marker);
}
/** db2Debug4
 *  Rendering a single DEBUG4 output line to the pg log file.
 */
void db2Debug4(const char* message, ...) {
  char cBuffer [4000];
  va_list arg_marker;
  va_start (arg_marker, message);
  vsnprintf (cBuffer, sizeof(cBuffer),  message, arg_marker);
  elog (DEBUG4, "%s", cBuffer);
  va_end   (arg_marker);
}
/** db2Debug5
 *  Rendering a single DEBUG5 output line to the pg log file.
 */
void db2Debug5(const char* message, ...) {
  char cBuffer [4000];
  va_list arg_marker;
  va_start (arg_marker, message);
  vsnprintf (cBuffer, sizeof(cBuffer),  message, arg_marker);
  elog (DEBUG5, "%s", cBuffer);
  va_end   (arg_marker);
}
