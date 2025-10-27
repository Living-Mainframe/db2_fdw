#include <string.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void*        db2Alloc             (size_t size);
extern void*        db2Realloc           (void* p, size_t size);
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);

/** internal prototypes */
void                db2GetLob            (DB2Session* session, DB2Column* column, int cidx, char** value, long* value_len, unsigned long trunc);

/** db2GetLob
 *   Get the LOB contents and store them in *value and *value_len.
 *   If "trunc" is nonzero, it contains the number of bytes or characters to get.
 */
void db2GetLob (DB2Session* session, DB2Column* column, int cidx, char** value, long* value_len, unsigned long trunc) {
  SQLRETURN      rc  = SQL_SUCCESS;
  SQLLEN         ind = 0;
  SQLCHAR        buf[LOB_CHUNK_SIZE+1];
  int            extend = 0;
  db2Debug1("> db2GetLob");
  db2Debug2("  column->colName: '%s'",column->colName);
  db2Debug2("  cidx           :  %d ",cidx);
  /* initialize result buffer length */
  *value_len = 0;
  /* read the LOB in chunks */
  do {
    db2Debug2("  value_len: %ld",*value_len);
    db2Debug2("  reading %d byte chunck of data",sizeof(buf));
    rc = SQLGetData(session->stmtp->hsql, cidx, SQL_C_CHAR, buf, sizeof(buf), &ind);
    rc = db2CheckErr(rc,session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc == SQL_ERROR) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLGetData failed to read LOB chunk", db2Message);
    }
    switch(ind) {
      case SQL_NULL_DATA:
      break;
      case SQL_NO_TOTAL:
        extend = LOB_CHUNK_SIZE;
      break;
      default:
        extend = ind;
      break;
    }
    /* extend result buffer by ind */
    if (*value_len == 0) {
      *value = db2Alloc (*value_len + extend + 1);
    } else {
      // do not add another 0 termination byte, since we already have one
      *value = db2Realloc (*value, *value_len + extend);
    }
    // append the buffer read to the value excluding 0 termination byte
    memcpy(*value + *value_len, buf, extend);
    /* update LOB length */
    *value_len += ind;
  } while (rc == SQL_SUCCESS_WITH_INFO);

  /* string end for CLOBs */
  (*value)[*value_len] = '\0';
  db2Debug2("  value_len    : %ld", *value_len);
  db2Debug2("  strlen of lob: %ld", strlen(*value));
  db2Debug1("< db2GetLob");
}
