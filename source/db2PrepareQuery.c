#include <string.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern void         db2Debug3            (const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error             (db2error sqlstate, const char* message);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern HdlEntry*    db2AllocStmtHdl      (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);
extern SQLSMALLINT  c2param              (SQLSMALLINT fparamType);
extern char*        param2name           (SQLSMALLINT fparamType);

/** internal prototypes */
void                db2PrepareQuery      (DB2Session* session, const char *query, DB2Table* db2Table, unsigned int prefetch);

/** db2PrepareQuery
 *   Prepares an SQL statement for execution.
 *   This function should handle everything that has to be done only once
 *   even if the statement is executed multiple times, that is:
 *   - For SELECT statements, defines the result values to be stored in db2Table.
 *   - For DML statements, allocates LOB locators for the RETURNING clause in db2Table.
 *   - Set the prefetch options.
 */
void db2PrepareQuery (DB2Session* session, const char *query, DB2Table* db2Table, unsigned int prefetch) {
  int        i          = 0;
  int        col_pos    = 0;
  int        is_select  = 0;
  int        for_update = 0;
  SQLRETURN  rc         = 0;

  db2Debug1("> db2PrepareQuery");
  db2Debug2("  query   : '%s'",query);
  db2Debug2("  prefetch: %d  ",prefetch);
  /* figure out if the query is FOR UPDATE */
  is_select  = (strncmp (query, "SELECT", 6) == 0);
  for_update = (strstr (query, "FOR UPDATE") != NULL);

  /* make sure there is no statement handle stored in "session" */
  if (session->stmtp != NULL) {
    db2Error(FDW_ERROR, "db2PrepareQuery internal error: statement handle is not NULL");
  }

  /* create statement handle */
  session->stmtp = db2AllocStmtHdl(SQL_HANDLE_STMT, session->connp, FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: failed to allocate statement handle");
  db2Debug2("  session->stmtp->hsql: %d",session->stmtp->hsql);
  /* set prefetch options */
  if (is_select) {
    SQLULEN prefetch_rows = prefetch;
    db2Debug3("  IS_SELECT");
    if (for_update) {
      db2Debug3("  FOR UPDATE");
      // Make the cursor sensitive scrollable (e.g., static) so PREFETCH_NROWS applies
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_DYNAMIC, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor dynamic", db2Message);
      }
      db2Debug3("  set cursor dynamic");
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_LOCK, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor pessemistic", db2Message);
      }
      db2Debug3("  set cursor pessemistic");
    } else {
      // Make the cursor insensitive scrollable (e.g., static) so PREFETCH_NROWS applies
      rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, 0);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to make cursor scrollable", db2Message);
      }
      db2Debug3("  set cursor static");
    }
    // Prefetch rows per block for scrollable (non-dynamic) cursors
    rc = SQLSetStmtAttr(session->stmtp->hsql, SQL_ATTR_PREFETCH_NROWS, (SQLPOINTER)prefetch_rows, 0);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d (FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLSetStmtAttr failed to set number of prefetched rows in statement handle", db2Message);
    }
    db2Debug2("  set cursor prefetch: %d",prefetch_rows);
  }

  /* prepare the statement */
  db2Debug2("  query to prepare: '%s'",query);
  rc = SQLPrepare(session->stmtp->hsql, (SQLCHAR*)query, SQL_NTS);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLPrepare failed to prepare remote query", db2Message);
  }

  /* loop through table columns */
  col_pos = 0;
  for (i = 0; i < db2Table->ncols; ++i) {
    if (db2Table->cols[i]->used) {
      SQLSMALLINT fparamType = c2param((SQLSMALLINT)db2Table->cols[i]->colType);
      /*
       * Unfortunately DB2 handles DML statements with a RETURNING clause
       * quite different from SELECT statements.  In the latter, the result
       * columns are "defined", i.e. bound to some storage space.
       * This definition is only necessary once, even if the query is executed
       * multiple times, so we do this here.
       * RETURNING clause are handled in db2ExecuteQuery, here we only
       * allocate locators for LOB columns in RETURNING clauses.
       */
      /* figure out in which format we want the results */
      if (db2Table->cols[i]->pgtype == UUIDOID) {
        fparamType = SQL_C_CHAR;
      }
      db2Debug2("  db2Table->cols[%d]->colName       : '%s' ",i,db2Table->cols[i]->colName);
      db2Debug2("  db2Table->cols[%d]->colSize       : '%ld'",i,db2Table->cols[i]->colSize);
      db2Debug2("  db2Table->cols[%d]->colScale      : '%d' ",i,db2Table->cols[i]->colScale);
      db2Debug2("  db2Table->cols[%d]->colNulls      : '%d' ",i,db2Table->cols[i]->colNulls);
      db2Debug2("  db2Table->cols[%d]->colChars      : '%ld'",i,db2Table->cols[i]->colChars);
      db2Debug2("  db2Table->cols[%d]->colBytes      : '%ld'",i,db2Table->cols[i]->colBytes);
      db2Debug2("  db2Table->cols[%d]->colPrimKeyPart: '%d' ",i,db2Table->cols[i]->colPrimKeyPart);
      db2Debug2("  db2Table->cols[%d]->colCodepage   : '%d' ",i,db2Table->cols[i]->colCodepage);
      db2Debug2("  db2Table->cols[%d]->val           : '%x'" ,i,db2Table->cols[i]->val);
      db2Debug2("  db2Table->cols[%d]->val_size      : '%ld'",i,db2Table->cols[i]->val_size);
      db2Debug2("  db2Table->cols[%d]->val_len       : '%d' ",i,db2Table->cols[i]->val_len);
      db2Debug2("  db2Table->cols[%d]->val_null      : '%d' ",i,db2Table->cols[i]->val_null);
      db2Debug2("  fparamType: %d (%s)",fparamType,param2name(fparamType));
      ++col_pos;
      db2Debug2("  SQLBindCol(%d,%d,%d(%s),%x,%ld,%x)",session->stmtp->hsql,col_pos, fparamType, param2name(fparamType), db2Table->cols[i]->val, db2Table->cols[i]->val_size, &db2Table->cols[i]->val_null);
      rc = SQLBindCol (session->stmtp->hsql,col_pos, fparamType, db2Table->cols[i]->val, db2Table->cols[i]->val_size, &db2Table->cols[i]->val_null);
      rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
      if (rc != SQL_SUCCESS) {
        db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindCol failed to define result value", db2Message);
      }
    }
  }
  if (is_select && col_pos == 0) {
    SQLCHAR    dummy[4];
    SQLLEN     dummy_null;
    /*
     * No columns selected (i.e., SELECT '1' FROM).
     * Define dummy result columnn.
     */
    rc = SQLBindCol(session->stmtp->hsql, 1, SQL_C_CHAR, dummy, sizeof(dummy), &dummy_null);
    rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindCol failed to define result value", db2Message);
    }
  }

  db2Debug1("< db2PrepareQuery");
}
