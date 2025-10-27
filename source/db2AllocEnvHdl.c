#include <string.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */
DB2EnvEntry*        rootenvEntry    = NULL;/* Linked list of handles for cached DB2 connections.            */
int                 sql_initialized = 0;   /* set to "1" as soon as SQLAllocHandle(SQL_HANDLE_ENV is called */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2SetHandlers       (void);
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
DB2EnvEntry*     db2AllocEnvHdl       (const char* nls_lang);
void             setDB2Environment    (char* nls_lang);
DB2EnvEntry*     insertenvEntry       (DB2EnvEntry* start, const char* nlslang, SQLHENV henv);

    /** db2AllocEnvHdl
 * 
 */
DB2EnvEntry* db2AllocEnvHdl(const char* nls_lang){
  char*         nlscopy = NULL;
  DB2EnvEntry*  envp    = NULL;
  SQLHENV       henv    = SQL_NULL_HENV;
  SQLRETURN     rc      = 0;

  db2Debug1("> db2AllocEnvHdl");
  /* create persistent copy of "nls_lang" */
  if ((nlscopy = strdup (nls_lang)) == NULL)
    db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", strlen (nls_lang) + 1);

  /* set DB2 environment */
  setDB2Environment (nlscopy);

  /* create environment handle */
  rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
  rc = db2CheckErr(rc, henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    free (nlscopy);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllocHandle failed to create environment handle", db2Message);
  }

  /* we can call db2Shutdown now */
  sql_initialized = 1;
  db2Debug2("  sql_initialized: %d",sql_initialized);

  rc = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
  rc = db2CheckErr(rc, henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    free (nlscopy);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLSetEnvAttr failed to set ODBC v3.0", db2Message);
  }

  /*
   * DB2 overwrites PostgreSQL's signal handlers, so we have to restore them.
   * DB2's SIGINT handler is ok (it cancels the query), but we must do something
   * reasonable for SIGTERM.
   */
  db2SetHandlers ();
  /* add handles to cache */
  envp = insertenvEntry(rootenvEntry, nlscopy, henv);
  if (envp  == NULL) {
    db2Error_d (FDW_OUT_OF_MEMORY, "error connecting to DB2:"," failed to allocate %d bytes of memory", sizeof (DB2EnvEntry));
  }
  if ( rootenvEntry == NULL) {
    rootenvEntry = envp;
  }

  db2Debug1("< db2AllocEnvHdl - returns: %x",envp);
  return envp;
}

/** setDB2Environment
 *   Set environment variables do that DB2 works as we want.
 *
 *   NLS_LANG sets the language and client encoding
 *   NLS_NCHAR is unset so that N* data types are converted to the
 *   character set specified in NLS_LANG.
 *
 *   The following variables are set to values that make DB2 convert
 *   numeric and date/time values to strings PostgreSQL can parse:
 *   NLS_DATE_FORMAT
 *   NLS_TIMESTAMP_FORMAT
 *   NLS_TIMESTAMP_TZ_FORMAT
 *   NLS_NUMERIC_CHARACTERS
 *   NLS_CALENDAR
 */
void setDB2Environment (char* nls_lang) {
  db2Debug1("> setDB2Environment");
  if (putenv (nls_lang) != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_LANG cannot be set.");
  }
  /* other environment variables that control DB2 formats */
  if (putenv ("NLS_DATE_LANGUAGE=AMERICAN") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_LANGUAGE cannot be set.");
  }
  if (putenv ("NLS_DATE_FORMAT=YYYY-MM-DD HH24:MI:SS BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_DATE_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIMESTAMP_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIMESTAMP_TZ_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIME_FORMAT=HH24:MI:SS.FF9 BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_FORMAT cannot be set.");
  }
  if (putenv ("NLS_TIME_TZ_FORMAT= HH24:MI:SS.FF9TZH:TZM BC") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_TIME_TZ_FORMAT cannot be set.");
  }
  if (putenv ("NLS_NUMERIC_CHARACTERS=.,") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NUMERIC_CHARACTERS cannot be set.");
  }
  if (putenv ("NLS_CALENDAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_CALENDAR cannot be set.");
  }
  if (putenv ("NLS_NCHAR=") != 0) {
    free (nls_lang);
    db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2", "Environment variable NLS_NCHAR cannot be set.");
  }
  db2Debug1("< setDB2Environment");
}

/** insertenvEntry
 * 
 */
DB2EnvEntry* insertenvEntry(DB2EnvEntry* start, const char* nlslang, SQLHENV henv) { 
  DB2EnvEntry* step = NULL;
  DB2EnvEntry* new  = NULL;
  db2Debug1("> insertenvEntry");
  if (start == NULL){ /* first entry in list */
    new = malloc(sizeof(DB2EnvEntry));
    new->right = new->left = NULL; 
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    new = malloc(sizeof(DB2EnvEntry));
    step->right = new; 
    new->left = step;
    new->right = NULL;
  }
  new->nls_lang = strdup(nlslang);
  new->henv     = henv;
  new->connlist = NULL;
  db2Debug1("< insertenvEntry - returns: %x", new);
  return new; 
} 
