#include <postgres.h>
#include <foreign/foreign.h>
#include <utils/lsyscache.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#endif
//#include "db2_pg.h"
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2FdwState.h"

/** external prototypes */
extern char*        guessNlsLang              (char* nls_lang);
extern void         db2GetOptions             (Oid foreigntableid, List** options);
extern DB2Session*  db2GetSession             (const char* connectstring, char* user, char* password, const char* nls_lang, int curlevel);
extern DB2Table*    db2Describe               (DB2Session* session, char* schema, char* table, char* pgname, long max_long, char* noencerr);
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
DB2FdwState* db2GetFdwState(Oid foreigntableid, double* sample_percent);
void         getColumnData (DB2Table* db2Table, Oid foreigntableid);
#ifndef OLD_FDW_API
bool         optionIsTrue  (const char* value);
#endif

/** db2GetFdwState
 *   Construct an DB2FdwState from the options of the foreign table.
 *   Establish an DB2 connection and get a description of the
 *   remote table.
 *   "sample_percent" is set from the foreign table options.
 *   "sample_percent" can be NULL, in that case it is not set.
 */
DB2FdwState* db2GetFdwState (Oid foreigntableid, double *sample_percent) {
  DB2FdwState* fdwState = palloc0 (sizeof (DB2FdwState));
  char *pgtablename = get_rel_name (foreigntableid);
  List *options;
  ListCell *cell;
  char *schema = NULL, *table = NULL, *maxlong = NULL, *sample = NULL, *fetch = NULL, *noencerr = NULL;
  long max_long;

  db2Debug1("> db2GetFdwState");
  /*
   * Get all relevant options from the foreign table, the user mapping,
   * the foreign server and the foreign data wrapper.
   */
  db2GetOptions (foreigntableid, &options);
  foreach (cell, options) {
    DefElem *def = (DefElem *) lfirst (cell);
    if (strcmp (def->defname, OPT_NLS_LANG) == 0)
      fdwState->nls_lang = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_DBSERVER) == 0)
      fdwState->dbserver = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_USER) == 0)
      fdwState->user = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PASSWORD) == 0)
      fdwState->password = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_SCHEMA) == 0)
      schema = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_TABLE) == 0)
      table = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_MAX_LONG) == 0)
      maxlong = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_SAMPLE) == 0)
      sample = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_PREFETCH) == 0)
      fetch = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0)
      noencerr = STRVAL(def->arg);
  }

  /* convert "max_long" option to number or use default */
  if (maxlong == NULL)
    max_long = DEFAULT_MAX_LONG;
  else
    max_long = strtol (maxlong, NULL, 0);

  /* convert "sample_percent" to double */
  if (sample_percent != NULL) {
    if (sample == NULL)
      *sample_percent = 100.0;
    else
      *sample_percent = strtod (sample, NULL);
  }

  /* convert "prefetch" to number (or use default) */
  if (fetch == NULL)
    fdwState->prefetch = DEFAULT_PREFETCH;
  else
    fdwState->prefetch = (unsigned int) strtoul (fetch, NULL, 0);

  /* check if options are ok */
  if (table == NULL)
    ereport (ERROR, (errcode (ERRCODE_FDW_OPTION_NAME_NOT_FOUND), errmsg ("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));

  /* guess a good NLS_LANG environment setting */
  fdwState->nls_lang = guessNlsLang (fdwState->nls_lang);

  /* connect to DB2 database */
  fdwState->session = db2GetSession (fdwState->dbserver, fdwState->user, fdwState->password, fdwState->nls_lang, GetCurrentTransactionNestLevel () );

  /* get remote table description */
  fdwState->db2Table = db2Describe (fdwState->session, schema, table, pgtablename, max_long, noencerr);

  /* add PostgreSQL data to table description */
  getColumnData (fdwState->db2Table, foreigntableid);

  db2Debug1("< db2GetFdwState");
  return fdwState;
}

/** getColumnData
 *   Get PostgreSQL column name and number, data type and data type modifier.
 *   Set db2Table->npgcols.
 *   For PostgreSQL 9.2 and better, find the primary key columns and mark them in db2Table.
 */
void getColumnData (DB2Table* db2Table, Oid foreigntableid) {
  Relation rel;
  TupleDesc tupdesc;
  int i, index;

  db2Debug1("> getColumnData");
  rel = table_open (foreigntableid, NoLock);
  tupdesc = rel->rd_att;

  /* number of PostgreSQL columns */
  db2Table->npgcols = tupdesc->natts;

  /* loop through foreign table columns */
  index = 0;
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att_tuple = TupleDescAttr (tupdesc, i);
    List*             options;
    ListCell*         option;

    /* ignore dropped columns */
    if (att_tuple->attisdropped)
      continue;

    ++index;
    /* get PostgreSQL column number and type */
    if (index <= db2Table->ncols) {
      db2Table->cols[index - 1]->pgattnum = att_tuple->attnum;
      db2Table->cols[index - 1]->pgtype   = att_tuple->atttypid;
      db2Table->cols[index - 1]->pgtypmod = att_tuple->atttypmod;
      db2Table->cols[index - 1]->pgname   = pstrdup (NameStr (att_tuple->attname));
    }

    /* loop through column options */
    options = GetForeignColumnOptions (foreigntableid, att_tuple->attnum);
    foreach (option, options) {
      DefElem* def = (DefElem*) lfirst (option);
      /* is it the "key" option and is it set to "true" ? */
      if (strcmp (def->defname, OPT_KEY) == 0 && optionIsTrue ((STRVAL(def->arg)))) {
        /* mark the column as primary key column */
        db2Table->cols[index - 1]->pkey           = 1;
      }
      /* is it the "no_encoding_error" option set */
      if (strcmp (def->defname, OPT_NO_ENCODING_ERROR) == 0) {
        db2Table->cols[index - 1]->noencerr = optionIsTrue((STRVAL(def->arg))) ? NO_ENC_ERR_TRUE : NO_ENC_ERR_FALSE; 
      }
    }
  }

  table_close (rel, NoLock);
  db2Debug1("< getColumnData");
}

#ifndef OLD_FDW_API
/** optionIsTrue
 *   Returns true if the string is "true", "on" or "yes".
 */
bool optionIsTrue (const char *value) {
  bool result = false;
  db2Debug1("> optionIsTrue(value: '%s')",value);
  result = (pg_strcasecmp (value, "on") == 0 || pg_strcasecmp (value, "yes") == 0 || pg_strcasecmp (value, "true") == 0);
  db2Debug1("< optionIsTrue - returns: '%s'",((result) ? "true" : "false"));
  return result;
}
#endif /* OLD_FDW_API */

