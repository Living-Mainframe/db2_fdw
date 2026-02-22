#include <postgres.h>
#include <foreign/foreign.h>
#include <nodes/makefuncs.h>
#if PG_VERSION_NUM >= 140000
#include <optimizer/appendinfo.h>
#endif  /* PG_VERSION_NUM */
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <utils/lsyscache.h>
#include "db2_fdw.h"

/** external prototypes */
extern bool            optionIsTrue              (const char* value);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
extern char*           db2strdup                 (const char* source);

/** local prototypes */
#if PG_VERSION_NUM < 140000
void db2AddForeignUpdateTargets(Query* parsetree, RangeTblEntry* target_rte, Relation target_relation);
#else
void db2AddForeignUpdateTargets(PlannerInfo* root, Index rtindex, RangeTblEntry* target_rte, Relation target_relation);
#endif

/** db2AddForeignUpdateTargets
 *     Add the primary key columns as resjunk entries.
 */
#if PG_VERSION_NUM < 140000
void db2AddForeignUpdateTargets (Query* parsetree,RangeTblEntry* target_rte, Relation target_relation){
#else
void db2AddForeignUpdateTargets (PlannerInfo* root, Index rtindex,RangeTblEntry* target_rte, Relation target_relation){
#endif
  Oid       relid   = RelationGetRelid (target_relation);
  TupleDesc tupdesc = target_relation->rd_att;
  int       i       = 0;
  bool      has_key = false;
  db2Debug1("> db2AddForeignUpdateTargets");
  db2Debug2("  add target columns for update on %d - %s", relid, get_rel_name(relid));
  /* loop through all columns of the foreign table */
  for (i = 0; i < tupdesc->natts; ++i) {
    Form_pg_attribute att     = TupleDescAttr (tupdesc, i);
    AttrNumber        attrno  = att->attnum;
    List*             options = NIL;
    ListCell*         option  = NULL;

    if (att->attisdropped)
      continue;

    /* look for the "key" option on this column */
    options = GetForeignColumnOptions (relid, attrno);
    foreach (option, options) {
      DefElem* def = (DefElem*) lfirst (option);
      /* if "key" is set, add a resjunk for this column */
      if (strcmp (def->defname, OPT_KEY) == 0) {
        if (optionIsTrue (STRVAL(def->arg))) {
          Var*  var           = NULL;
          char* key_col_name  = db2strdup(psprintf("__db2fdw_rowid_%s", NameStr(att->attname)));

          #if PG_VERSION_NUM < 140000
          TargetEntry *tle;
          /* Make a Var representing the desired value */
          var = makeVar(
            parsetree->resultRelation,
            attrno,
            att->atttypid,
            att->atttypmod,
            att->attcollation,
            0);
          /* Wrap it in a resjunk TLE with the right name ... */
          tle = makeTargetEntry( (Expr*)var
                               , list_length(parsetree->targetList) + 1
                               , key_col_name
                               , true
                              );
          /* ... and add it to the query's targetlist */
          parsetree->targetList = lappend(parsetree->targetList, tle);
          #else
          /* Build a Var referencing the PK column of the target RTE */
          var = makeVar( rtindex
                       , attrno
                       , att->atttypid
                       , att->atttypmod
                       , att->attcollation
                       , 0
                       );
          db2Debug2("  create var rtindex: %d, attrno: %d, typid: %d, typmod: %d, collation: %d",rtindex,attrno,att->atttypid,att->atttypmod,att->attcollation);
          /* Register it as a required row-identity column.
           * The name becomes the resjunk column name in the plan.
           */
          add_row_identity_var(root, var, rtindex, key_col_name);
          db2Debug2("  add resjunk column %s: %d", key_col_name, rtindex);
          #endif  /* PG_VERSION_NUM */
          has_key = true;
        }
      }
    }
  }
  if (!has_key) {
    ereport ( ERROR
            , ( errcode   (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
              , errmsg    ("no primary key column specified for foreign DB2 table")
              , errdetail ("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column.")
              , errhint   ("Set the option \"%s\" on the columns that belong to the primary key.", OPT_KEY)
              )
            );
  }
  db2Debug1("< db2AddForeignUpdateTargets");
}
