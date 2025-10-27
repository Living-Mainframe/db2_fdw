#ifndef DB2FDWSTATE_H
#define DB2FDWSTATE_H

/** DB2FdwState
 *  FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 *  The same structure is used to hold information for query planning and execution.
 *  The structure is initialized during query planning and passed on to the execution
 *  step serialized as a List (see serializePlanData and deserializePlanData).
 *  For DML statements, the scan stage and the modify stage both hold an
 *  DB2FdwState, and the latter is initialized by copying the former (see copyPlanData).
 * 
 *  @author Ing Wolfgang Brandl
 *  @since  1.0
 */
typedef struct db2FdwState {
  char*               dbserver;      // DB2 connect string
  char*               user;          // DB2 username
  char*               password;      // DB2 password
  char*               nls_lang;      // DB2 locale information
  DB2Session*         session;       // encapsulates the active DB2 session
  char*               query;         // query we issue against DB2
  List*               params;        // list of parameters needed for the query
  ParamDesc*          paramList;     // description of parameters needed for the query
  DB2Table*           db2Table;      // description of the remote DB2 table
  Cost                startup_cost;  // cost estimate, only needed for planning
  Cost                total_cost;    // cost estimate, only needed for planning
  unsigned long       rowcount;      // rows already read from DB2
  int                 columnindex;   // currently processed column for error context
  MemoryContext       temp_cxt;      // short-lived memory for data modification
  unsigned int        prefetch;      // number of rows to prefetch
  char*               order_clause;  // for sort-pushdown
  char*               where_clause;  // deparsed where clause
  /*
   * Restriction clauses, divided into safe and unsafe to pushdown subsets.
   *
   * For a base foreign relation this is a list of clauses along-with
   * RestrictInfo wrapper. Keeping RestrictInfo wrapper helps while dividing
   * scan_clauses in db2GetForeignPlan into safe and unsafe subsets.
   * Also it helps in estimating costs since RestrictInfo caches the
   * selectivity and qual cost for the clause in it.
   *
   * For a join relation, however, they are part of otherclause list
   * obtained from extract_actual_join_clauses, which strips RestrictInfo
   * construct. So, for a join relation they are list of bare clauses.
   */
  List*               remote_conds;
  List*               local_conds;
  /* Join information */
  RelOptInfo*         outerrel;
  RelOptInfo*         innerrel;
  JoinType            jointype;
  List*               joinclauses;
} DB2FdwState;
#endif