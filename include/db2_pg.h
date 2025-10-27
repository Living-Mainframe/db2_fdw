#ifndef _db2_pg_h_
#define _db2_pg_h_

//#include <postgres.h>
//#include "fmgr.h"
//#include "access/htup_details.h"
//#include "access/reloptions.h"
//#include "access/sysattr.h"
//#include "access/xact.h"
//#include "catalog/indexing.h"
//#include "catalog/pg_attribute.h"
//#include "catalog/pg_cast.h"
//#include "catalog/pg_collation.h"
//#include "catalog/pg_foreign_data_wrapper.h"
//#include "catalog/pg_foreign_server.h"
//#include "catalog/pg_foreign_table.h"
//#include "catalog/pg_namespace.h"
//#include "catalog/pg_operator.h"
//#include "catalog/pg_proc.h"
//#include "catalog/pg_user_mapping.h"
//#include "catalog/pg_type.h"
//#include "commands/defrem.h"
//#include "commands/explain.h"
//#include "commands/vacuum.h"
//#include "foreign/fdwapi.h"
//#include "foreign/foreign.h"

//#if PG_VERSION_NUM < 100000
//#include "libpq/md5.h"
//#else
//#include "common/md5.h"
//#endif /* PG_VERSION_NUM */

//#include "libpq/pqsignal.h"
//#include "mb/pg_wchar.h"
//#include "miscadmin.h"
//#include "nodes/makefuncs.h"
//#include "nodes/nodeFuncs.h"
//#include "nodes/pg_list.h"
//#include "optimizer/cost.h"

//#if PG_VERSION_NUM >= 140000
//#include "optimizer/appendinfo.h"
//#endif  /* PG_VERSION_NUM */

//#include "optimizer/pathnode.h"

//#if PG_VERSION_NUM >= 130000
//#include "optimizer/paths.h"
//#endif  /* PG_VERSION_NUM */

//#include "optimizer/planmain.h"
//#include "optimizer/restrictinfo.h"
//#include "optimizer/tlist.h"
//#include "parser/parse_relation.h"
//#include "parser/parsetree.h"
//#include "port.h"

//#include "storage/ipc.h"
//#include "storage/lock.h"
//#include "tcop/tcopprot.h"
//#include "utils/array.h"
//#include "utils/builtins.h"
//#include "utils/catcache.h"
//#include "utils/date.h"
//#include "utils/datetime.h"
//#include "utils/elog.h"
//#include "utils/fmgroids.h"
//#include "utils/formatting.h"
//#include "utils/guc.h"
//#include "utils/lsyscache.h"
//#include "utils/memutils.h"
//#include "utils/rel.h"
//#include "utils/resowner.h"
//#include "utils/timestamp.h"
//#include "utils/snapmgr.h"
//#include "utils/syscache.h"

//#if PG_VERSION_NUM < 120000
//#include "nodes/relation.h"
//#include "optimizer/var.h"
//#include "utils/tqual.h"
//#else
//#include "nodes/pathnodes.h"
//#include "optimizer/optimizer.h"
//#include "access/heapam.h"
//#endif
//#if PG_VERSION_NUM >= 150000
//#include <datatype/timestamp.h>
//#endif

//#if PG_VERSION_NUM >= 150000
//#define STRVAL(arg) ((String *)(arg))->sval
//#else
//#define STRVAL(arg) ((Value*)(arg))->val.str
//#endif
//
///* defined in backend/commands/analyze.c */
//#ifndef WIDTH_THRESHOLD
//#define WIDTH_THRESHOLD 1024
//#endif /* WIDTH_THRESHOLD */
//
//#undef  OLD_FDW_API
//#define WRITE_API
//#define IMPORT_API
//
///* array_create_iterator has a new signature from 9.5 on */
//#define array_create_iterator(arr, slice_ndim) array_create_iterator(arr, slice_ndim, NULL)
//#define JOIN_API
//
///* the useful macro IS_SIMPLE_REL is defined in v10, backport */
//#ifndef IS_SIMPLE_REL
#define IS_SIMPLE_REL(rel) \
  ((rel)->reloptkind == RELOPT_BASEREL || \
  (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
//#endif
//
///* GetConfigOptionByName has a new signature from 9.6 on */
//#define GetConfigOptionByName(name, varname) GetConfigOptionByName(name, varname, false)
//
//#if PG_VERSION_NUM < 110000
///* backport macro from V11 */
//#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
//#endif /* PG_VERSION_NUM */
//
///* list API has changed in v13 */
//#if PG_VERSION_NUM < 130000
//#define list_next(l, e) lnext((e))
//#define do_each_cell(cell, list, element) for_each_cell(cell, (element))
//#else
//#define list_next(l, e) lnext((l), (e))
//#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))
//#endif  /* PG_VERSION_NUM */
//
///* older versions don't have JSONOID */
//#ifndef JSONOID
//#define JSONOID InvalidOid
//#endif
//
///* "table_open" was "heap_open" before v12 */
//#if PG_VERSION_NUM < 120000
//#define table_open(x, y) heap_open(x, y)
//#define table_close(x, y) heap_close(x, y)
//#endif  /* PG_VERSION_NUM */

#endif