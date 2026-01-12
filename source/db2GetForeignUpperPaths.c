#include <postgres.h>
#include <foreign/fdwapi.h>
#include <nodes/pathnodes.h>
#include <optimizer/paths.h>

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);

void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra) {
    db2Debug1("> %s::db2GetForeignUpperPaths",__FILE__);
    if (stage == UPPERREL_GROUP_AGG) {
    /* input_rel is the scan/join rel; output_rel is the aggregate rel */

    /* 1) reject if grouping sets / DISTINCT aggregates / ordered-set etc, unless you support them */
//    if (!db2_groupagg_supported(root, output_rel))
//        return;

    /* 2) ensure all target expressions and having quals are shippable */
//    if (!db2_upper_targets_shippable(root, output_rel) || !db2_having_shippable(root, output_rel))
//        return;

    /* 3) create a ForeignPath that performs aggregation remotely */
//    Path* path = (Path*) create_foreign_upper_path(
//        root,
//        output_rel,
//        /* fdw_private */ db2_build_upper_fdw_private(root, input_rel, output_rel),
//        /* rows */ output_rel->rows,
//        /* startup_cost */ 10000.0,
//        /* total_cost */ 10000.0 + output_rel->rows * 10.0,
//        /* pathkeys */ NIL,
//        /* required_outer */ NULL,
//        /* fdw_outerpath */ NULL
//    );

//    add_path(output_rel, path);
    }
    db2Debug1("< %s::db2GetForeignUpperPaths",__FILE__);
}
