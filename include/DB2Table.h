#ifndef DB2TABLE_H
#define DB2TABLE_H

/** DB2Table
 *  A full table descriptor of a DB2 table and its columns.
 * 
 *  @see    DB2Column.h for more details on a DB2Column descriptor
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
typedef struct db2Table {
  char*               name;          // DB2 table name
  char*               pgname;        // PG tabelname, for error messages
  int                 batchsz;       // Defined size of batch inserts
  int                 ncols;         // number of columns in DB2 table
  int                 npgcols;       // number of columns (including dropped) in the PostgreSQL foreign table
  DB2Column**         cols;          // pointer to an array of DB2Column descriptors, as many as ncols tells
} DB2Table;
#endif
