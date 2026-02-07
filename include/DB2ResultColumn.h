#ifndef DB2RESULTCOLUMN_H
#define DB2RESULTCOLUMN_H
/** DB2ResultColumn
 *  A full descriptor of a DB2 table column and its corresponding PG column.
 * 
 *  @author Thomas Muenz
 *  @since  18.2.0
 */
typedef struct db2ResultColumn {
  char*                   colName;       // column name in DB2
  short                   colType;       // column data type in DB2
  size_t                  colSize;       // column size
  short                   colScale;      // column scale of size describing digits right of decimal point
  short                   colNulls;      // column is nullable
  size_t                  colChars;      // numer of characters fit in column size, it is less if UTF8, 16 or DBCS
  size_t                  colBytes;      // number of bytes representing colSize
  int                     colPrimKeyPart;// 1 if column is part of the primary key - only relevant for UPDATE or DELETE
  int                     colCodepage;   // codepage set for this column (only set on char columns), if 0 the content is binary
  char*                   pgname;        // PG column name
  int                     pgattnum;      // PG attribute number
  Oid                     pgtype;        // PG data type
  int                     pgtypmod;      // PG type modifier
  int                     pkey;          // nonzero for primary keys, later set to the resjunk attribute number
  int                     resnum;        // position of result in cursor 1 based
  char*                   val;           // buffer for DB2 to return results in (LOB locator for LOBs)
  size_t                  val_size;      // allocated size in val
  size_t                  val_len;       // actual length of val
  int                     val_null;      // indicator for NULL value
  int                     varno;         // range table index of this column's relation
  db2NoEncErrType         noencerr;      // no encoding error produced
  struct db2ResultColumn* next;
} DB2ResultColumn;

#endif