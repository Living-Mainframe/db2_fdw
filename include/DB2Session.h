#ifndef DB2SESSION_H
#define DB2SESSION_H

/** DB2Session
 *  An active database session contains the db environment handle, the connection handle and a statement handle.
 *  It is used to be passed around the functions which cannot include DB2 SQL CLI API.
 * 
 *  NOTE: the typedef struct db2Session DB2Session definition is outside of the block included when sqlcli1.h is
 *        included so that the other code at least has that type definition to declare pointers to that type.
 *        Check db2_fdw.h for details.
 * 
 *  @see    DB2EnvEntry.h for more details on a DB2EnvEntry element
 *  @see    DB2ConnEntry.h for more details on a DB2ConnEntry element
 *  @see    HdlEntry.h for more details on a HdlEntry element
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
/*
 * Represents one DB2 connection, points to cached entries.
 * This is necessary to be able to pass them back to
 * db2_fdw.c without having to #include sqlcli1.h there.
 */
struct db2Session {
  DB2EnvEntry*        envp;
  DB2ConnEntry*       connp;
  HdlEntry*           stmtp;
};
#endif