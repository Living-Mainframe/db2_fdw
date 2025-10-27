#ifndef ENVENTRY_H
#define ENVENTRY_H
/** DB2EnvEntry
 *  DB2 environments are managed in a double linked list of DB2EnvEntry elements.
 *  That way the code is able to traverse the linked list back and forth in search of
 *  a specific enviornment. Each connection is identifyable ist nls_lang attribute.
 * 
 *  Attached to a specific connection is a pure forward linked list of Db2ConnEntry elemens
 *  in "handleList". By that the code is able to reuse any active connection handle in that
 *  chain.
 * 
 *  @see    DB2ConnEntry.h for more details on a DB2ConnEntry element of connlist.
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
typedef struct envEntry {
  char*               nls_lang;   // National Language Support language code
  SQLHENV             henv;       // SQL environment handle
  DB2ConnEntry*       connlist;   // double linked list of DB2ConnEntry elements
  struct envEntry*    left;       // preceeding DB environment
  struct envEntry*    right;      // next DB environment
} DB2EnvEntry;

#endif