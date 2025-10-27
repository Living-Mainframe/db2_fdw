#ifndef CONNENTRY_H
#define CONNENTRY_H
/** DB2ConnEntry
 *  DB2 connections are managed in a double linked list of DB2ConnEntry elements.
 *  That way the code is able to traverse the linked list back and forth in search of
 *  a specific connection. Each connection is identifyable by servername, userid.
 *  It is not recommend to identify by password.
 * 
 *  Attached to a specific connection is a pure forward linked list of HdlEntry elemens
 *  in "handleList". By that the code is able to reuse any active statment handle in that
 *  chain.
 * 
 *  @see    HdlList.h for more details on a HdlEntry element of handleList.
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
typedef struct connEntry {
  char*               srvname;    // Server Name or IP Address
  char*               uid;        // Userid
  char*               pwd;        // Password
  SQLHDBC             hdbc;       // SQL DB connect handle
  ULONG               conAttr;    // connection attributes
  HdlEntry*           handlelist; // linked list of statement handles
  int                 xact_level; // transaction level 0 = none, 1 = main, else subtransaction 
  struct connEntry*   left;       // preceeding connection
  struct connEntry*   right;      // following connection
} DB2ConnEntry;
#endif