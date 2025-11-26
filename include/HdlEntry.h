#ifndef HDLENTRY_H
#define HDLENTRY_H
/** HdlEntry
 *  Primarily HandleEntry keeps SQL statement handles.
 *  That way the code is able to traverse the linked list forth in search of
 *  a statement handle. For historic reasons the element carries a type.
 *  It was formerly used to distinguish statement handles from resource handles.
 *  Hence, resource handles are no longer required by DB2 SQL CLI API.
 * 
 *  @author Ing. Wolfgang Brandl
 *  @since  1.0
 */
/*
 * Linked list for temporary DB2 handless.
 * Stores statement handles as well as timetamp and LOB descriptors.
 * Other handles are stored in the handle cache below.
 */
typedef struct handleEntry
{
  SQLHANDLE           hsql;
  SQLSMALLINT         type;
  struct handleEntry* next;
  SQLCHAR             dummy_buffer[4];   /* Buffer for COUNT(*) queries with no columns */
  SQLLEN              dummy_null;        /* Null indicator for dummy buffer */
} HdlEntry;

#endif