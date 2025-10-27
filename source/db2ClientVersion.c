#include <string.h>
#include <stdio.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
void             db2ClientVersion     (DB2Session* session, char* version);

/** db2ClientVersion
 *   Returns the five components of the client version.
 */
void db2ClientVersion (DB2Session* session, char* version) {
  SQLSMALLINT len = 0;
  size_t      ver_len = sizeof(version);
  SQLRETURN   rc  = 0;
  db2Debug1("> db2ClientVersion");
  memset(version,0x00,ver_len);
  rc = SQLGetInfo(session->connp->hdbc, SQL_DRIVER_VER, version, sizeof(version), &len);
  db2Debug2("  rc = %d, version = '%s', ind = %d", rc, version, len);
  rc = db2CheckErr(rc,session->connp->hdbc,SQL_HANDLE_DBC,__LINE__,__FILE__);
  db2Debug1("< db2ClientVersion - version: '%s'", version);
}
