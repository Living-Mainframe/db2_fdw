#include <string.h>
#include <stdio.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables  */
int                 err_code = 0;          /* error code, set by db2CheckErr()                              */
char                db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external variables */

/** external prototypes */
extern void      db2Debug4            (const char* message, ...);
extern void      db2Debug5            (const char* message, ...);

/** local prototypes */
SQLRETURN db2CheckErr (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** db2CheckErr
 *    Call SQLGetDiagRec to get sqlcode, sqlstate and db2 error message.
 *    It sets the global err_code with a value, so subsequent code can evaluate.
 *    It populates the db2Message with SQLCODE, SQLSTATE and the DB2 message text.
 *    It modifys the result to SQL_SUCCESS in case the status was SQL_SUCCESS_WITH_INFO.
 *    It sets err_code to 100 upon SQL_NO_DATA.
 * 
 *  @param status     the returncode from a previous executed SQL API call
 *  @param handle     the handle used in that previous SQL API call
 *  @param handleType the type of handle used (HENV, HDBC, HSTMT, etc)
 *  @param line       the source-code-line db2CheckErr was invoked from
 *  @param file       the name of the sourcefile db2CheckErr was invoked from
 * 
 *  @return SQLRETURN passing back the status, which in cases is modified
 *  @since  1.0.0
 */
SQLRETURN db2CheckErr (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file) {
  db2Debug4("> db2CheckErr");
  memset (db2Message,0x00,sizeof(db2Message));
  switch (status) {
    case SQL_INVALID_HANDLE: {
      snprintf(db2Message,sizeof(db2Message),"-CI INVALID HANDLE-----\nline=%d\nfile=%s\n",line,file);
      err_code = -1;
    }
    break;
    case SQL_ERROR: {
      SQLCHAR     message    [SQL_MAX_MESSAGE_LENGTH];
      SQLCHAR     submessage [SUBMESSAGE_LEN];
      SQLCHAR     sqlstate   [SQLSTATE_LEN];
      SQLINTEGER  sqlcode;
      SQLSMALLINT msgLen;
      int         i = 1;

      memset(submessage,0x00,SUBMESSAGE_LEN);
      memset(message   ,0x00,SQL_MAX_MESSAGE_LENGTH);
 
      while (SQL_SUCCEEDED(SQLGetDiagRec(handleType,handle,i,sqlstate,&sqlcode,message,SQL_MAX_MESSAGE_LENGTH,&msgLen))) {
        db2Debug5("  SQLCODE :  %d ",sqlcode);
        db2Debug5("  SQLSTATE:  %d ",sqlstate);
        db2Debug5("  MESSAGE : '%s'",message);
        snprintf((char*)submessage, SUBMESSAGE_LEN, "SQLSTATE = %s  SQLCODE = %d\nline=%d\nfile=%s\n", sqlstate,sqlcode,line,file);
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)submessage) + 1) {
          strncat ((char*)db2Message,(char*)submessage, SUBMESSAGE_LEN);
        }
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)message) + 2) {
          strncat ((char*)db2Message,(char*)message,SQL_MAX_MESSAGE_LENGTH);
          strncat ((char*)db2Message,"\n",strlen("\n"));
        }
        if (i == 1) {
          err_code = ((sqlcode == -911 || sqlcode == -913) && strcmp((char*)sqlstate,"40001") == 0) ? 8177 : abs(sqlcode);
        }
        i++;
        memset(submessage,0x00,SUBMESSAGE_LEN);
        memset(message   ,0x00,SQL_MAX_MESSAGE_LENGTH);
      }
    }
    break;
    case SQL_SUCCESS_WITH_INFO: {
      status  = SQL_SUCCESS;
      err_code = 0;
    }
    break;
    case SQL_NO_DATA: {
      strncpy (db2Message, "SQL0100W: no data found", sizeof(db2Message));
      err_code = 100;
    }
    break;
  }
  db2Debug5("  db2Message: '%s'",db2Message);
  db2Debug5("  err_code  :  %d ",err_code);
  db2Debug4("< db2CheckErr - returns: %d",status);
  return status;
}
