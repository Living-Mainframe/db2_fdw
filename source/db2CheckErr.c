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
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);

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
  db2Debug1("> db2CheckErr");
  memset (db2Message,0x00,sizeof(db2Message));
  switch (status) {
    case SQL_INVALID_HANDLE: {
      sprintf(db2Message,"-CI INVALID HANDLE-----\nline=%d\nfile=%s\n",line,file);
      err_code = -1;
    }
    break;
    case SQL_ERROR: {
      SQLCHAR     message    [SQL_MAX_MESSAGE_LENGTH];
      SQLCHAR     submessage [200];
      SQLCHAR     sqlstate   [6];
      SQLINTEGER  sqlcode;
      SQLSMALLINT msgLen;
      int         i = 1;

      memset(submessage,0x00,sizeof(submessage));
      memset(message   ,0x00,sizeof(message));
 
      while (SQL_SUCCEEDED(SQLGetDiagRec(handleType,handle,i,sqlstate,&sqlcode,message,sizeof(message),&msgLen))) {
        db2Debug2("  SQLCODE :  %d ",sqlcode);
        db2Debug2("  SQLSTATE:  %d ",sqlstate);
        db2Debug2("  MESSAGE : '%s'",message);
        sprintf((char*)submessage,"SQLSTATE = %s  SQLCODE = %d\nline=%d\nfile=%s\n", sqlstate,sqlcode,line,file);
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)submessage) + 1) {
          strcat ((char*)db2Message,(char*)submessage);
        }
        if ((sizeof(db2Message) - strlen((char*)db2Message)) > strlen((char*)message) + 2) {
          strcat ((char*)db2Message,(char*)message);
          strcat ((char*)db2Message,"\n");
        }
        if (i == 1) {
          err_code = ((sqlcode == -911 || sqlcode == -913) && strcmp((char*)sqlstate,"40001") == 0) ? 8177 : abs(sqlcode);
        }
        i++;
        memset(submessage,0x00,sizeof(submessage));
        memset(message   ,0x00,sizeof(message));
      }
    }
    break;
    case SQL_SUCCESS_WITH_INFO: {
      status  = SQL_SUCCESS;
      err_code = 0;
    }
    break;
    case SQL_NO_DATA: {
      strcpy (db2Message, "SQL0100W: no data found");
      err_code = 100;
    }
    break;
  }
  db2Debug2("  db2Message: '%s'",db2Message);
  db2Debug2("  err_code  :  %d ",err_code);
  db2Debug1("< db2CheckErr - returns: %d",status);
  return status;
}
