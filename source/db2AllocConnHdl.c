#include <string.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** external variables */
extern char      db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);
extern void      db2Debug3            (const char* message, ...);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern void      db2RegisterCallback  (void* arg);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void      db2FreeEnvHdl        (DB2EnvEntry* envp, const char* nls_lang);

/** local prototypes */
DB2ConnEntry*    db2AllocConnHdl      (DB2EnvEntry* envp,const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang);
DB2ConnEntry*    findconnEntry        (DB2ConnEntry* start, const char* srvname, const char* user);
DB2ConnEntry*    insertconnEntry      (DB2ConnEntry* start, const char* srvname, const char* uid, const char* pwd, const char* jwt_token, SQLHDBC hdbc);

/** db2AllocConnHdl
 *
 */
DB2ConnEntry* db2AllocConnHdl(DB2EnvEntry* envp,const char* srvname, char* user, char* password, char* jwt_token, const char* nls_lang) {
  DB2ConnEntry* connp   = NULL;
  SQLRETURN     rc      = 0;
  SQLHDBC       hdbc    = SQL_NULL_HDBC;

  db2Debug1("> db2AllocConnHdl(envp: %x, srvname: %s, user: %s, password: %s, jwt_token: %s, nls_lang: %s)", envp, srvname,user, password, jwt_token ? "***" : "NULL", nls_lang);
  if (nls_lang != NULL) {
    rc = SQLAllocHandle(SQL_HANDLE_DBC, envp->henv, &hdbc);
    db2Debug3("  alloc dbc handle - rc: %d, henv: %d, hdbc: %d",rc, envp->henv, hdbc);
    rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllocHandle failed to allocate hdbc handle", db2Message);
        db2FreeEnvHdl(envp,nls_lang);
        envp = NULL;
    }
    // envp->connlist = connp = insertconnEntry (envp->connlist, srvname, user, password, hdbc);
  } else {
    /* search user session for this server in cache */
    connp = findconnEntry(envp->connlist, srvname, user);
    if (connp == NULL) {
      /* create connection handle */
      rc = SQLAllocHandle(SQL_HANDLE_DBC, envp->henv, &hdbc);
      db2Debug3("  alloc dbc handle - rc: %d, henv: %d, hdbc: %d",rc, envp->henv, hdbc);
      rc = db2CheckErr(rc, envp->henv, SQL_HANDLE_ENV, __LINE__, __FILE__);
      if (rc  != SQL_SUCCESS) {
        db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "error connecting to DB2: SQLAllochHandle failed to allocate hdbc handle", db2Message);
      }

      /* Check if JWT token authentication is used */
      if (jwt_token != NULL && jwt_token[0] != '\0') {
        /* JWT token authentication */
        db2Debug1("  using JWT token authentication");

        /* For DB2 11.5 LUW, JWT token is passed as the password parameter */
        /* Some installations may require a specific user like "token" or empty string */
        /* The actual authentication is handled by the DB2 authentication plugin */
        const char* jwt_user = (user != NULL && user[0] != '\0') ? user : "";

        rc = SQLConnect(hdbc, (SQLCHAR*)srvname, SQL_NTS, (SQLCHAR*)jwt_user, SQL_NTS, (SQLCHAR*)jwt_token, SQL_NTS);
        db2Debug1("  connect to database(%s) with JWT token - rc: %d, hdbc: %d", srvname, rc, hdbc);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate with JWT token", " connection connectstring: %s ,%s", srvname, db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate", " connection to foreign DB2 server,%s", db2Message);
        }
      } else {
        /* Traditional user/password authentication */
        db2Debug1("  using user/password authentication");
        rc = SQLConnect(hdbc, (SQLCHAR*)srvname, SQL_NTS, (SQLCHAR*)user, SQL_NTS, (SQLCHAR*)password, SQL_NTS);
        db2Debug1("  connect to database(%s) - rc: %d, hdbc: %d",srvname, rc, hdbc);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection User: %s ,%s"            , user    , db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection password: %s ,%s"        , password, db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection connectstring: %s ,%s"   , srvname , db2Message);
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "cannot authenticate"," connection to foreign DB2 server,%s"          , db2Message);
        }
      }

      if (rc == SQL_SUCCESS) {
        /* add session handle to cache */
        envp->connlist = connp = insertconnEntry (envp->connlist, srvname, user, password, jwt_token, hdbc);

        /* set Autocommit off */
        rc = SQLSetConnectAttr(hdbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, SQL_IS_UINTEGER);
        rc = db2CheckErr(rc, hdbc, SQL_HANDLE_DBC, __LINE__, __FILE__);
        if (rc != SQL_SUCCESS) {
          db2Error_d (FDW_UNABLE_TO_ESTABLISH_CONNECTION, "failed to set autocommit=off"," connection to foreign DB2 server,%s", db2Message);
        }
        /* register callback for PostgreSQL transaction events */
        db2RegisterCallback (connp);
      }
    }
  }
  db2Debug1("< db2AllocConnHdl - returns: %x",connp);
  return connp;
}

/** findconnEntry
 * 
 */
DB2ConnEntry* findconnEntry(DB2ConnEntry* start, const char* srvname, const char* user) {
  DB2ConnEntry* step = NULL;
  db2Debug2("  > findconnEntry");
  for (step = start; step != NULL; step = step->right){
    if (strcmp(step->srvname, srvname) == 0 && strcmp(step->uid, user) == 0) {
      break;
    }
  }
  db2Debug2("  < findconnEntry - returns: %x", step);
  return step;
}

/** insertconnEntry
 *
 */
DB2ConnEntry* insertconnEntry(DB2ConnEntry* start, const char* srvname, const char* uid, const char* pwd, const char* jwt_token, SQLHDBC hdbc) {
  DB2ConnEntry* step = NULL;
  DB2ConnEntry* new  = NULL;
<<<<<<< HEAD
  
  db2Debug2("  > insertconnEntry");
=======

  db2Debug1("> insertconnEntry");
>>>>>>> c2a54a9 (Add JWT token authentication support as alternative to user/password)
  if (start == NULL){ /* first entry in list */
    new = malloc(sizeof(DB2ConnEntry));
    new->right = new->left = NULL;
  } else {
    for (step = start; step->right != NULL; step = step->right){ }
    new = malloc(sizeof(DB2ConnEntry));
    step->right = new;
    new->left = step;
    new->right = NULL;
  }
  new->srvname    = strdup(srvname);
  new->uid        = strdup(uid);
  new->pwd        = strdup(pwd);
  new->jwt_token  = (jwt_token && jwt_token[0] != '\0') ? strdup(jwt_token) : NULL;
  new->handlelist = NULL;
  new->hdbc       = hdbc;
  new->xact_level = 0;
  db2Debug2("  < insertconnEntry - returns: %x",new);
  return new;
}
