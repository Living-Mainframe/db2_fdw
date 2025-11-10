# JWT Authentication for DB2 FDW

This fork adds JWT (JSON Web Token) authentication support to db2_fdw as an alternative to traditional user/password authentication.

## Prerequisites

### Minimum Version Requirements

**CRITICAL:** JWT authentication requires:
- **DB2 Client: 11.5 Mod Pack 4 (11.5.4) or higher**
- **DB2 Server: 11.5.4 or higher** with JWT configuration

If your client is below 11.5.4, you will receive errors like "password missing" (SQL30082N, RC 3) or "Invalid argument value" (HY009).

### DB2 Server Configuration

JWT authentication in DB2 requires proper server-side configuration:

1. **Create db2token.cfg file** in `$DB2_INSTANCE_HOME/sqllib/cfg/`:
   ```
   # Example db2token.cfg
   # Specifies keystore location and JWT validation parameters
   KEYSTORE=/path/to/keystore
   ISSUER=your-issuer-url
   ALGORITHM=RS256
   ```

2. **Configure Server Authentication**:
   ```bash
   # Update database manager configuration to support token authentication
   db2 update dbm cfg using SRVCON_AUTH SERVER_ENCRYPT_TOKEN
   # Or other token-supporting authentication types
   ```

3. **Verify Configuration**:
   ```bash
   db2 get dbm cfg | grep -i authentication
   # Should show SERVER_ENCRYPT_TOKEN or similar
   ```

### DB2 Client Configuration

The db2_fdw uses SQLDriverConnect with these ODBC/CLI keywords:
- `AUTHENTICATION=TOKEN` - Specifies token-based authentication
- `ACCESSTOKEN=<your_jwt_token>` - The actual JWT token
- `ACCESSTOKENTYPE=JWT` - Identifies token format as JWT

## Usage

### Option 1: JWT Token Only

When using JWT token authentication:

```sql
-- JWT token authentication (user identity is in the token)
CREATE USER MAPPING FOR PUBLIC SERVER db2_server
  OPTIONS (jwt_token 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...');
```

**Note:** JWT tokens contain identity information, so specifying a separate `user` option is typically not required.

### Option 2: Traditional User/Password

```sql
CREATE USER MAPPING FOR PUBLIC SERVER db2_server
  OPTIONS (user 'db2inst1', password 'db2inst1');
```

### Validation Rules

- Either `jwt_token` OR (`user` AND `password`) must be specified
- You cannot specify both `jwt_token` and `password` simultaneously
- Clear error messages guide you to correct configuration

## Implementation Details

### How JWT Authentication Works

The db2_fdw implementation uses SQLDriverConnect with DB2-specific ODBC keywords:

```c
// Connection string format:
"DSN=<datasource>;AUTHENTICATION=TOKEN;ACCESSTOKEN=<jwt_token>;ACCESSTOKENTYPE=JWT;"
```

**Key points:**
- Uses `AUTHENTICATION=TOKEN` instead of standard username/password auth
- Token passed via `ACCESSTOKEN` keyword (NOT as password)
- `ACCESSTOKENTYPE=JWT` explicitly identifies the token format
- User identity is extracted from the JWT claims by DB2 server

### Connection Caching

- JWT tokens are cached in connection entries
- Connections are reused based on (server, jwt_token) tuple
- Token expiration handling is responsibility of the application
- If token expires, connection will fail and need re-establishment with new token

## Troubleshooting

### Error: HY009 "Invalid argument value" (CLI0124E)

**This error indicates one of the following:**

1. **DB2 Client version is below 11.5.4**
   - Check: `db2level` on client machine
   - Solution: Upgrade to DB2 11.5.4 or higher

2. **DB2 Server not configured for JWT authentication**
   - Missing db2token.cfg file
   - SRVCON_AUTH not set to support token authentication
   - Solution: Follow server configuration steps above

3. **Token format or content is invalid**
   - Token is expired
   - Token not properly formatted
   - Solution: Validate token with JWT decoder (jwt.io)

**Debugging steps:**
```bash
# Check DB2 client version
db2level

# Check server authentication configuration
db2 get dbm cfg | grep -i SRVCON_AUTH

# Enable DB2 CLI trace for detailed diagnostics
export DB2_CLI_TRACE=1
export DB2_CLI_TRACE_FILE=/tmp/db2cli.log

# Check trace file for connection attempt details
tail -f /tmp/db2cli.log
```

### Error: SQL30082N "Security processing failed" (RC 24)

**Possible causes:**
- JWT signature validation failed on server
- Token issuer not in db2token.cfg
- Public key mismatch between token and keystore

**Check:**
```bash
# Verify db2token.cfg exists and is readable
ls -la $DB2_INSTANCE_HOME/sqllib/cfg/db2token.cfg

# Check DB2 diagnostic log
db2diag -A

# Look for JWT-related errors in db2diag.log
```

### Testing JWT Authentication

```sql
-- Create test foreign table
CREATE FOREIGN TABLE test_table (
    id INTEGER,
    name VARCHAR(100)
) SERVER db2_server OPTIONS (schema 'MYSCHEMA', table 'MYTABLE');

-- Test query
SELECT * FROM test_table LIMIT 1;
```

## Environment Variables

For debugging, enable DB2 CLI tracing:

```bash
export DB2_CLI_TRACE=1
export DB2_CLI_TRACE_FILE=/tmp/db2cli.log
```

## References

### Official IBM Documentation
- [DB2 11.5 Authentication](https://www.ibm.com/docs/en/db2/11.5?topic=authentication-plugins)
- [DB2 CLI Configuration Keywords](https://www.ibm.com/docs/en/db2/11.5?topic=parameters-cli-odbc-configuration-keywords)
- [DB2 db2dsdriver.cfg Configuration](https://www.ibm.com/docs/en/db2/11.5?topic=files-db2dsdrivercfg-file)

### Community Resources
- [Data Henrik: Db2 Security - Configure JWT Authentication](https://blog.4loeser.net/2021/01/db2-security-jwt-access-token.html)
- [Data Henrik: JWT token authentication in Db2 runtimes](https://blog.4loeser.net/2021/03/db2-jwt-security-python-nodejs.html)
- [JWTutil - Db2 JWT Setup Guide](https://github.com/data-henrik/JWTutil/blob/main/Db2_JWT.md)
- [Stack Overflow: Db2 JWT Access Token Connection](https://stackoverflow.com/questions/66413357/db2-ibm-db-python-how-to-connect-using-a-jwt-access-token)

## License

Same as db2_fdw - see LICENSE file.
