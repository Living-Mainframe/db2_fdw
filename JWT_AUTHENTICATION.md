# JWT Authentication for DB2 FDW

This fork adds JWT (JSON Web Token) authentication support to db2_fdw as an alternative to traditional user/password authentication.

## Configuration

### For DB2 11.5 LUW on-premise

JWT tokens are passed as the password parameter in the connection. The actual authentication is handled by the DB2 authentication plugin configured on your DB2 server.

#### Setup Requirements

1. **DB2 Server Configuration**
   - JWT authentication plugin must be configured on the DB2 server
   - Common setup: IBMSecurityAccessManager or custom JWT plugin
   - Verify with: `db2 get dbm cfg | grep -i authentication`

2. **db2dsdriver.cfg Configuration** (if using)
   ```xml
   <configuration>
     <dsncollection>
       <dsn alias="YOUR_DSN" name="YOURDB" host="hostname" port="50000">
         <parameter name="Authentication" value="SERVER"/>
         <parameter name="SecurityTransportMode" value="SSL"/>
       </dsn>
     </dsncollection>
   </configuration>
   ```

## Usage

### Option 1: JWT Token Only

When using JWT token, you can optionally specify a user or leave it empty:

```sql
-- With JWT token (no user specified)
CREATE USER MAPPING FOR PUBLIC SERVER db2_server
  OPTIONS (jwt_token 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...');

-- With JWT token and specific user
CREATE USER MAPPING FOR PUBLIC SERVER db2_server
  OPTIONS (user 'myuser', jwt_token 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...');
```

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

### How JWT is Passed

For DB2 11.5 LUW, the JWT token is passed as the password parameter:
- User field: Can be empty or contain a username (depends on your DB2 setup)
- Password field: Contains the JWT token
- DB2 authentication plugin validates the token

### Connection Caching

- JWT tokens are cached in connection entries
- Connections are reused based on (server, user, jwt_token) tuple
- Token expiration handling is responsibility of the application

## Troubleshooting

### Error: "cannot authenticate with JWT token"

**Possible causes:**
1. DB2 server doesn't have JWT authentication plugin configured
2. Token is expired or invalid
3. Wrong DSN configuration

**Check:**
```bash
# Test connection with db2cli
db2cli validate -dsn YOUR_DSN -user "" -passwd "YOUR_JWT_TOKEN"

# Enable DB2 CLI trace for debugging
export DB2_CLI_TRACE=1
export DB2_CLI_TRACE_FILE=/tmp/db2cli.log
```

### Error: "Option type out of range"

This was the original error with `SQL_ATTR_ACCESS_TOKEN_STR`. The updated implementation doesn't use this attribute anymore, so this error should be resolved.

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

- [DB2 Authentication Plugins](https://www.ibm.com/docs/en/db2/11.5?topic=authentication-plugins)
- [DB2 CLI Configuration](https://www.ibm.com/docs/en/db2/11.5?topic=files-db2dsdrivercfg-file)

## License

Same as db2_fdw - see LICENSE file.
