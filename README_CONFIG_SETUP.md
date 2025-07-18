# Configuration Setup Guide

## SQL Server Authentication Setup

Since you're using Azure SQL Database or a SQL Server that doesn't support Windows authentication, you need to use SQL Server authentication.

### 1. Edit config.jcl.json

Update your `config.jcl.json` file with the following settings:

```json
{
    "database": {
        "server": "your-server.database.windows.net",  // Your actual server name
        "database": "JCL",
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": false,                   // MUST be false for SQL auth
        "username": "your-sql-username",               // Your SQL username
        "password": "your-sql-password",               // Your SQL password
        "connection_timeout": 30,
        "query_timeout": 300,
        "schema_prefix": "lme_"
    },
    "bloomberg": {
        "host": "localhost",
        "port": 8194
    },
    "collection": {
        "batch_size": 50,
        "retry_attempts": 3,
        "retry_delay_seconds": 5,
        "duplicate_check": true
    },
    "logging": {
        "level": "INFO",
        "file": "logs/lme_collector.log",
        "max_bytes": 10485760,
        "backup_count": 5
    }
}
```

### 2. Important Settings

- **trusted_connection**: Must be `false` for SQL Server authentication
- **username**: Your SQL Server login username
- **password**: Your SQL Server login password
- **server**: Full server name (for Azure: `servername.database.windows.net`)

### 3. Security Notes

- Never commit `config.jcl.json` to Git (it's already in .gitignore)
- Consider using environment variables for sensitive information
- For production, use Azure Key Vault or similar secure storage

### 4. Test Connection

After updating the config, test the connection:

```bash
python scripts\sql_collector\test_sql_system_jcl.py
```

### 5. Troubleshooting

If you still get connection errors:

1. **Check server name**: Make sure it includes the full domain for Azure SQL
2. **Check firewall**: Ensure your IP is allowed in Azure SQL firewall rules
3. **Check credentials**: Verify username/password in SSMS first
4. **Check port**: Some environments may need to specify port (e.g., `server,1433`)

### Example for Azure SQL Database

```json
"server": "myserver.database.windows.net",
"database": "JCL",
"username": "sqladmin@myserver",  // Note: may need @servername for older Azure SQL
"password": "MySecurePassword123!"
```