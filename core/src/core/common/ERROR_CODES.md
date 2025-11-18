# MedalFlow Error Codes Reference Guide

## Overview

MedalFlow uses a structured error handling system with categorized error codes for consistent error reporting and troubleshooting. All errors inherit from `CTEError` base class and include an error code from the `ErrorCode` enum.

### Error Code Structure

Error codes follow the pattern: `CATEGORY_XXX`
- **Category**: Describes the error domain (CONFIG, VALIDATION, etc.)
- **Number**: Sequential identifier within the category

### Categories:
- **1xxx (CONFIG)**: Configuration-related errors
- **2xxx (VALIDATION)**: Input validation errors  
- **3xxx (CONNECTION)**: Network and connection errors
- **4xxx (EXECUTION)**: Runtime execution errors
- **5xxx (RESOURCE)**: Resource availability errors
- **6xxx (DATA)**: Data quality and integrity errors
- **7xxx (PLATFORM)**: Platform-specific errors
- **8xxx (OPERATION)**: High-level operation errors
- **9xxx (RETRY)**: Transient/retryable errors

## Quick Reference Table

| Error Code | Name | Description |
|------------|------|-------------|
| CONFIG_001 | Configuration Error | General configuration issue |
| CONFIG_002 | Missing Configuration | Required configuration not found |
| CONFIG_003 | Invalid Configuration | Configuration value is invalid |
| CONFIG_004 | Feature Disabled | Feature is not enabled |
| VALIDATION_001 | Validation Error | General validation failure |
| VALIDATION_002 | Invalid Argument | Function argument is invalid |
| VALIDATION_003 | Missing Parameter | Required parameter missing |
| VALIDATION_004 | Invalid Identifier | Invalid name or identifier |
| CONNECTION_001 | Connection Error | Failed to establish connection |
| CONNECTION_002 | Authentication Error | Authentication/authorization failed |
| CONNECTION_003 | Timeout Error | Operation timed out |
| EXECUTION_001 | Execution Error | General execution failure |
| EXECUTION_002 | Query Execution Error | SQL query failed |
| EXECUTION_003 | Job Submission Error | Failed to submit job |
| EXECUTION_004 | Job Status Error | Failed to get job status |
| EXECUTION_005 | Transformation Error | Data transformation failed |
| RESOURCE_001 | Resource Not Found | General resource missing |
| RESOURCE_002 | Table Not Found | Database table doesn't exist |
| RESOURCE_003 | File Not Found | File or path doesn't exist |
| RESOURCE_004 | Secret Not Found | Key Vault secret missing |
| DATA_001 | Data Quality Error | Data quality check failed |
| DATA_002 | Duplicate Key Error | Unique constraint violation |
| DATA_003 | Data Integrity Error | Data consistency issue |
| PLATFORM_001 | Platform Error | General platform issue |
| PLATFORM_002 | Platform Not Supported | Unsupported platform |
| PLATFORM_003 | Engine Not Available | Compute engine unavailable |
| OPERATION_001 | Operation Error | General operation failure |
| OPERATION_002 | Layer Processing Error | Medallion layer failed |
| OPERATION_003 | Copy Operation Error | Data copy failed |
| OPERATION_004 | Table Operation Error | Table operation failed |
| OPERATION_005 | ADLS Operation Error | Azure Data Lake operation failed |
| RETRY_001 | Retryable Error | Transient error, can retry |
| RETRY_002 | Rate Limit Error | API rate limit exceeded |

---

## Configuration Errors (1xxx)

### CONFIG_001: Configuration Error

**Description**: General configuration error when settings are misconfigured.

**Common Causes**:
- Malformed configuration files
- Incompatible configuration values
- Missing environment variables

**Solutions**:
1. Check configuration file syntax (JSON/YAML)
2. Verify all required fields are present
3. Ensure environment variables are set
4. Review configuration documentation

**Example**:
```python
try:
    platform = create_platform()
except CTEError as e:
    if e.error_code == ErrorCode.CONFIG_ERROR:
        print(f"Configuration issue: {e.details}")
        # Check configuration files and environment
```

---

### CONFIG_002: Missing Configuration

**Description**: Required configuration key or file is not found.

**Common Causes**:
- Configuration file not created
- Required key missing from config
- Wrong environment specified

**Solutions**:
1. Create missing configuration file
2. Add required configuration keys
3. Set correct environment (dev/staging/prod)
4. Check CLAUDE.md for required settings

**Example**:
```python
# Error typically shows missing key
# "synapse_endpoint not found in configuration"

# Solution: Add to your settings
export CTE_COMPUTE__SYNAPSE__CONNECTION__ENDPOINT="https://..."
```

---

### CONFIG_003: Invalid Configuration

**Description**: Configuration value doesn't meet requirements.

**Common Causes**:
- Invalid URL format
- Wrong data types
- Out-of-range values
- Invalid enum values

**Solutions**:
1. Verify configuration value format
2. Check data type requirements
3. Ensure values are within valid ranges
4. Review allowed enum options

**Example**:
```python
# Invalid compute type
compute_type: "invalid"  # Should be: synapse, fabric, databricks

# Invalid URL format
endpoint: "not-a-url"  # Should be: https://...
```

---

### CONFIG_004: Feature Disabled

**Description**: Attempting to use a feature that's not enabled.

**Common Causes**:
- Feature flag is off
- License limitation
- Environment restriction

**Solutions**:
1. Enable feature in configuration
2. Check license/subscription
3. Verify feature availability in environment
4. Contact admin for feature access

**Example**:
```python
# Enable feature in configuration
features:
  enable_spark: true
  enable_streaming: false  # This would cause FEATURE_DISABLED error
```

---

## Validation Errors (2xxx)

### VALIDATION_001: Validation Error

**Description**: General validation failure for input data.

**Common Causes**:
- Invalid data format
- Business rule violation
- Schema mismatch

**Solutions**:
1. Check input data format
2. Verify business rules
3. Validate against schema
4. Review validation requirements

**Example**:
```python
try:
    result = process_data(df)
except CTEError as e:
    if e.error_code == ErrorCode.VALIDATION_ERROR:
        print(f"Validation failed for field: {e.details.get('field')}")
```

---

### VALIDATION_002: Invalid Argument

**Description**: Function received invalid argument.

**Common Causes**:
- Wrong data type
- Out-of-range value
- Null/None where not allowed
- Invalid enum value

**Solutions**:
1. Check argument data types
2. Verify value ranges
3. Handle None/null cases
4. Use valid enum values

**Example**:
```python
# Invalid argument example
create_table(schema="", table_name="users")  # Empty schema not allowed

# Fix:
create_table(schema="bronze", table_name="users")
```

---

### VALIDATION_003: Missing Parameter

**Description**: Required parameter not provided.

**Common Causes**:
- Forgot to pass required argument
- Configuration missing required field
- API call missing parameter

**Solutions**:
1. Check function signature
2. Provide all required parameters
3. Review API documentation
4. Set default values where appropriate

**Example**:
```python
# Missing required parameter
platform.execute_query()  # Missing 'query' parameter

# Fix:
platform.execute_query(query="SELECT * FROM table")
```

---

### VALIDATION_004: Invalid Identifier

**Description**: Invalid name or identifier format.

**Common Causes**:
- Special characters in names
- Reserved keywords used
- Name too long
- Invalid SQL identifier

**Solutions**:
1. Use valid characters (alphanumeric, underscore)
2. Avoid reserved keywords
3. Check length limits
4. Follow naming conventions

**Example**:
```python
# Invalid identifiers
table_name = "user-data"  # Hyphen not allowed
schema_name = "SELECT"     # Reserved keyword

# Valid identifiers
table_name = "user_data"
schema_name = "bronze"
```

---

## Connection Errors (3xxx)

### CONNECTION_001: Connection Error

**Description**: Failed to establish connection to service.

**Common Causes**:
- Service unavailable
- Network issues
- Firewall blocking
- Wrong endpoint

**Solutions**:
1. Verify service is running
2. Check network connectivity
3. Review firewall rules
4. Confirm endpoint URL
5. Test with connection tools

**Example**:
```python
# Troubleshooting steps
1. Test connectivity: ping/telnet to endpoint
2. Check credentials: az login
3. Verify firewall: Check Azure NSG/Firewall rules
4. Review logs: Check detailed error message
```

---

### CONNECTION_002: Authentication Error

**Description**: Authentication or authorization failed.

**Common Causes**:
- Invalid credentials
- Expired token
- Insufficient permissions
- Wrong auth method

**Solutions**:
1. Verify credentials
2. Refresh authentication token
3. Check RBAC permissions
4. Use correct auth method
5. Review service principal

**Example**:
```python
# Common fixes
az login --service-principal -u <app-id> -p <password> --tenant <tenant>
az account set --subscription <subscription-id>

# Check permissions
az role assignment list --assignee <principal-id>
```

---

### CONNECTION_003: Timeout Error

**Description**: Operation exceeded timeout limit.

**Common Causes**:
- Slow network
- Large data processing
- Service overloaded
- Deadlock

**Solutions**:
1. Increase timeout value
2. Optimize query/operation
3. Retry with backoff
4. Use smaller batch sizes
5. Check for deadlocks

**Example**:
```python
# Increase timeout
config = SparkJobConfig(
    timeout_seconds=600,  # Increase from default
    max_retries=3
)

# Use batching
for batch in chunks(data, size=1000):
    process_batch(batch)
```

---

## Execution Errors (4xxx)

### EXECUTION_001: Execution Error

**Description**: General runtime execution failure.

**Common Causes**:
- Logic errors
- Runtime exceptions
- Resource constraints
- Dependency issues

**Solutions**:
1. Review error stack trace
2. Check resource availability
3. Verify dependencies
4. Add error handling
5. Review logs

---

### EXECUTION_002: Query Execution Error

**Description**: SQL query failed to execute.

**Common Causes**:
- Syntax errors
- Missing tables/columns
- Permission issues
- Resource limits

**Solutions**:
1. Validate SQL syntax
2. Check table/column existence
3. Verify permissions
4. Optimize query
5. Increase resource limits

**Example**:
```python
# Common SQL issues
- Missing table: Check if table exists in schema
- Syntax error: Validate with SQL linter
- Permission: GRANT SELECT ON schema.table TO user
- Timeout: Add query hints, create indexes
```

---

### EXECUTION_003: Job Submission Error

**Description**: Failed to submit Spark/batch job.

**Common Causes**:
- Cluster unavailable
- Invalid job configuration
- Resource quota exceeded
- File not found

**Solutions**:
1. Verify cluster status
2. Check job configuration
3. Review resource quotas
4. Ensure files exist
5. Check submission logs

---

### EXECUTION_004: Job Status Error

**Description**: Failed to retrieve job status.

**Common Causes**:
- Job ID not found
- API issues
- Permission problems
- Job already deleted

**Solutions**:
1. Verify job ID
2. Check job history
3. Review permissions
4. Retry API call
5. Check retention policy

---

### EXECUTION_005: Transformation Error

**Description**: Data transformation operation failed.

**Common Causes**:
- Invalid transformation logic
- Data type mismatch
- Null value handling
- Memory issues

**Solutions**:
1. Review transformation logic
2. Handle data type conversions
3. Add null checks
4. Optimize memory usage
5. Use appropriate data types

---

## Resource Errors (5xxx)

### RESOURCE_001: Resource Not Found

**Description**: General resource not found error.

**Common Causes**:
- Resource deleted
- Wrong name/path
- Permission issues
- Not yet created

**Solutions**:
1. Verify resource exists
2. Check resource name/path
3. Review permissions
4. Create resource if needed

---

### RESOURCE_002: Table Not Found

**Description**: Database table doesn't exist.

**Common Causes**:
- Table not created
- Wrong schema/database
- Case sensitivity issue
- Dropped table

**Solutions**:
1. Create missing table
2. Verify schema.table name
3. Check case sensitivity
4. Review table creation logs

**Example**:
```sql
-- Check if table exists
SELECT * FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'users';

-- Create if missing
CREATE TABLE bronze.users (...);
```

---

### RESOURCE_003: File Not Found

**Description**: File or directory doesn't exist.

**Common Causes**:
- Wrong file path
- File deleted
- Permission denied
- Not uploaded yet

**Solutions**:
1. Verify file path
2. Check file existence
3. Review permissions
4. Upload missing file
5. Check mount points

---

### RESOURCE_004: Secret Not Found

**Description**: Key Vault secret not found.

**Common Causes**:
- Secret not created
- Wrong secret name
- No access policy
- Wrong Key Vault

**Solutions**:
1. Create secret in Key Vault
2. Verify secret name
3. Add access policy
4. Check Key Vault URL

**Example**:
```bash
# Create secret
az keyvault secret set --vault-name <vault> --name <secret> --value <value>

# Grant access
az keyvault set-policy --name <vault> --object-id <principal> --secret-permissions get list
```

---

## Data Errors (6xxx)

### DATA_001: Data Quality Error

**Description**: Data quality validation failed.

**Common Causes**:
- Missing required fields
- Invalid data format
- Out-of-range values
- Referential integrity

**Solutions**:
1. Review data quality rules
2. Clean/transform data
3. Add data validation
4. Fix source data
5. Update quality thresholds

---

### DATA_002: Duplicate Key Error

**Description**: Unique constraint violation.

**Common Causes**:
- Duplicate primary key
- Unique index violation
- Concurrent inserts
- Data loading issue

**Solutions**:
1. Remove duplicates
2. Use MERGE/UPSERT
3. Add conflict handling
4. Review data source
5. Add deduplication logic

---

### DATA_003: Data Integrity Error

**Description**: Data consistency issue detected.

**Common Causes**:
- Foreign key violation
- Orphaned records
- Inconsistent state
- Transaction failure

**Solutions**:
1. Fix referential integrity
2. Clean orphaned records
3. Add transaction handling
4. Implement consistency checks
5. Review data relationships

---

## Platform Errors (7xxx)

### PLATFORM_001: Platform Error

**Description**: General platform-level error.

**Common Causes**:
- Platform service issue
- Configuration mismatch
- Version incompatibility
- Platform limits

**Solutions**:
1. Check platform status
2. Review platform logs
3. Verify compatibility
4. Check service limits
5. Contact platform support

---

### PLATFORM_002: Platform Not Supported

**Description**: Requested platform not supported.

**Common Causes**:
- Invalid platform name
- Not implemented yet
- License restriction
- Region limitation

**Solutions**:
1. Use supported platform (synapse/fabric)
2. Check platform availability
3. Review license
4. Verify region support

---

### PLATFORM_003: Engine Not Available

**Description**: Compute engine is unavailable.

**Common Causes**:
- Engine not started
- Resource exhausted
- Cluster scaled down
- Configuration issue

**Solutions**:
1. Start compute engine
2. Scale up resources
3. Check cluster status
4. Review engine config
5. Wait for engine startup

---

## Operation Errors (8xxx)

### OPERATION_001: Operation Error

**Description**: High-level operation failed.

**Common Causes**:
- Complex operation failure
- Multiple step failure
- Orchestration issue
- Dependency failure

**Solutions**:
1. Review operation logs
2. Check each step
3. Verify dependencies
4. Retry operation
5. Break into smaller operations

---

### OPERATION_002: Layer Processing Error

**Description**: Medallion layer processing failed.

**Common Causes**:
- Source data issue
- Transformation failure
- Schema mismatch
- Resource constraints

**Solutions**:
1. Check source data
2. Review transformations
3. Verify schemas
4. Increase resources
5. Check layer dependencies

---

### OPERATION_003: Copy Operation Error

**Description**: Data copy operation failed.

**Common Causes**:
- Source unavailable
- Target issue
- Network problem
- Permission denied

**Solutions**:
1. Verify source availability
2. Check target access
3. Review network
4. Fix permissions
5. Retry with smaller batches

---

### OPERATION_004: Table Operation Error

**Description**: Table operation (CREATE/ALTER/DROP) failed.

**Common Causes**:
- DDL syntax error
- Permission issue
- Table locked
- Schema conflict

**Solutions**:
1. Check DDL syntax
2. Verify permissions
3. Check table locks
4. Review schema
5. Use IF EXISTS/IF NOT EXISTS

---

### OPERATION_005: ADLS Operation Error

**Description**: Azure Data Lake Storage operation failed.

**Common Causes**:
- Storage account issue
- Container not found
- Permission denied
- Network issue

**Solutions**:
1. Check storage account
2. Verify container exists
3. Review ACLs/RBAC
4. Check firewall rules
5. Verify service endpoints

---

## Retry/Transient Errors (9xxx)

### RETRY_001: Retryable Error

**Description**: Transient error that can be retried.

**Common Causes**:
- Temporary unavailability
- Network glitch
- Resource contention
- Throttling

**Solutions**:
1. Retry with exponential backoff
2. Add jitter to retries
3. Implement circuit breaker
4. Queue for later retry
5. Check retry configuration

**Example**:
```python
from core.utils.decorators import retry_with_backoff

@retry_with_backoff(max_retries=3, initial_delay=1, exponential_base=2)
def operation():
    # Operation that might fail transiently
    pass
```

---

### RETRY_002: Rate Limit Error

**Description**: API rate limit exceeded.

**Common Causes**:
- Too many requests
- Burst limit exceeded
- Quota exhausted
- No rate limiting

**Solutions**:
1. Implement rate limiting
2. Add request throttling
3. Use exponential backoff
4. Batch requests
5. Increase quota limits

**Example**:
```python
import time

def with_rate_limit(func, max_per_second=10):
    delay = 1.0 / max_per_second
    time.sleep(delay)
    return func()
```

---

## Error Handling Best Practices

### 1. Catch Specific Error Codes
```python
try:
    result = operation()
except CTEError as e:
    if e.error_code == ErrorCode.RESOURCE_NOT_FOUND:
        # Handle missing resource
        create_resource()
    elif e.error_code == ErrorCode.RETRY_001:
        # Retry transient error
        retry_operation()
    else:
        # Log and re-raise
        logger.error(f"Operation failed: {e}")
        raise
```

### 2. Use Helper Functions
```python
from core.common.exceptions import (
    configuration_error,
    validation_error,
    resource_not_found_error
)

# Raise specific errors
if not config.get("endpoint"):
    raise configuration_error(
        "Missing endpoint configuration",
        config_key="endpoint"
    )
```

### 3. Log Error Details
```python
try:
    process_data()
except CTEError as e:
    logger.error(
        "Process failed",
        error_code=e.error_code.value,
        details=e.details,
        is_retryable=e.is_retryable
    )
```

### 4. Handle Retryable Errors
```python
def process_with_retry(data, max_attempts=3):
    for attempt in range(max_attempts):
        try:
            return process_data(data)
        except CTEError as e:
            if not e.is_retryable or attempt == max_attempts - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

---

## Troubleshooting Workflow

1. **Identify Error Code**: Check the error message for the error code
2. **Look Up Error**: Find the error code in this guide
3. **Review Common Causes**: Check which cause matches your scenario
4. **Apply Solutions**: Try solutions in order
5. **Check Logs**: Review detailed logs for more context
6. **Verify Fix**: Ensure the error is resolved
7. **Document**: Update runbook if new solution found

---

## Related Documentation

- [Exception Handling](exceptions.py): Core exception implementation
- [ARCHITECTURE.md](../../ARCHITECTURE.md): System architecture
- [CODING_GUIDELINES.md](../../CODING_GUIDELINES.md): Coding standards
- [README.md](../../README.md): Project overview

---

## Support

If you encounter an error not covered here or need additional assistance:

1. Check the detailed logs
2. Review the stack trace
3. Search existing issues
4. Create a new issue with error details
5. Contact the development team

Remember to include:
- Error code
- Error message
- Stack trace
- Steps to reproduce
- Environment details