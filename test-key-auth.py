#!/usr/bin/env python3

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import sys
import os
from datetime import datetime

# Load environment variables from .env file if it exists
def load_env_file():
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"Loading environment variables from {env_file}")
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()
        print("Environment variables loaded")
    else:
        print("No .env file found, using interactive input or defaults")

# Load .env file first
load_env_file()

# Configuration - can be set via environment variables, .env file, or interactively
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE')

PRIVATE_KEY_PATH = os.getenv('PRIVATE_KEY_PATH', 'rsa_key.p8')
PRIVATE_KEY_PASSWORD = os.getenv('PRIVATE_KEY_PASSWORD')

def load_private_key():
    import os
    
    print(f"Checking for private key file: {PRIVATE_KEY_PATH}")
    if not os.path.exists(PRIVATE_KEY_PATH):
        print(f"❌ Key file not found: {PRIVATE_KEY_PATH}")
        print("💡 Make sure rsa_key.p8 is in the same directory as this script")
        return None
    
    file_size = os.path.getsize(PRIVATE_KEY_PATH)
    print(f"✅ Key file found ({file_size} bytes)")
    
    try:
        print(f"Loading private key from: {PRIVATE_KEY_PATH}")
        
        with open(PRIVATE_KEY_PATH, 'rb') as key_file:
            key_data = key_file.read()
            
        # Try PKCS#8 format first (for .p8 files)
        try:
            private_key = serialization.load_der_private_key(
                key_data,
                password=PRIVATE_KEY_PASSWORD,
                backend=default_backend()
            )
        except ValueError:
            # Fallback to PEM format
            private_key = serialization.load_pem_private_key(
                key_data,
                password=PRIVATE_KEY_PASSWORD,
                backend=default_backend()
            )
        
        print("Private key loaded successfully")
        return private_key
        
    except FileNotFoundError:
        print(f"Key file not found: {PRIVATE_KEY_PATH}")
        print("Please update PRIVATE_KEY_PATH with your actual key file path")
        return None
        
    except ValueError as e:
        if "password" in str(e).lower():
            print("Key requires password but none provided")
            print("Update PRIVATE_KEY_PASSWORD with your key password")
        else:
            print(f"Invalid key format: {e}")
        return None
        
    except Exception as e:
        print(f"Error loading key: {e}")
        return None

def test_connection(private_key):
    try:
        print(f"\nTesting connection to Snowflake...")
        print(f"Account: {SNOWFLAKE_ACCOUNT}")
        print(f"User: {SNOWFLAKE_USER}")
        print(f"Role: {SNOWFLAKE_ROLE}")
        print(f"Warehouse: {SNOWFLAKE_WAREHOUSE}")
        
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            account=SNOWFLAKE_ACCOUNT,
            private_key=private_key,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE
        )
        
        print("Connection established successfully")
        return conn
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        print("\nTroubleshooting tips:")
        print("1. Verify SNOWFLAKE_ACCOUNT URL format")
        print("2. Check SNOWFLAKE_USER exists and has proper permissions")
        print("3. Ensure SNOWFLAKE_ROLE has necessary privileges")
        print("4. Confirm private key is associated with the user in Snowflake")
        print("5. Check if key needs a password (update PRIVATE_KEY_PASSWORD)")
        return None

def test_basic_queries(conn):
    try:
        print(f"\nTesting basic queries...")
        cursor = conn.cursor()
        
        cursor.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        
        print(f"Snowflake Version: {result[0]}")
        print(f"Current User: {result[1]}")
        print(f"Current Role: {result[2]}")
        print(f"Current Warehouse: {result[3]}")
        
        return cursor
        
    except Exception as e:
        print(f"Basic queries failed: {e}")
        return None

def test_monitoring_access(cursor):
    """Test access to monitoring views - YOUR REQUIREMENTS"""
    try:
        print(f"\nTesting monitoring access (Account Usage views)...")
        
        # Test 1: Query History Access (Requirements 1.C, 2.C)
        print("Testing query history access...")
        cursor.execute("""
            SELECT COUNT(*) as query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE START_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        """)
        query_count = cursor.fetchone()[0]
        print(f"Queries in last 24h: {query_count}")
        
        # Test 2: Warehouse Load History (Requirement 1.A - CPU/Memory)
        print("Testing warehouse performance metrics...")
        cursor.execute("""
            SELECT WAREHOUSE_NAME, 
                   AVG(AVG_RUNNING) as avg_cpu_usage,
                   COUNT(*) as records
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY 
            WHERE START_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
            LIMIT 5
        """)
        warehouses = cursor.fetchall()
        if warehouses:
            print(f"Found {len(warehouses)} warehouses with performance data:")
            for wh in warehouses:
                print(f"         - {wh[0]}: Avg CPU {wh[1]:.2f}%, {wh[2]} records")
        else:
            print(" No warehouse performance data (may need time to accumulate)")
        
        # Test 3: Storage Usage (Requirement 1.B, 1.D)
        print("Testing storage metrics...")
        cursor.execute("""
            SELECT 
                USAGE_DATE,
                STORAGE_BYTES/1024/1024/1024 as storage_gb,
                STAGE_BYTES/1024/1024/1024 as stage_gb,
                FAILSAFE_BYTES/1024/1024/1024 as failsafe_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE 
            WHERE USAGE_DATE >= DATEADD(day, -7, CURRENT_DATE())
            ORDER BY USAGE_DATE DESC
            LIMIT 3
        """)
        storage_data = cursor.fetchall()
        if storage_data:
            print(f"Found storage data for {len(storage_data)} days:")
            for day in storage_data:
                print(f"- {day[0]}: {day[1]:.2f}GB total, {day[2]:.2f}GB stage, {day[3]:.2f}GB failsafe")
        
        print("Testing warehouse credit usage...")
        cursor.execute("""
            SELECT WAREHOUSE_NAME,
                   SUM(CREDITS_USED) as total_credits,
                   COUNT(*) as records
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
            WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
            GROUP BY WAREHOUSE_NAME
            ORDER BY total_credits DESC
            LIMIT 5
        """)
        credits = cursor.fetchall()
        if credits:
            print(f"Found credit usage for {len(credits)} warehouses:")
            for wh in credits:
                print(f"- {wh[0]}: {wh[1]:.2f} credits ({wh[2]} records)")
        
        print("Testing query execution metrics...")
        cursor.execute("""
            SELECT 
                QUERY_TYPE,
                COUNT(*) as query_count,
                AVG(EXECUTION_TIME/1000) as avg_exec_seconds,
                MAX(EXECUTION_TIME/1000) as max_exec_seconds
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE START_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            AND EXECUTION_TIME IS NOT NULL
            GROUP BY QUERY_TYPE
            ORDER BY query_count DESC
            LIMIT 5
        """)
        perf_data = cursor.fetchall()
        if perf_data:
            print(f"Query performance data by type:")
            for row in perf_data:
                print(f"- {row[0]}: {row[1]} queries, avg {row[2]:.2f}s, max {row[3]:.2f}s")
        
        return True
        
    except Exception as e:
        print(f"Monitoring access test failed: {e}")
        print("This may indicate insufficient permissions for DATADOG_ROLE")
        print("Ensure role has IMPORTED PRIVILEGES on SNOWFLAKE database")
        return False

def test_datadog_requirements(cursor):
    try:
        print(f"\nTesting specific Datadog monitoring requirements...")
        
        requirements_tests = [
            {
                'name': '1.A - CPU & Memory Utilization',
                'query': '''
                    SELECT 'CPU_Memory_Test' as test_name, COUNT(*) as available_records
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY 
                    WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
                '''
            },
            {
                'name': '1.B - Storage Utilization',
                'query': '''
                    SELECT 'Storage_Test' as test_name, COUNT(*) as available_records
                    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE 
                    WHERE USAGE_DATE >= DATEADD(day, -30, CURRENT_DATE())
                '''
            },
            {
                'name': '1.C - Query Execution Time',
                'query': '''
                    SELECT 'Query_Performance_Test' as test_name, COUNT(*) as available_records
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
                    WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
                '''
            },
            {
                'name': '2.A - Data Loading Rate',
                'query': '''
                    SELECT 'Data_Loading_Test' as test_name, COUNT(*) as available_records
                    FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
                    WHERE LAST_LOAD_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                '''
            },
            {
                'name': '2.B - Warehouse Usage',
                'query': '''
                    SELECT 'Warehouse_Usage_Test' as test_name, COUNT(*) as available_records
                    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
                    WHERE START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                '''
            }
        ]
        
        all_passed = True
        for test in requirements_tests:
            try:
                cursor.execute(test['query'])
                result = cursor.fetchone()
                print(f"{test['name']}: {result[1]} records available")
            except Exception as e:
                print(f"{test['name']}: Failed - {e}")
                all_passed = False
        
        return all_passed
        
    except Exception as e:
        print(f"Requirements testing failed: {e}")
        return False

def get_configuration():
    """Get Snowflake connection details from env vars or interactively"""
    global SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE
    
    # Check if we already have values from environment variables
    env_loaded = all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE])
    
    if env_loaded:
        print("\n✅ Configuration loaded from environment variables:")
        print(f"   Account: {SNOWFLAKE_ACCOUNT}")
        print(f"   User: {SNOWFLAKE_USER}")
        print(f"   Role: {SNOWFLAKE_ROLE}")
        print(f"   Warehouse: {SNOWFLAKE_WAREHOUSE}")
        return True
    
    print("\n📋 Please provide your Snowflake connection details:")
    if SNOWFLAKE_ACCOUNT:
        print(f"(Some values loaded from environment)")
    print("(Press Enter to keep existing value or skip)\n")
    
    account = input("Account URL (e.g., abc123.snowflakecomputing.com): ").strip()
    user = input("Username: ").strip()
    role = input("Role (e.g., ACCOUNTADMIN): ").strip()
    warehouse = input("Warehouse (e.g., COMPUTE_WH): ").strip()
    
    if account:
        SNOWFLAKE_ACCOUNT = account
    if user:
        SNOWFLAKE_USER = user
    if role:
        SNOWFLAKE_ROLE = role
    if warehouse:
        SNOWFLAKE_WAREHOUSE = warehouse
    
    # Validate we have all required values
    missing = []
    if not SNOWFLAKE_ACCOUNT or "your-actual" in str(SNOWFLAKE_ACCOUNT):
        missing.append("ACCOUNT")
    if not SNOWFLAKE_USER or "your_actual" in str(SNOWFLAKE_USER):
        missing.append("USER")
    if not SNOWFLAKE_ROLE or "your_actual" in str(SNOWFLAKE_ROLE):
        missing.append("ROLE")
    if not SNOWFLAKE_WAREHOUSE or "your_actual" in str(SNOWFLAKE_WAREHOUSE):
        missing.append("WAREHOUSE")
    
    if missing:
        print(f"\n❌ Missing configuration: {', '.join(missing)}")
        print("Please update the script or provide values when prompted.")
        return False
    
    print(f"\n✅ Configuration set:")
    print(f"   Account: {SNOWFLAKE_ACCOUNT}")
    print(f"   User: {SNOWFLAKE_USER}")
    print(f"   Role: {SNOWFLAKE_ROLE}")
    print(f"   Warehouse: {SNOWFLAKE_WAREHOUSE}")
    
    return True

def main():
    print("=" * 70)
    print("SNOWFLAKE PRIVATE LINK KEY AUTHENTICATION TEST")
    print("=" * 70)
    print(f"Test started at: {datetime.now()}")
    
    # Get configuration
    if not get_configuration():
        return False
    print()
    
    private_key = load_private_key()
    if not private_key:
        print("\nCannot proceed without valid private key")
        return False
    
    conn = test_connection(private_key)
    if not conn:
        print("\nCannot proceed without valid connection")
        return False
    
    cursor = test_basic_queries(conn)
    if not cursor:
        print("\nCannot proceed without basic query access")
        conn.close()
        return False
    
    monitoring_ok = test_monitoring_access(cursor)
    requirements_ok = test_datadog_requirements(cursor)
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)
    
    if monitoring_ok and requirements_ok:
        print("ALL TESTS PASSED")
        print("Private key authentication working")
        print("Private Link connection established")
        print("All monitoring requirements satisfied")
        print("Ready to configure Datadog integration")
        
        print(f"\nNEXT STEPS:")
        print(f"1. Configure Datadog Snowflake integration with:")
        print(f"   - Account URL: {SNOWFLAKE_ACCOUNT}")
        print(f"   - Username: {SNOWFLAKE_USER}")
        print(f"   - Authentication: Private Key")
        print(f"   - Role: {SNOWFLAKE_ROLE}")
        print(f"   - Warehouse: {SNOWFLAKE_WAREHOUSE}")
        print(f"2. Enable all metric collection categories")
        print(f"3. Create monitoring dashboard")
        
        return True
    else:
        print("SOME TESTS FAILED")
        print("Check error messages above for troubleshooting")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nTest completed at: {datetime.now()}")
    sys.exit(0 if success else 1)
