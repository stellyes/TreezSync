#!/usr/bin/env python3
"""
Test script for Treez + METRC sync
Run locally to test API connections and data extraction before deploying
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

# Load environment variables from .env file if present
def load_env():
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value

load_env()

# Import the lambda function module
from lambda_function import TreezAPIClient, MetrcAPIClient, DatabaseClient, DataSync


def test_treez_connection():
    """Test Treez API connectivity"""
    print("\n" + "="*50)
    print("Testing Treez API Connection")
    print("="*50)

    api_key = os.environ.get('TREEZ_API_KEY')
    dispensary = os.environ.get('TREEZ_DISPENSARY', 'barbarycoast')

    if not api_key:
        print("❌ TREEZ_API_KEY not set")
        return False

    print(f"Dispensary: {dispensary}")
    print(f"API Key: {api_key[:10]}...")

    client = TreezAPIClient(api_key, dispensary)

    # Test endpoints
    endpoints = [
        ('tickets', {'limit': 1}),
        ('customers', {'limit': 1}),
        ('products', {'limit': 1}),
    ]

    for endpoint, params in endpoints:
        print(f"\nTesting /{endpoint}...")
        result = client.get(endpoint, params)
        if 'error' in result:
            print(f"  ❌ Error: {result['error']}")
        else:
            count = len(result.get('data', result.get('tickets', result.get('customers', result.get('products', [])))))
            print(f"  ✅ Success - {count} record(s) returned")

    return True


def test_metrc_connection():
    """Test METRC API connectivity"""
    print("\n" + "="*50)
    print("Testing METRC API Connection")
    print("="*50)

    api_key = os.environ.get('METRC_API_KEY')
    user_key = os.environ.get('METRC_USER_KEY')

    if not api_key or not user_key:
        print("❌ METRC_API_KEY or METRC_USER_KEY not set")
        print("   Skipping METRC tests")
        return False

    print(f"API Key: {api_key[:10]}...")
    print(f"User Key: {user_key[:10]}...")

    client = MetrcAPIClient(api_key, user_key)

    # Test endpoints
    today = datetime.now().strftime('%Y-%m-%d')
    endpoints = [
        (f'packages/v2/active?licenseNumber=C10-0000000-LIC&lastModifiedStart={today}', 'Active Packages'),
        (f'sales/v2/receipts?licenseNumber=C10-0000000-LIC&salesDateStart={today}', 'Sales Receipts'),
    ]

    for endpoint, name in endpoints:
        print(f"\nTesting {name}...")
        result = client.get(endpoint)
        if 'error' in result:
            print(f"  ❌ Error: {result['error']}")
        else:
            count = len(result) if isinstance(result, list) else 1
            print(f"  ✅ Success - {count} record(s) returned")

    return True


def test_database_connection():
    """Test Aurora PostgreSQL connectivity"""
    print("\n" + "="*50)
    print("Testing Aurora PostgreSQL Connection")
    print("="*50)

    db_url = os.environ.get('DATABASE_URL')

    if not db_url:
        print("❌ DATABASE_URL not set")
        return False

    # Parse URL for display (hide password)
    parsed = urlparse(db_url)
    safe_url = f"{parsed.scheme}://{parsed.username}:***@{parsed.hostname}:{parsed.port}{parsed.path}"
    print(f"Database: {safe_url}")

    try:
        client = DatabaseClient(db_url)

        # Test connection with simple query
        with client.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM sales_records")
                sales_count = cursor.fetchone()[0]
                print(f"  ✅ Connected successfully")
                print(f"  📊 Sales records: {sales_count:,}")

                cursor.execute("SELECT COUNT(*) FROM customers")
                customer_count = cursor.fetchone()[0]
                print(f"  👥 Customers: {customer_count:,}")

                cursor.execute("SELECT COUNT(*) FROM invoices")
                invoice_count = cursor.fetchone()[0]
                print(f"  📄 Invoices: {invoice_count:,}")

        return True
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        return False


def test_treez_data_extraction(days_back=1):
    """Test Treez data extraction"""
    print("\n" + "="*50)
    print(f"Testing Treez Data Extraction (last {days_back} days)")
    print("="*50)

    api_key = os.environ.get('TREEZ_API_KEY')
    dispensary = os.environ.get('TREEZ_DISPENSARY', 'barbarycoast')

    if not api_key:
        print("❌ TREEZ_API_KEY not set")
        return

    client = TreezAPIClient(api_key, dispensary)

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # Fetch tickets
    print("\nFetching tickets...")
    params = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'limit': 100
    }
    result = client.get('tickets', params)

    if 'error' in result:
        print(f"  ❌ Error: {result['error']}")
    else:
        tickets = result.get('data', result.get('tickets', []))
        print(f"  ✅ Found {len(tickets)} tickets")

        if tickets:
            # Sample ticket analysis
            print("\n  Sample ticket structure:")
            sample = tickets[0]
            for key in list(sample.keys())[:10]:
                value = sample[key]
                if isinstance(value, dict):
                    value = f"{{...}} ({len(value)} keys)"
                elif isinstance(value, list):
                    value = f"[...] ({len(value)} items)"
                elif isinstance(value, str) and len(value) > 50:
                    value = value[:50] + "..."
                print(f"    - {key}: {value}")

    # Fetch customers
    print("\nFetching customers...")
    result = client.get('customers', {'limit': 100})

    if 'error' in result:
        print(f"  ❌ Error: {result['error']}")
    else:
        customers = result.get('data', result.get('customers', []))
        print(f"  ✅ Found {len(customers)} customers")


def test_full_sync(dry_run=True):
    """Test full sync process"""
    print("\n" + "="*50)
    print(f"Testing Full Sync (dry_run={dry_run})")
    print("="*50)

    # Check all required env vars
    required_vars = ['TREEZ_API_KEY', 'DATABASE_URL']
    missing = [v for v in required_vars if not os.environ.get(v)]

    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        return

    db_url = os.environ.get('DATABASE_URL')
    treez_config = {
        'api_key': os.environ.get('TREEZ_API_KEY'),
        'client_id': os.environ.get('TREEZ_CLIENT_ID', 'chapters_sync'),
        'dispensary': os.environ.get('TREEZ_DISPENSARY', 'barbarycoast')
    }
    metrc_config = {
        'api_key': os.environ.get('METRC_API_KEY'),
        'user_key': os.environ.get('METRC_USER_KEY')
    } if os.environ.get('METRC_API_KEY') else None

    sync = DataSync(db_url, treez_config, metrc_config)

    if dry_run:
        print("\n🔍 Dry run - would sync:")
        print("  - Treez tickets (sales data)")
        print("  - Treez customers")
        print("  - Treez invoices")
        if metrc_config:
            print("  - METRC packages")
            print("  - METRC transfers")
        print("\nRun with --execute to perform actual sync")
    else:
        print("\n🚀 Executing full sync...")
        result = sync.run_full_sync()
        print(f"\n📊 Sync Result:")
        print(json.dumps(result, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description='Test Treez + METRC sync')
    parser.add_argument('--treez', action='store_true', help='Test Treez API connection')
    parser.add_argument('--metrc', action='store_true', help='Test METRC API connection')
    parser.add_argument('--database', action='store_true', help='Test database connection')
    parser.add_argument('--extract', action='store_true', help='Test data extraction')
    parser.add_argument('--days', type=int, default=1, help='Days of data to extract (default: 1)')
    parser.add_argument('--sync', action='store_true', help='Test full sync (dry run)')
    parser.add_argument('--execute', action='store_true', help='Execute actual sync (use with --sync)')
    parser.add_argument('--all', action='store_true', help='Run all tests')

    args = parser.parse_args()

    # If no arguments, show help
    if not any([args.treez, args.metrc, args.database, args.extract, args.sync, args.all]):
        parser.print_help()
        print("\n\nExamples:")
        print("  python test_sync.py --all              # Run all connection tests")
        print("  python test_sync.py --treez            # Test Treez API only")
        print("  python test_sync.py --extract --days 7 # Extract last 7 days of data")
        print("  python test_sync.py --sync             # Dry run full sync")
        print("  python test_sync.py --sync --execute   # Execute actual sync")
        return

    print("="*50)
    print("Treez + METRC Sync Test Suite")
    print("="*50)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if args.all or args.treez:
        test_treez_connection()

    if args.all or args.metrc:
        test_metrc_connection()

    if args.all or args.database:
        test_database_connection()

    if args.extract:
        test_treez_data_extraction(args.days)

    if args.sync:
        test_full_sync(dry_run=not args.execute)

    print("\n" + "="*50)
    print("Tests Complete")
    print("="*50)


if __name__ == '__main__':
    main()
