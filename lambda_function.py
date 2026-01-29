"""
Treez + METRC Data Sync Lambda Function
Extracts data from Treez API (retail operations) and METRC API (compliance/inventory)
and loads into Aurora PostgreSQL with proper normalization and relationship linking.

Features:
- Product catalog sync with brand resolution
- Vendor/Distributor normalization with fuzzy matching
- Brand normalization with alias resolution
- METRC package and transfer tracking
- Data discrepancy flagging for manual review

Runs daily at 11:00 PM PST via EventBridge
"""

import os
import json
import logging
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any, Tuple
import urllib.request
import urllib.parse
import ssl
import base64
import re

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secrets():
    """
    Load secrets from AWS Secrets Manager (Lambda) or environment variables (local)
    Returns dict with TREEZ_API_KEY, METRC_VENDOR_KEY, etc.
    """
    # First, check if running in Lambda with Secrets Manager
    secret_name = os.environ.get('SECRET_NAME', 'treez-sync/api-keys')
    region = os.environ.get('AWS_REGION', 'us-west-1')

    # Try to get from Secrets Manager if we're in Lambda
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        try:
            import boto3
            client = boto3.client('secretsmanager', region_name=region)
            response = client.get_secret_value(SecretId=secret_name)
            secrets = json.loads(response['SecretString'])
            logger.info("Loaded secrets from AWS Secrets Manager")
            return secrets
        except Exception as e:
            logger.warning(f"Could not load from Secrets Manager: {e}, falling back to env vars")

    # Fall back to environment variables
    return {
        'TREEZ_API_KEY': os.environ.get('TREEZ_API_KEY'),
        'TREEZ_CLIENT_ID': os.environ.get('TREEZ_CLIENT_ID', 'chapters_sync'),
        'TREEZ_DISPENSARY': os.environ.get('TREEZ_DISPENSARY', 'barbarycoast'),
        'METRC_VENDOR_KEY': os.environ.get('METRC_VENDOR_KEY'),
        'METRC_USER_KEY': os.environ.get('METRC_USER_KEY'),
        'METRC_LICENSE_NUMBER': os.environ.get('METRC_LICENSE_NUMBER'),
        'DATABASE_URL': os.environ.get('DATABASE_URL')
    }


# Will be populated at runtime
SECRETS = None

# API base URLs (will be configured at runtime)
METRC_BASE_URL = "https://api-ca.metrc.com"

# SSL context for API calls
ssl_context = ssl.create_default_context()


# ============================================
# UTILITY FUNCTIONS
# ============================================

def normalize_string(s: str) -> str:
    """Normalize a string for comparison (lowercase, strip whitespace, remove special chars)"""
    if not s:
        return ''
    return re.sub(r'[^a-z0-9]', '', s.lower().strip())


def calculate_similarity(str1: str, str2: str) -> float:
    """Calculate a simple similarity score between two strings (0-1)"""
    s1 = normalize_string(str1)
    s2 = normalize_string(str2)

    if s1 == s2:
        return 1.0

    if not s1 or not s2:
        return 0.0

    max_len = max(len(s1), len(s2))
    common_chars = sum(1 for c in s1 if c in s2)

    return round(common_chars / max_len, 4)


class TreezAPIClient:
    """Client for interacting with Treez API"""

    def __init__(self, api_key: str, client_id: str, dispensary: str):
        self.api_key = api_key
        self.client_id = client_id
        self.dispensary = dispensary
        self.base_url = f"https://api.treez.io/v2.0/dispensary/{dispensary}"
        self.access_token = None
        self.token_expires_at = None

    def _make_request(self, method: str, url: str, data: Optional[Dict] = None,
                      headers: Optional[Dict] = None) -> Dict:
        """Make HTTP request to Treez API"""
        if headers is None:
            headers = {}

        if self.access_token and 'Authorization' not in headers:
            headers['Authorization'] = f'Bearer {self.access_token}'

        if data and method == 'POST':
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
            encoded_data = urllib.parse.urlencode(data).encode('utf-8')
        else:
            encoded_data = None

        req = urllib.request.Request(url, data=encoded_data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, context=ssl_context, timeout=30) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            logger.error(f"HTTP Error {e.code}: {error_body}")
            raise
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            raise

    def authenticate(self) -> bool:
        """Get access token from Treez API"""
        url = f"{self.base_url}/config/api/gettokens"
        data = {
            'apikey': self.api_key,
            'client_id': self.client_id
        }

        try:
            response = self._make_request('POST', url, data)
            if response.get('resultCode') == 'SUCCESS':
                self.access_token = response.get('access_token')
                expires_at = response.get('expires_at')
                if expires_at:
                    self.token_expires_at = datetime.fromisoformat(expires_at.replace('-0800', '-08:00').replace('-0700', '-07:00'))
                logger.info("Successfully authenticated with Treez API")
                return True
            else:
                logger.error(f"Authentication failed: {response}")
                return False
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def ensure_authenticated(self):
        """Ensure we have a valid access token"""
        if not self.access_token:
            if not self.authenticate():
                raise Exception("Failed to authenticate with Treez API")
        elif self.token_expires_at:
            # Compare as naive datetimes (remove timezone info)
            now = datetime.now()
            expires = self.token_expires_at.replace(tzinfo=None) if self.token_expires_at.tzinfo else self.token_expires_at
            if now >= expires:
                if not self.authenticate():
                    raise Exception("Failed to authenticate with Treez API")

    # ==========================================
    # TICKET (SALES) API
    # ==========================================

    def get_tickets_by_close_date(self, date: str, page: int = 0, page_size: int = 50) -> Dict:
        """Get tickets closed on a specific date (YYYY-MM-DD)"""
        self.ensure_authenticated()
        url = f"{self.base_url}/ticket/closedate/{date}/page/{page}/pagesize/{page_size}"
        return self._make_request('GET', url)

    def get_tickets_by_last_updated(self, after_datetime: str, page: int = 0, page_size: int = 50) -> Dict:
        """Get tickets updated after a specific datetime (ISO format)"""
        self.ensure_authenticated()
        url = f"{self.base_url}/ticket/lastUpdated/after/{after_datetime}/page/{page}/pagesize/{page_size}"
        return self._make_request('GET', url)

    # ==========================================
    # CUSTOMER API
    # ==========================================

    def get_customers_by_last_updated(self, after_datetime: str, page: int = 0, page_size: int = 50) -> Dict:
        """Get customers updated after a specific datetime"""
        self.ensure_authenticated()
        url = f"{self.base_url}/customer/lastUpdated/after/{after_datetime}?page={page}&page_size={page_size}"
        return self._make_request('GET', url)

    # ==========================================
    # PRODUCT API
    # ==========================================

    def get_products(self, page: int = 0, page_size: int = 1000, active: bool = True) -> Dict:
        """Get all products with pagination"""
        self.ensure_authenticated()
        url = f"{self.base_url}/product/list?page={page}&page_size={page_size}&active={str(active).lower()}"
        return self._make_request('GET', url)

    def get_products_by_last_updated(self, after_datetime: str, page: int = 0, page_size: int = 1000) -> Dict:
        """Get products updated after a specific datetime"""
        self.ensure_authenticated()
        url = f"{self.base_url}/product/lastUpdated/after/{after_datetime}?page={page}&page_size={page_size}"
        return self._make_request('GET', url)

    # ==========================================
    # INVOICE API
    # ==========================================

    def get_invoices_by_date_range(self, start_date: str, end_date: str, page: int = 0, page_size: int = 50) -> Dict:
        """Get invoices updated within date range (YYYY-MM-DD)"""
        self.ensure_authenticated()
        url = f"{self.base_url}/invoice/updated/from/{start_date}/to/{end_date}?page={page}&page_size={page_size}"
        return self._make_request('GET', url)

    # ==========================================
    # PAGINATION HELPERS
    # ==========================================

    def fetch_all_pages(self, fetch_func, *args, **kwargs) -> List[Dict]:
        """Fetch all pages of a paginated endpoint"""
        all_data = []
        page = 0
        page_size = kwargs.pop('page_size', 50)

        while True:
            try:
                response = fetch_func(*args, page=page, page_size=page_size, **kwargs)
                data = response.get('data', response.get('customers', response.get('products', [])))

                if isinstance(data, list):
                    if not data:
                        break
                    all_data.extend(data)
                    if len(data) < page_size:
                        break
                else:
                    if data:
                        all_data.append(data)
                    break

                page += 1
                if page > 1000:
                    logger.warning("Hit pagination safety limit")
                    break

            except Exception as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break

        return all_data


class MetrcAPIClient:
    """Client for interacting with METRC API (California)"""

    def __init__(self, vendor_key: str, user_key: str, license_number: str):
        self.vendor_key = vendor_key
        self.user_key = user_key
        self.license_number = license_number
        self.base_url = "https://api-ca.metrc.com"

    def _get_auth_header(self) -> str:
        """Generate Basic Auth header for METRC API"""
        credentials = f"{self.vendor_key}:{self.user_key}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make HTTP request to METRC API"""
        url = f"{self.base_url}{endpoint}"

        if params:
            # Add license number to all requests
            params['licenseNumber'] = self.license_number
            query_string = urllib.parse.urlencode(params)
            url = f"{url}?{query_string}"
        else:
            url = f"{url}?licenseNumber={self.license_number}"

        headers = {
            'Authorization': self._get_auth_header(),
            'Content-Type': 'application/json'
        }

        req = urllib.request.Request(url, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, context=ssl_context, timeout=60) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            logger.error(f"METRC HTTP Error {e.code}: {error_body}")
            raise
        except Exception as e:
            logger.error(f"METRC Request error: {str(e)}")
            raise

    # ==========================================
    # PACKAGES API - Inventory tracking
    # ==========================================

    def get_active_packages(self, last_modified_start: Optional[str] = None,
                            last_modified_end: Optional[str] = None) -> List[Dict]:
        """Get active packages (current inventory)"""
        params = {}
        if last_modified_start:
            params['lastModifiedStart'] = last_modified_start
        if last_modified_end:
            params['lastModifiedEnd'] = last_modified_end

        return self._make_request('GET', '/packages/v2/active', params)

    def get_packages_on_hold(self) -> List[Dict]:
        """Get packages on hold (quarantined inventory)"""
        return self._make_request('GET', '/packages/v2/onhold')

    def get_packages_in_transit(self) -> List[Dict]:
        """Get packages currently in transit"""
        return self._make_request('GET', '/packages/v2/intransit')

    def get_package_by_label(self, label: str) -> Dict:
        """Get a specific package by its METRC tag label"""
        return self._make_request('GET', f'/packages/v2/{label}')

    # ==========================================
    # TRANSFERS API - Incoming shipments
    # ==========================================

    def get_incoming_transfers(self, last_modified_start: Optional[str] = None,
                                last_modified_end: Optional[str] = None) -> List[Dict]:
        """Get incoming transfers (vendor shipments)"""
        params = {}
        if last_modified_start:
            params['lastModifiedStart'] = last_modified_start
        if last_modified_end:
            params['lastModifiedEnd'] = last_modified_end

        return self._make_request('GET', '/transfers/v2/incoming', params)

    def get_outgoing_transfers(self, last_modified_start: Optional[str] = None,
                                last_modified_end: Optional[str] = None) -> List[Dict]:
        """Get outgoing transfers"""
        params = {}
        if last_modified_start:
            params['lastModifiedStart'] = last_modified_start
        if last_modified_end:
            params['lastModifiedEnd'] = last_modified_end

        return self._make_request('GET', '/transfers/v2/outgoing', params)

    def get_transfer_deliveries(self, transfer_id: int) -> List[Dict]:
        """Get deliveries for a specific transfer"""
        return self._make_request('GET', f'/transfers/v2/{transfer_id}/deliveries')

    def get_delivery_packages(self, delivery_id: int) -> List[Dict]:
        """Get packages in a specific delivery"""
        return self._make_request('GET', f'/transfers/v2/deliveries/{delivery_id}/packages')

    # ==========================================
    # ITEMS API - Product catalog
    # ==========================================

    def get_active_items(self) -> List[Dict]:
        """Get active items (product catalog)"""
        return self._make_request('GET', '/items/v2/active')

    def get_item_categories(self) -> List[Dict]:
        """Get item categories"""
        return self._make_request('GET', '/items/v2/categories')

    def get_item_brands(self) -> List[Dict]:
        """Get registered brands"""
        return self._make_request('GET', '/items/v2/brands')

    # ==========================================
    # SALES API - Retail transactions
    # ==========================================

    def get_sales_receipts(self, sales_date_start: str, sales_date_end: str) -> List[Dict]:
        """Get sales receipts for date range (YYYY-MM-DD)"""
        params = {
            'salesDateStart': sales_date_start,
            'salesDateEnd': sales_date_end
        }
        return self._make_request('GET', '/sales/v2/receipts', params)

    # ==========================================
    # LAB TESTS API - Quality/compliance data
    # ==========================================

    def get_lab_test_states(self) -> List[Dict]:
        """Get lab test states"""
        return self._make_request('GET', '/labtests/v2/states')

    # ==========================================
    # FACILITIES API
    # ==========================================

    def get_facilities(self) -> List[Dict]:
        """Get licensed facilities"""
        # Facilities endpoint doesn't require license number in params
        url = f"{self.base_url}/facilities/v2"
        headers = {
            'Authorization': self._get_auth_header(),
            'Content-Type': 'application/json'
        }
        req = urllib.request.Request(url, headers=headers, method='GET')

        try:
            with urllib.request.urlopen(req, context=ssl_context, timeout=60) as response:
                return json.loads(response.read().decode('utf-8'))
        except Exception as e:
            logger.error(f"METRC Facilities error: {str(e)}")
            return []


class DatabaseClient:
    """Client for Aurora PostgreSQL operations"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.conn = None

    def connect(self):
        """Establish database connection"""
        try:
            import pg8000
            from urllib.parse import urlparse, unquote

            parsed = urlparse(self.database_url)
            self.conn = pg8000.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                user=parsed.username,
                password=unquote(parsed.password) if parsed.password else None,
                database=parsed.path[1:],
                ssl_context=ssl_context
            )
            logger.info("Connected to Aurora PostgreSQL via pg8000")
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def execute(self, query: str, params: tuple = None):
        """Execute a query"""
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor

    def fetchone(self, query: str, params: tuple = None):
        """Execute a query and fetch one result"""
        cursor = self.execute(query, params)
        return cursor.fetchone()

    def fetchall(self, query: str, params: tuple = None):
        """Execute a query and fetch all results"""
        cursor = self.execute(query, params)
        return cursor.fetchall()

    def commit(self):
        """Commit transaction"""
        self.conn.commit()

    def rollback(self):
        """Rollback transaction"""
        self.conn.rollback()


class DataSync:
    """Main sync orchestrator for Treez + METRC data with normalization and flagging"""

    def __init__(self, treez_client: Optional[TreezAPIClient],
                 metrc_client: Optional[MetrcAPIClient],
                 db_client: DatabaseClient,
                 store_name: str = 'Barbary Coast',
                 storefront_id: str = None):
        self.treez = treez_client
        self.metrc = metrc_client
        self.db = db_client
        self.store_name = store_name
        self.storefront_id = storefront_id
        self.sync_stats = {
            'tickets_synced': 0,
            'customers_synced': 0,
            'products_synced': 0,
            'invoices_synced': 0,
            'packages_synced': 0,
            'transfers_synced': 0,
            'flags_created': 0,
            'vendors_resolved': 0,
            'brands_resolved': 0,
            'errors': []
        }

        # Cache for vendor and brand lookups
        self._vendor_cache = {}
        self._brand_cache = {}
        self._load_normalization_caches()

    def _load_normalization_caches(self):
        """Load vendor and brand normalization caches from database"""
        try:
            # Load vendor aliases
            rows = self.db.fetchall("""
                SELECT va.alias_name, v.id, v.canonical_name
                FROM vendor_aliases va
                JOIN vendors v ON va.vendor_id = v.id
            """)
            for row in rows:
                self._vendor_cache[normalize_string(row[0])] = {
                    'id': row[1],
                    'canonical_name': row[2]
                }

            # Also add canonical names
            rows = self.db.fetchall("SELECT id, canonical_name FROM vendors")
            for row in rows:
                self._vendor_cache[normalize_string(row[1])] = {
                    'id': row[0],
                    'canonical_name': row[1]
                }

            # Load brand aliases
            rows = self.db.fetchall("""
                SELECT ba.alias_name, b.id, b.canonical_name
                FROM brand_aliases ba
                JOIN canonical_brands b ON ba.brand_id = b.id
            """)
            for row in rows:
                self._brand_cache[normalize_string(row[0])] = {
                    'id': row[1],
                    'canonical_name': row[2]
                }

            # Also add canonical names
            rows = self.db.fetchall("SELECT id, canonical_name FROM canonical_brands")
            for row in rows:
                self._brand_cache[normalize_string(row[1])] = {
                    'id': row[0],
                    'canonical_name': row[1]
                }

            logger.info(f"Loaded {len(self._vendor_cache)} vendor mappings, {len(self._brand_cache)} brand mappings")

        except Exception as e:
            logger.warning(f"Could not load normalization caches: {e}")

    def resolve_vendor(self, vendor_name: str) -> Tuple[Optional[str], Optional[str], float]:
        """
        Resolve a vendor name to canonical vendor ID.
        Returns (vendor_id, canonical_name, similarity_score)
        """
        if not vendor_name:
            return None, None, 0.0

        normalized = normalize_string(vendor_name)

        # Check cache first
        if normalized in self._vendor_cache:
            self.sync_stats['vendors_resolved'] += 1
            return self._vendor_cache[normalized]['id'], self._vendor_cache[normalized]['canonical_name'], 1.0

        # Try fuzzy matching
        best_match = None
        best_score = 0.0

        for key, value in self._vendor_cache.items():
            score = calculate_similarity(vendor_name, value['canonical_name'])
            if score > best_score:
                best_score = score
                best_match = value

        if best_match and best_score >= 0.8:
            self.sync_stats['vendors_resolved'] += 1
            return best_match['id'], best_match['canonical_name'], best_score

        return None, None, best_score

    def resolve_brand(self, brand_name: str) -> Tuple[Optional[str], Optional[str], float]:
        """
        Resolve a brand name to canonical brand ID.
        Returns (brand_id, canonical_name, similarity_score)
        """
        if not brand_name:
            return None, None, 0.0

        normalized = normalize_string(brand_name)

        # Check cache first
        if normalized in self._brand_cache:
            self.sync_stats['brands_resolved'] += 1
            return self._brand_cache[normalized]['id'], self._brand_cache[normalized]['canonical_name'], 1.0

        # Try fuzzy matching
        best_match = None
        best_score = 0.0

        for key, value in self._brand_cache.items():
            score = calculate_similarity(brand_name, value['canonical_name'])
            if score > best_score:
                best_score = score
                best_match = value

        if best_match and best_score >= 0.8:
            self.sync_stats['brands_resolved'] += 1
            return best_match['id'], best_match['canonical_name'], best_score

        return None, None, best_score

    def create_data_flag(self, flag_type: str, severity: str, source_table: str,
                         source_record_id: str, title: str, description: str,
                         raw_value: str = None, suggested_match: str = None,
                         suggested_match_id: str = None, similarity_score: float = None,
                         metadata: Dict = None):
        """Create a data discrepancy flag for manual review"""
        try:
            self.db.execute("""
                INSERT INTO data_flags (
                    id, storefront_id, flag_type, severity, status, source_table,
                    source_record_id, title, description, raw_value, suggested_match,
                    suggested_match_id, similarity_score, metadata, created_at, updated_at
                )
                VALUES (
                    gen_random_uuid(), %s, %s, %s, 'pending', %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, NOW(), NOW()
                )
                ON CONFLICT DO NOTHING
            """, (
                self.storefront_id,
                flag_type,
                severity,
                source_table,
                source_record_id,
                title,
                description,
                raw_value,
                suggested_match,
                suggested_match_id,
                similarity_score,
                json.dumps(metadata or {})
            ))
            self.sync_stats['flags_created'] += 1
            logger.info(f"Created data flag: {title}")
        except Exception as e:
            logger.error(f"Error creating data flag: {e}")

    # ==========================================
    # TREEZ SYNC METHODS
    # ==========================================

    def sync_products(self, since_datetime: str = None):
        """Sync product catalog from Treez API"""
        if not self.treez:
            logger.warning("Treez client not configured, skipping product sync")
            return

        logger.info("Syncing product catalog from Treez")

        try:
            if since_datetime:
                products = self.treez.fetch_all_pages(
                    self.treez.get_products_by_last_updated,
                    since_datetime,
                    page_size=1000
                )
            else:
                products = self.treez.fetch_all_pages(
                    self.treez.get_products,
                    page_size=1000
                )

            logger.info(f"Found {len(products)} products to sync")

            for product in products:
                try:
                    self._upsert_product(product)
                    self.sync_stats['products_synced'] += 1
                except Exception as e:
                    logger.error(f"Error syncing product {product.get('product_id')}: {str(e)}")
                    self.sync_stats['errors'].append(f"Product {product.get('product_id')}: {str(e)}")

            self.db.commit()
            logger.info(f"Successfully synced {self.sync_stats['products_synced']} products")

        except Exception as e:
            logger.error(f"Error syncing products: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"Product sync: {str(e)}")

    def _upsert_product(self, product: Dict):
        """Insert or update a product with brand resolution"""
        product_id = str(product.get('product_id') or product.get('id'))
        brand_name = product.get('brand', '')
        product_name = product.get('name', '')
        product_type = product.get('product_type') or product.get('category_type')
        product_subtype = product.get('product_subtype') or product.get('subcategory')

        # Resolve brand
        brand_id, canonical_brand, brand_score = self.resolve_brand(brand_name)

        # If brand not found but we have a name, create a flag
        if not brand_id and brand_name:
            self.create_data_flag(
                flag_type='brand_mismatch',
                severity='medium' if brand_score < 0.5 else 'low',
                source_table='products',
                source_record_id=product_id,
                title=f'Unknown brand: {brand_name}',
                description=f'Product "{product_name}" has brand "{brand_name}" which could not be resolved.',
                raw_value=brand_name,
                similarity_score=brand_score,
                metadata={'product_type': product_type}
            )

        # Extract numeric values
        retail_price = product.get('retail_price') or product.get('price')
        wholesale_price = product.get('wholesale_price') or product.get('cost')
        quantity = product.get('quantity_on_hand') or product.get('quantity') or 0
        thc = product.get('thc_content') or product.get('thc')
        cbd = product.get('cbd_content') or product.get('cbd')

        self.db.execute("""
            INSERT INTO products (
                id, treez_product_id, storefront_id, brand_id, original_brand_name,
                product_name, product_type, product_subtype, category, strain,
                unit_size, thc_content, cbd_content, retail_price, wholesale_price,
                quantity_on_hand, is_active, last_synced_at, created_at, updated_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, NOW(), NOW(), NOW()
            )
            ON CONFLICT (treez_product_id)
            DO UPDATE SET
                brand_id = COALESCE(EXCLUDED.brand_id, products.brand_id),
                original_brand_name = EXCLUDED.original_brand_name,
                product_name = EXCLUDED.product_name,
                product_type = COALESCE(EXCLUDED.product_type, products.product_type),
                product_subtype = COALESCE(EXCLUDED.product_subtype, products.product_subtype),
                category = COALESCE(EXCLUDED.category, products.category),
                strain = COALESCE(EXCLUDED.strain, products.strain),
                unit_size = COALESCE(EXCLUDED.unit_size, products.unit_size),
                thc_content = COALESCE(EXCLUDED.thc_content, products.thc_content),
                cbd_content = COALESCE(EXCLUDED.cbd_content, products.cbd_content),
                retail_price = COALESCE(EXCLUDED.retail_price, products.retail_price),
                wholesale_price = COALESCE(EXCLUDED.wholesale_price, products.wholesale_price),
                quantity_on_hand = EXCLUDED.quantity_on_hand,
                is_active = EXCLUDED.is_active,
                last_synced_at = NOW(),
                updated_at = NOW()
        """, (
            product_id,
            self.storefront_id,
            brand_id,
            brand_name,
            product_name,
            product_type,
            product_subtype,
            product.get('category'),
            product.get('strain'),
            product.get('unit_size'),
            float(thc) if thc else None,
            float(cbd) if cbd else None,
            float(retail_price) if retail_price else None,
            float(wholesale_price) if wholesale_price else None,
            int(quantity),
            product.get('is_active', True)
        ))

    def sync_tickets(self, date: str):
        """Sync tickets (sales) for a specific date"""
        if not self.treez:
            logger.warning("Treez client not configured, skipping ticket sync")
            return

        logger.info(f"Syncing tickets for {date}")

        try:
            tickets = self.treez.fetch_all_pages(
                self.treez.get_tickets_by_close_date,
                date,
                page_size=50
            )

            logger.info(f"Found {len(tickets)} tickets to sync")

            # Aggregate daily totals
            daily_stats = {
                'tickets_count': 0,
                'units_sold': 0,
                'customers': set(),
                'gross_sales': Decimal('0'),
                'discounts': Decimal('0'),
                'net_sales': Decimal('0'),
                'taxes': Decimal('0')
            }

            for ticket in tickets:
                try:
                    # Aggregate stats
                    daily_stats['tickets_count'] += 1
                    line_items = ticket.get('line_items', [])
                    daily_stats['units_sold'] += sum(int(item.get('quantity', 1) or 1) for item in line_items)

                    customer_id = ticket.get('customer_id')
                    if customer_id:
                        daily_stats['customers'].add(customer_id)

                    sub_total = Decimal(str(ticket.get('sub_total', 0) or 0))
                    discount_total = Decimal(str(ticket.get('discount_total', 0) or 0))
                    tax_total = Decimal(str(ticket.get('tax_total', 0) or 0))

                    daily_stats['gross_sales'] += sub_total + discount_total
                    daily_stats['discounts'] += discount_total
                    daily_stats['net_sales'] += sub_total
                    daily_stats['taxes'] += tax_total

                    self.sync_stats['tickets_synced'] += 1

                except Exception as e:
                    logger.error(f"Error processing ticket {ticket.get('ticket_id')}: {str(e)}")
                    self.sync_stats['errors'].append(f"Ticket {ticket.get('ticket_id')}: {str(e)}")

            # Upsert daily sales record
            if daily_stats['tickets_count'] > 0:
                self._upsert_daily_sales(date, daily_stats)

            self.db.commit()
            logger.info(f"Successfully synced {self.sync_stats['tickets_synced']} tickets")

        except Exception as e:
            logger.error(f"Error syncing tickets: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"Ticket sync: {str(e)}")

    def _upsert_daily_sales(self, date: str, stats: Dict):
        """Insert or update daily sales record"""
        self.db.execute("""
            INSERT INTO sales_records (
                id, store_id, store_name, date, tickets_count, units_sold,
                customers_count, gross_sales, discounts, net_sales, taxes, created_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s::date, %s, %s,
                %s, %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (store_id, date)
            DO UPDATE SET
                tickets_count = EXCLUDED.tickets_count,
                units_sold = EXCLUDED.units_sold,
                customers_count = EXCLUDED.customers_count,
                gross_sales = EXCLUDED.gross_sales,
                discounts = EXCLUDED.discounts,
                net_sales = EXCLUDED.net_sales,
                taxes = EXCLUDED.taxes
        """, (
            'barbary_coast',
            self.store_name,
            date,
            stats['tickets_count'],
            stats['units_sold'],
            len(stats['customers']),
            float(stats['gross_sales']),
            float(stats['discounts']),
            float(stats['net_sales']),
            float(stats['taxes'])
        ))

    def sync_customers(self, since_datetime: str):
        """Sync customers updated since a specific datetime"""
        if not self.treez:
            logger.warning("Treez client not configured, skipping customer sync")
            return

        logger.info(f"Syncing customers updated since {since_datetime}")

        try:
            customers = self.treez.fetch_all_pages(
                self.treez.get_customers_by_last_updated,
                since_datetime,
                page_size=50
            )

            logger.info(f"Found {len(customers)} customers to sync")

            for customer in customers:
                try:
                    self._upsert_customer(customer)
                    self.sync_stats['customers_synced'] += 1
                except Exception as e:
                    logger.error(f"Error syncing customer {customer.get('customer_id')}: {str(e)}")
                    self.sync_stats['errors'].append(f"Customer {customer.get('customer_id')}: {str(e)}")

            self.db.commit()
            logger.info(f"Successfully synced {self.sync_stats['customers_synced']} customers")

        except Exception as e:
            logger.error(f"Error syncing customers: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"Customer sync: {str(e)}")

    def _upsert_customer(self, customer: Dict):
        """Insert or update a customer"""
        customer_id = customer.get('customer_id')
        first_name = customer.get('first_name', '')
        last_name = customer.get('last_name', '')
        name = f"{first_name} {last_name}".strip() or None

        dob = customer.get('date_of_birth')
        signup_date = customer.get('signup_date')
        last_visit = customer.get('last_visit_date')

        age = None
        if dob:
            try:
                dob_date = datetime.strptime(dob.split('T')[0], '%Y-%m-%d')
                age = (datetime.now() - dob_date).days // 365
            except:
                pass

        lifetime_visits = customer.get('lifetime_visits', 0) or 0
        lifetime_net_sales = Decimal(str(customer.get('lifetime_net_sales', 0) or 0))

        self.db.execute("""
            INSERT INTO customers (
                id, store_name, customer_id, name, date_of_birth, age,
                lifetime_visits, lifetime_net_sales, signup_date, last_visit_date,
                created_at, updated_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s::date, %s,
                %s, %s, %s::date, %s::date,
                NOW(), NOW()
            )
            ON CONFLICT (store_name, customer_id)
            DO UPDATE SET
                name = COALESCE(EXCLUDED.name, customers.name),
                date_of_birth = COALESCE(EXCLUDED.date_of_birth, customers.date_of_birth),
                age = COALESCE(EXCLUDED.age, customers.age),
                lifetime_visits = EXCLUDED.lifetime_visits,
                lifetime_net_sales = EXCLUDED.lifetime_net_sales,
                last_visit_date = COALESCE(EXCLUDED.last_visit_date, customers.last_visit_date),
                updated_at = NOW()
        """, (
            self.store_name,
            customer_id,
            name,
            dob.split('T')[0] if dob and 'T' in dob else dob,
            age,
            lifetime_visits,
            float(lifetime_net_sales),
            signup_date.split('T')[0] if signup_date and 'T' in signup_date else signup_date,
            last_visit.split('T')[0] if last_visit and 'T' in last_visit else last_visit
        ))

    def sync_invoices(self, start_date: str, end_date: str):
        """Sync invoices within a date range with vendor normalization"""
        if not self.treez:
            logger.warning("Treez client not configured, skipping invoice sync")
            return

        logger.info(f"Syncing invoices from {start_date} to {end_date}")

        try:
            invoices = self.treez.fetch_all_pages(
                self.treez.get_invoices_by_date_range,
                start_date,
                end_date,
                page_size=50
            )

            logger.info(f"Found {len(invoices)} invoices to sync")

            for invoice in invoices:
                try:
                    self._upsert_invoice(invoice)
                    self.sync_stats['invoices_synced'] += 1
                except Exception as e:
                    logger.error(f"Error syncing invoice {invoice.get('invoice_id')}: {str(e)}")
                    self.sync_stats['errors'].append(f"Invoice {invoice.get('invoice_id')}: {str(e)}")

            self.db.commit()
            logger.info(f"Successfully synced {self.sync_stats['invoices_synced']} invoices")

        except Exception as e:
            logger.error(f"Error syncing invoices: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"Invoice sync: {str(e)}")

    def _upsert_invoice(self, invoice: Dict):
        """Insert or update an invoice with vendor normalization"""
        invoice_id = str(invoice.get('invoice_id') or invoice.get('id'))
        invoice_number = invoice.get('invoice_number') or invoice.get('number')
        invoice_date = invoice.get('created_date') or invoice.get('invoice_date')

        # Get vendor name and try to normalize
        vendor_name = invoice.get('distributor', {}).get('name') if invoice.get('distributor') else None
        vendor_id, canonical_vendor, vendor_score = self.resolve_vendor(vendor_name)

        # If vendor not found but we have a name, create a flag
        if not vendor_id and vendor_name:
            self.create_data_flag(
                flag_type='vendor_mismatch',
                severity='medium' if vendor_score < 0.5 else 'low',
                source_table='invoices',
                source_record_id=invoice_id,
                title=f'Unknown vendor: {vendor_name}',
                description=f'Invoice {invoice_number or invoice_id} has vendor "{vendor_name}" which could not be resolved.',
                raw_value=vendor_name,
                similarity_score=vendor_score,
                metadata={'invoice_number': invoice_number, 'invoice_date': invoice_date}
            )

        total_cost = Decimal(str(invoice.get('total_cost', 0) or 0))
        line_items = invoice.get('line_items', [])

        cursor = self.db.execute("""
            INSERT INTO invoices (
                id, invoice_id, invoice_number, invoice_date, vendor_id, original_vendor_name,
                customer_name, total_cost, line_items_count, created_at, updated_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s::date, %s, %s,
                %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (invoice_id)
            DO UPDATE SET
                invoice_number = COALESCE(EXCLUDED.invoice_number, invoices.invoice_number),
                invoice_date = COALESCE(EXCLUDED.invoice_date, invoices.invoice_date),
                vendor_id = COALESCE(EXCLUDED.vendor_id, invoices.vendor_id),
                original_vendor_name = COALESCE(EXCLUDED.original_vendor_name, invoices.original_vendor_name),
                total_cost = EXCLUDED.total_cost,
                line_items_count = EXCLUDED.line_items_count,
                updated_at = NOW()
            RETURNING id
        """, (
            invoice_id,
            invoice_number,
            invoice_date.split('T')[0] if invoice_date and 'T' in invoice_date else invoice_date,
            vendor_id,
            vendor_name,
            self.store_name,
            float(total_cost),
            len(line_items)
        ))

        result = cursor.fetchone()
        db_invoice_id = result[0] if result else None

        if db_invoice_id and line_items:
            for idx, item in enumerate(line_items):
                self._upsert_invoice_line_item(db_invoice_id, invoice_id, idx + 1, item)

    def _upsert_invoice_line_item(self, db_invoice_id: str, treez_invoice_id: str, line_number: int, item: Dict):
        """Insert or update an invoice line item with brand normalization"""
        brand_name = item.get('brand', 'Unknown')
        product_name = item.get('product_name') or item.get('name')
        product_type = item.get('product_type') or item.get('category_type')

        # Resolve brand
        brand_id, canonical_brand, brand_score = self.resolve_brand(brand_name)

        # If brand not found but we have a name, create a flag
        if not brand_id and brand_name and brand_name != 'Unknown':
            self.create_data_flag(
                flag_type='brand_mismatch',
                severity='low',
                source_table='invoice_line_items',
                source_record_id=f"{treez_invoice_id}_{line_number}",
                title=f'Unknown brand in invoice: {brand_name}',
                description=f'Invoice line item has brand "{brand_name}" which could not be resolved.',
                raw_value=brand_name,
                similarity_score=brand_score,
                metadata={'product_name': product_name, 'product_type': product_type}
            )

        sku_units = int(item.get('quantity', 0) or 0)
        unit_cost = Decimal(str(item.get('unit_cost', 0) or 0))
        total_cost = Decimal(str(item.get('total_cost', 0) or 0))

        self.db.execute("""
            INSERT INTO invoice_line_items (
                id, invoice_id, line_number, brand_id, original_brand_name, product_name,
                product_type, sku_units, unit_cost, total_cost, created_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (invoice_id, line_number)
            DO UPDATE SET
                brand_id = COALESCE(EXCLUDED.brand_id, invoice_line_items.brand_id),
                original_brand_name = EXCLUDED.original_brand_name,
                product_name = EXCLUDED.product_name,
                product_type = EXCLUDED.product_type,
                sku_units = EXCLUDED.sku_units,
                unit_cost = EXCLUDED.unit_cost,
                total_cost = EXCLUDED.total_cost
        """, (
            db_invoice_id,
            line_number,
            brand_id,
            brand_name,
            product_name,
            product_type,
            sku_units,
            float(unit_cost),
            float(total_cost)
        ))

    # ==========================================
    # METRC SYNC METHODS
    # ==========================================

    def sync_metrc_packages(self, last_modified_start: Optional[str] = None):
        """Sync active packages from METRC (inventory)"""
        if not self.metrc:
            logger.warning("METRC client not configured, skipping package sync")
            return

        logger.info("Syncing METRC packages (inventory)")

        try:
            packages = self.metrc.get_active_packages(last_modified_start=last_modified_start)

            if isinstance(packages, list):
                logger.info(f"Found {len(packages)} packages to sync")

                for package in packages:
                    try:
                        self._upsert_metrc_package(package, 'active')
                        self.sync_stats['packages_synced'] += 1
                    except Exception as e:
                        logger.error(f"Error syncing package {package.get('Label')}: {str(e)}")
                        self.sync_stats['errors'].append(f"Package {package.get('Label')}: {str(e)}")

                self.db.commit()
                logger.info(f"Successfully synced {self.sync_stats['packages_synced']} packages")

            # Also sync on-hold packages
            try:
                on_hold = self.metrc.get_packages_on_hold()
                if isinstance(on_hold, list):
                    for package in on_hold:
                        self._upsert_metrc_package(package, 'on_hold')
                    self.db.commit()
            except Exception as e:
                logger.warning(f"Could not sync on-hold packages: {e}")

            # Also sync in-transit packages
            try:
                in_transit = self.metrc.get_packages_in_transit()
                if isinstance(in_transit, list):
                    for package in in_transit:
                        self._upsert_metrc_package(package, 'in_transit')
                    self.db.commit()
            except Exception as e:
                logger.warning(f"Could not sync in-transit packages: {e}")

        except Exception as e:
            logger.error(f"Error syncing METRC packages: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"METRC package sync: {str(e)}")

    def _upsert_metrc_package(self, package: Dict, status: str):
        """Insert or update a METRC package record"""
        label = package.get('Label')
        item = package.get('Item', {})
        item_name = item.get('Name') if isinstance(item, dict) else package.get('ItemName')
        product_category = item.get('ProductCategoryName') if isinstance(item, dict) else package.get('ProductCategoryName')

        quantity = package.get('Quantity', 0)
        unit = package.get('UnitOfMeasureName')
        source_harvest = package.get('SourceHarvestNames')
        lab_test_state = package.get('LabTestingState')
        lab_passed = package.get('LabTestingStatePassed')
        received_from = package.get('ReceivedFromFacilityName')
        received_dt = package.get('ReceivedDateTime')
        packaged_date = package.get('PackagedDate')
        last_modified = package.get('LastModified')

        self.db.execute("""
            INSERT INTO metrc_packages (
                id, metrc_label, storefront_id, item_name, product_category,
                quantity, unit_of_measure, package_status, source_harvest_name,
                lab_test_state, lab_test_passed, received_from_facility,
                received_date_time, packaged_date, last_modified_at,
                created_at, updated_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s::timestamp, %s::date, %s::timestamp,
                NOW(), NOW()
            )
            ON CONFLICT (metrc_label)
            DO UPDATE SET
                item_name = EXCLUDED.item_name,
                product_category = EXCLUDED.product_category,
                quantity = EXCLUDED.quantity,
                unit_of_measure = EXCLUDED.unit_of_measure,
                package_status = EXCLUDED.package_status,
                source_harvest_name = EXCLUDED.source_harvest_name,
                lab_test_state = EXCLUDED.lab_test_state,
                lab_test_passed = EXCLUDED.lab_test_passed,
                received_from_facility = EXCLUDED.received_from_facility,
                received_date_time = COALESCE(EXCLUDED.received_date_time, metrc_packages.received_date_time),
                packaged_date = COALESCE(EXCLUDED.packaged_date, metrc_packages.packaged_date),
                last_modified_at = EXCLUDED.last_modified_at,
                updated_at = NOW()
        """, (
            label,
            self.storefront_id,
            item_name,
            product_category,
            float(quantity) if quantity else 0,
            unit,
            status,
            source_harvest,
            lab_test_state,
            lab_passed,
            received_from,
            received_dt,
            packaged_date.split('T')[0] if packaged_date and 'T' in packaged_date else packaged_date,
            last_modified
        ))

    def sync_metrc_transfers(self, last_modified_start: Optional[str] = None):
        """Sync incoming transfers from METRC"""
        if not self.metrc:
            logger.warning("METRC client not configured, skipping transfer sync")
            return

        logger.info("Syncing METRC incoming transfers")

        try:
            transfers = self.metrc.get_incoming_transfers(last_modified_start=last_modified_start)

            if isinstance(transfers, list):
                logger.info(f"Found {len(transfers)} transfers to sync")

                for transfer in transfers:
                    try:
                        self._upsert_metrc_transfer(transfer, 'incoming')
                        self.sync_stats['transfers_synced'] += 1
                    except Exception as e:
                        logger.error(f"Error syncing transfer {transfer.get('Id')}: {str(e)}")
                        self.sync_stats['errors'].append(f"Transfer {transfer.get('Id')}: {str(e)}")

                self.db.commit()
                logger.info(f"Successfully synced {self.sync_stats['transfers_synced']} transfers")

        except Exception as e:
            logger.error(f"Error syncing METRC transfers: {str(e)}")
            self.db.rollback()
            self.sync_stats['errors'].append(f"METRC transfer sync: {str(e)}")

    def _upsert_metrc_transfer(self, transfer: Dict, transfer_type: str):
        """Insert or update a METRC transfer record"""
        transfer_id = transfer.get('Id')
        manifest_number = transfer.get('ManifestNumber')
        shipper_facility = transfer.get('ShipperFacilityName')
        shipper_license = transfer.get('ShipperFacilityLicenseNumber')
        recipient_facility = transfer.get('RecipientFacilityName')
        recipient_license = transfer.get('RecipientFacilityLicenseNumber')
        created_dt = transfer.get('CreatedDateTime')
        received_dt = transfer.get('ReceivedDateTime')
        last_modified = transfer.get('LastModified')

        # Try to get package count and total quantity from deliveries
        package_count = 0
        total_quantity = Decimal('0')
        try:
            deliveries = self.metrc.get_transfer_deliveries(transfer_id)
            for delivery in deliveries:
                delivery_id = delivery.get('Id')
                packages = self.metrc.get_delivery_packages(delivery_id)
                package_count += len(packages)
                for pkg in packages:
                    total_quantity += Decimal(str(pkg.get('ShippedQuantity', 0) or 0))
        except Exception as e:
            logger.warning(f"Could not get delivery details for transfer {transfer_id}: {e}")

        self.db.execute("""
            INSERT INTO metrc_transfers (
                id, metrc_transfer_id, storefront_id, manifest_number, transfer_type,
                shipper_facility, shipper_license, recipient_facility, recipient_license,
                created_date_time, received_date_time, package_count, total_quantity,
                last_modified_at, created_at, updated_at
            )
            VALUES (
                gen_random_uuid(), %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s::timestamp, %s::timestamp, %s, %s,
                %s::timestamp, NOW(), NOW()
            )
            ON CONFLICT (metrc_transfer_id)
            DO UPDATE SET
                manifest_number = EXCLUDED.manifest_number,
                shipper_facility = EXCLUDED.shipper_facility,
                shipper_license = EXCLUDED.shipper_license,
                recipient_facility = EXCLUDED.recipient_facility,
                recipient_license = EXCLUDED.recipient_license,
                received_date_time = COALESCE(EXCLUDED.received_date_time, metrc_transfers.received_date_time),
                package_count = EXCLUDED.package_count,
                total_quantity = EXCLUDED.total_quantity,
                last_modified_at = EXCLUDED.last_modified_at,
                updated_at = NOW()
        """, (
            transfer_id,
            self.storefront_id,
            manifest_number,
            transfer_type,
            shipper_facility,
            shipper_license,
            recipient_facility,
            recipient_license,
            created_dt,
            received_dt,
            package_count,
            float(total_quantity),
            last_modified
        ))

    # ==========================================
    # MAIN SYNC ORCHESTRATION
    # ==========================================

    def run_daily_sync(self):
        """Run full daily sync from both Treez and METRC"""
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_datetime = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT00:00:00')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        logger.info(f"Starting daily sync for {today}")

        # ========== TREEZ DATA ==========

        # Sync product catalog first (for brand/product linking)
        try:
            self.sync_products(yesterday_datetime)
        except Exception as e:
            logger.error(f"Product sync failed: {str(e)}")

        # Sync today's closed tickets (sales)
        try:
            self.sync_tickets(today)
        except Exception as e:
            logger.error(f"Ticket sync failed: {str(e)}")

        # Sync customers updated in last 24 hours
        try:
            self.sync_customers(yesterday_datetime)
        except Exception as e:
            logger.error(f"Customer sync failed: {str(e)}")

        # Sync invoices from last 7 days
        try:
            self.sync_invoices(week_ago, today)
        except Exception as e:
            logger.error(f"Invoice sync failed: {str(e)}")

        # ========== METRC DATA ==========

        # Sync METRC packages (inventory) - last 24 hours
        try:
            self.sync_metrc_packages(yesterday_datetime)
        except Exception as e:
            logger.error(f"METRC package sync failed: {str(e)}")

        # Sync METRC transfers - last 7 days
        try:
            self.sync_metrc_transfers(f"{week_ago}T00:00:00")
        except Exception as e:
            logger.error(f"METRC transfer sync failed: {str(e)}")

        return self.sync_stats


def lambda_handler(event, context):
    """AWS Lambda entry point"""
    logger.info(f"Lambda invoked with event: {json.dumps(event)}")

    # Load secrets
    secrets = get_secrets()

    # Extract config from secrets
    database_url = secrets.get('DATABASE_URL')
    treez_api_key = secrets.get('TREEZ_API_KEY')
    treez_client_id = secrets.get('TREEZ_CLIENT_ID', 'chapters_sync')
    treez_dispensary = secrets.get('TREEZ_DISPENSARY', 'barbarycoast')
    metrc_vendor_key = secrets.get('METRC_VENDOR_KEY')
    metrc_user_key = secrets.get('METRC_USER_KEY')
    metrc_license_number = secrets.get('METRC_LICENSE_NUMBER')

    # Validate database URL
    if not database_url:
        logger.error("DATABASE_URL not configured")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'DATABASE_URL not configured'})
        }

    # Check if at least one data source is configured
    if not treez_api_key and not metrc_vendor_key:
        logger.error("No data source configured (need TREEZ_API_KEY or METRC_VENDOR_KEY)")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'No data source configured'})
        }

    try:
        # Initialize Treez client (if configured)
        treez_client = None
        if treez_api_key:
            treez_client = TreezAPIClient(treez_api_key, treez_client_id, treez_dispensary)
            logger.info("Treez client initialized")

        # Initialize METRC client (if configured)
        metrc_client = None
        if metrc_vendor_key and metrc_user_key and metrc_license_number:
            metrc_client = MetrcAPIClient(metrc_vendor_key, metrc_user_key, metrc_license_number)
            logger.info("METRC client initialized")

        # Initialize database client
        db_client = DatabaseClient(database_url)
        db_client.connect()

        # Run sync
        sync = DataSync(treez_client, metrc_client, db_client)
        stats = sync.run_daily_sync()

        # Close database connection
        db_client.close()

        logger.info(f"Sync completed: {json.dumps(stats)}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sync completed successfully',
                'stats': stats
            })
        }

    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


# CLI support for local testing
if __name__ == '__main__':
    import sys

    # Load environment from .env file if available
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"').strip("'")

    # Run sync
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))
    sys.exit(0 if result['statusCode'] == 200 else 1)
