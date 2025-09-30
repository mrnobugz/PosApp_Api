"""
API client for external system integration
"""
import requests
import json
import time
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import logging
from config import APIConfig

logger = logging.getLogger(__name__)

class APIClient:
    """HTTP API client for external system communication"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {config.api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'POS-Sync/1.0'
        })
        
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Tuple[bool, Any]:
        """Make HTTP request with retry logic"""
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.config.retry_attempts + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.config.timeout,
                    **kwargs
                )
                
                if response.status_code == 200:
                    return True, response.json()
                elif response.status_code == 201:
                    return True, response.json()
                elif response.status_code == 204:
                    return True, None
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    logger.warning(f"API request failed (attempt {attempt + 1}): {error_msg}")
                    
                    if attempt < self.config.retry_attempts:
                        time.sleep(self.config.retry_delay * (attempt + 1))
                        continue
                    
                    return False, error_msg
                    
            except requests.exceptions.Timeout:
                error_msg = "Request timeout"
                logger.warning(f"API timeout (attempt {attempt + 1})")
                
            except requests.exceptions.ConnectionError:
                error_msg = "Connection error"
                logger.warning(f"API connection error (attempt {attempt + 1})")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"API request error (attempt {attempt + 1}): {error_msg}")
                
            if attempt < self.config.retry_attempts:
                time.sleep(self.config.retry_delay * (attempt + 1))
        
        return False, error_msg
    
    def health_check(self) -> Tuple[bool, Dict[str, Any]]:
        """Check API health status"""
        return self._make_request('GET', '/health')
    
    def get_products(self, updated_since: Optional[str] = None, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Get products from external system"""
        params = {
            'limit': limit,
            'offset': offset
        }
        
        if updated_since:
            params['updated_since'] = updated_since
            
        return self._make_request('GET', '/products', params=params)
    
    def get_product(self, product_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Get single product by ID"""
        return self._make_request('GET', f'/products/{product_id}')
    
    def create_product(self, product_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Create new product"""
        return self._make_request('POST', '/products', json=product_data)
    
    def update_product(self, product_id: str, product_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Update existing product"""
        return self._make_request('PUT', f'/products/{product_id}', json=product_data)
    
    def delete_product(self, product_id: str) -> Tuple[bool, Any]:
        """Delete product"""
        return self._make_request('DELETE', f'/products/{product_id}')
    
    def get_categories(self, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Get categories from external system"""
        params = {
            'limit': limit,
            'offset': offset
        }
        return self._make_request('GET', '/categories', params=params)
    
    def get_category(self, category_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Get single category by ID"""
        return self._make_request('GET', f'/categories/{category_id}')
    
    def create_category(self, category_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Create new category"""
        return self._make_request('POST', '/categories', json=category_data)
    
    def update_category(self, category_id: str, category_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Update existing category"""
        return self._make_request('PUT', f'/categories/{category_id}', json=category_data)
    
    def delete_category(self, category_id: str) -> Tuple[bool, Any]:
        """Delete category"""
        return self._make_request('DELETE', f'/categories/{category_id}')
    
    def get_suppliers(self, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Get suppliers from external system"""
        params = {
            'limit': limit,
            'offset': offset
        }
        return self._make_request('GET', '/suppliers', params=params)
    
    def get_supplier(self, supplier_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Get single supplier by ID"""
        return self._make_request('GET', f'/suppliers/{supplier_id}')
    
    def create_supplier(self, supplier_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Create new supplier"""
        return self._make_request('POST', '/suppliers', json=supplier_data)
    
    def update_supplier(self, supplier_id: str, supplier_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Update existing supplier"""
        return self._make_request('PUT', f'/suppliers/{supplier_id}', json=supplier_data)
    
    def delete_supplier(self, supplier_id: str) -> Tuple[bool, Any]:
        """Delete supplier"""
        return self._make_request('DELETE', f'/suppliers/{supplier_id}')
    
    def batch_sync(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Batch synchronization endpoint"""
        return self._make_request('POST', '/sync/batch', json=data)

class MockAPIClient:
    """Mock API client for testing without external dependencies"""
    
    def __init__(self):
        self.mock_data = {
            "products": [
                {
                    "id": "ext_prod_1",
                    "name": "External Product 1",
                    "price": 29.99,
                    "stock": 100,
                    "sku": "EXT001",
                    "description": "Mock external product",
                    "barcode": "1234567890123",
                    "buying_price": 15.00,
                    "low_stock_threshold": 10,
                    "category_id": 1,
                    "updated_at": datetime.now().isoformat()
                },
                {
                    "id": "ext_prod_2",
                    "name": "External Product 2",
                    "price": 49.99,
                    "stock": 50,
                    "sku": "EXT002",
                    "description": "Another mock product",
                    "barcode": "9876543210987",
                    "buying_price": 25.00,
                    "low_stock_threshold": 5,
                    "category_id": 2,
                    "updated_at": datetime.now().isoformat()
                }
            ],
            "categories": [
                {"id": "ext_cat_1", "name": "External Electronics"},
                {"id": "ext_cat_2", "name": "External Books"},
                {"id": "ext_cat_3", "name": "External Clothing"}
            ],
            "suppliers": [
                {
                    "id": "ext_sup_1",
                    "name": "External Supplier Co",
                    "contact_person": "John Doe",
                    "phone": "+1-555-123-4567"
                },
                {
                    "id": "ext_sup_2",
                    "name": "Global Supplies Ltd",
                    "contact_person": "Jane Smith",
                    "phone": "+1-555-987-6543"
                }
            ]
        }
    
    def _mock_response(self, data: Any) -> Tuple[bool, Any]:
        """Return mock response"""
        time.sleep(0.1)  # Simulate network delay
        return True, data
    
    def health_check(self) -> Tuple[bool, Dict[str, Any]]:
        """Mock health check"""
        return self._mock_response({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0"
        })
    
    def get_products(self, updated_since: Optional[str] = None, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Mock get products"""
        products = self.mock_data["products"]
        
        if updated_since:
            # Filter by updated_since (simplified)
            try:
                since_date = datetime.fromisoformat(updated_since.replace('Z', '+00:00'))
                products = [p for p in products if datetime.fromisoformat(p["updated_at"].replace('Z', '+00:00')) > since_date]
            except:
                pass
        
        # Apply pagination
        paginated_products = products[offset:offset + limit]
        
        return self._mock_response({
            "products": paginated_products,
            "total": len(products),
            "offset": offset,
            "limit": limit
        })
    
    def get_product(self, product_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Mock get single product"""
        product = next((p for p in self.mock_data["products"] if p["id"] == product_id), None)
        if product:
            return self._mock_response({"product": product})
        return False, {"error": "Product not found"}
    
    def create_product(self, product_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock create product"""
        new_id = f"ext_prod_{len(self.mock_data['products']) + 1}"
        product_data["id"] = new_id
        product_data["updated_at"] = datetime.now().isoformat()
        self.mock_data["products"].append(product_data)
        return self._mock_response({"product": product_data})
    
    def update_product(self, product_id: str, product_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock update product"""
        for i, product in enumerate(self.mock_data["products"]):
            if product["id"] == product_id:
                product_data["id"] = product_id
                product_data["updated_at"] = datetime.now().isoformat()
                self.mock_data["products"][i] = product_data
                return self._mock_response({"product": product_data})
        return False, {"error": "Product not found"}
    
    def delete_product(self, product_id: str) -> Tuple[bool, Any]:
        """Mock delete product"""
        for i, product in enumerate(self.mock_data["products"]):
            if product["id"] == product_id:
                del self.mock_data["products"][i]
                return True, None
        return False, {"error": "Product not found"}
    
    def get_categories(self, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Mock get categories"""
        categories = self.mock_data["categories"][offset:offset + limit]
        return self._mock_response({
            "categories": categories,
            "total": len(self.mock_data["categories"]),
            "offset": offset,
            "limit": limit
        })
    
    def get_category(self, category_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Mock get single category"""
        category = next((c for c in self.mock_data["categories"] if c["id"] == category_id), None)
        if category:
            return self._mock_response({"category": category})
        return False, {"error": "Category not found"}
    
    def create_category(self, category_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock create category"""
        new_id = f"ext_cat_{len(self.mock_data['categories']) + 1}"
        category_data["id"] = new_id
        self.mock_data["categories"].append(category_data)
        return self._mock_response({"category": category_data})
    
    def update_category(self, category_id: str, category_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock update category"""
        for i, category in enumerate(self.mock_data["categories"]):
            if category["id"] == category_id:
                category_data["id"] = category_id
                self.mock_data["categories"][i] = category_data
                return self._mock_response({"category": category_data})
        return False, {"error": "Category not found"}
    
    def delete_category(self, category_id: str) -> Tuple[bool, Any]:
        """Mock delete category"""
        for i, category in enumerate(self.mock_data["categories"]):
            if category["id"] == category_id:
                del self.mock_data["categories"][i]
                return True, None
        return False, {"error": "Category not found"}
    
    def get_suppliers(self, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        """Mock get suppliers"""
        suppliers = self.mock_data["suppliers"][offset:offset + limit]
        return self._mock_response({
            "suppliers": suppliers,
            "total": len(self.mock_data["suppliers"]),
            "offset": offset,
            "limit": limit
        })
    
    def get_supplier(self, supplier_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Mock get single supplier"""
        supplier = next((s for s in self.mock_data["suppliers"] if s["id"] == supplier_id), None)
        if supplier:
            return self._mock_response({"supplier": supplier})
        return False, {"error": "Supplier not found"}
    
    def create_supplier(self, supplier_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock create supplier"""
        new_id = f"ext_sup_{len(self.mock_data['suppliers']) + 1}"
        supplier_data["id"] = new_id
        self.mock_data["suppliers"].append(supplier_data)
        return self._mock_response({"supplier": supplier_data})
    
    def update_supplier(self, supplier_id: str, supplier_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock update supplier"""
        for i, supplier in enumerate(self.mock_data["suppliers"]):
            if supplier["id"] == supplier_id:
                supplier_data["id"] = supplier_id
                self.mock_data["suppliers"][i] = supplier_data
                return self._mock_response({"supplier": supplier_data})
        return False, {"error": "Supplier not found"}
    
    def delete_supplier(self, supplier_id: str) -> Tuple[bool, Any]:
        """Mock delete supplier"""
        for i, supplier in enumerate(self.mock_data["suppliers"]):
            if supplier["id"] == supplier_id:
                del self.mock_data["suppliers"][i]
                return True, None
        return False, {"error": "Supplier not found"}
    
    def batch_sync(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Mock batch sync"""
        return self._mock_response({
            "success": True,
            "processed": len(data.get("products", [])) + len(data.get("categories", [])) + len(data.get("suppliers", [])),
            "timestamp": datetime.now().isoformat()
        })

    def get_customers(self, limit: int = 100, offset: int = 0) -> Tuple[bool, Dict[str, Any]]:
        params = {'limit': limit, 'offset': offset}
        return self._make_request('GET', '/customers', params=params)
    
    def create_customer(self, customer_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        return self._make_request('POST', '/customers', json=customer_data)
    
    def update_customer(self, customer_id: str, customer_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        return self._make_request('PUT', f'/customers/{customer_id}', json=customer_data)
    
    def delete_customer(self, customer_id: str) -> Tuple[bool, Any]:
        return self._make_request('DELETE', f'/customers/{customer_id}')

    # --- NEW: Transaction Endpoints (typically push-only) ---
    def create_sale(self, sale_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        return self._make_request('POST', '/sales', json=sale_data)

    def create_purchase(self, purchase_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        return self._make_request('POST', '/purchases', json=purchase_data)

    def batch_sync(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Batch synchronization endpoint"""
        return self._make_request('POST', '/sync/batch', json=data)


    def create_customer(self, customer_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        new_id = f"ext_cust_{len(self.mock_data.get('customers', [])) + 1}"
        customer_data["id"] = new_id
        if "customers" not in self.mock_data: self.mock_data["customers"] = []
        self.mock_data["customers"].append(customer_data)
        return self._mock_response({"customer": customer_data})

    def create_sale(self, sale_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        new_id = f"ext_sale_{len(self.mock_data.get('sales', [])) + 1}"
        sale_data["id"] = new_id
        if "sales" not in self.mock_data: self.mock_data["sales"] = []
        self.mock_data["sales"].append(sale_data)
        return self._mock_response({"sale": sale_data, "sale_id": new_id})

    def create_purchase(self, purchase_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        new_id = f"ext_purch_{len(self.mock_data.get('purchases', [])) + 1}"
        purchase_data["id"] = new_id
        if "purchases" not in self.mock_data: self.mock_data["purchases"] = []
        self.mock_data["purchases"].append(purchase_data)
        return self._mock_response({"purchase": purchase_data, "purchase_id": new_id})
