"""
Synchronization service for bidirectional data sync with external APIs
"""
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import threading
import schedule
import database_sync as db_sync
import api_client
from config import config, SyncConfig
import database as db

class SyncService:
    """Main synchronization service for bidirectional data sync"""
    
    def __init__(self, config_obj: SyncConfig = None): 
        self.config = config_obj or config 
        self.mock_mode = False
        self.sync_thread = None
        self.running = False
        
    def enable_mock_mode(self):
        """Enable mock mode for testing"""
        self.api_client = api_client.MockAPIClient()
        self.mock_mode = True
    
    def sync_products(self, direction: str = "bidirectional") -> Dict[str, Any]:
        """Synchronize products with external system"""
        results = {
            "success": True,
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "conflicts": 0,
            "errors": [],
            "details": []
        }
        
        try:
            if direction in ["pull", "bidirectional"]:
                pull_results = self._pull_products()
                results["created"] += pull_results["created"]
                results["updated"] += pull_results["updated"]
                results["errors"].extend(pull_results["errors"])
                
            if direction in ["push", "bidirectional"]:
                push_results = self._push_products()
                results["created"] += push_results["created"]
                results["updated"] += push_results["updated"]
                results["deleted"] += push_results["deleted"]
                results["errors"].extend(push_results["errors"])
                
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            
        return results
    
    def _pull_products(self) -> Dict[str, Any]:
        """Pull products from external system"""
        results = {"created": 0, "updated": 0, "errors": []}
        
        try:
            # Get last sync time
            last_sync = db_sync.get_sync_config("last_product_sync")
            if last_sync:
                last_sync_time = datetime.fromisoformat(last_sync)
            else:
                last_sync_time = datetime.min
            
            # Fetch products from external system
            success, response = self.api_client.get_products(
                updated_since=last_sync_time.isoformat()
            )
            
            if not success:
                results["errors"].append(f"Failed to fetch products: {response}")
                return results
            
            external_products = response.get("products", [])
            
            for ext_product in external_products:
                try:
                    # Check if product exists locally
                    local_product = None
                    if ext_product.get("external_id"):
                        # Find by external_id
                        conn = db.connect_db()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT * FROM products WHERE external_id = ?",
                                (ext_product["id"],)
                            )
                            local_product = cursor.fetchone()
                            conn.close()
                    
                    # Map external product to local format
                    product_data = self._map_external_product(ext_product)
                    
                    if local_product:
                        # Update existing product
                        db.update_product(local_product["id"], **product_data)
                        results["updated"] += 1
                    else:
                        # Create new product
                        db.add_product(**product_data)
                        results["created"] += 1
                        
                except Exception as e:
                    results["errors"].append(f"Error processing product {ext_product.get('id')}: {str(e)}")
                    
            # Update last sync time
            db_sync.set_sync_config("last_product_sync", datetime.now().isoformat())
            
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def _push_products(self) -> Dict[str, Any]:
        """Push local products to external system"""
        results = {"created": 0, "updated": 0, "deleted": 0, "errors": []}
        
        try:
            # Get pending products
            pending_products = db_sync.get_pending_sync_items("product", self.config.sync.batch_size)
            
            for local_product in pending_products:
                try:
                    # Map local product to external format
                    ext_product = self._map_local_product(local_product)
                    
                    if local_product["sync_status"] == "deleted":
                        # Delete from external system
                        if local_product["external_id"]:
                            success, response = self.api_client.delete_product(local_product["external_id"])
                            if success:
                                results["deleted"] += 1
                            else:
                                results["errors"].append(f"Failed to delete product: {response}")
                    elif local_product["external_id"]:
                        # Update existing product
                        success, response = self.api_client.update_product(
                            local_product["external_id"],
                            ext_product
                        )
                        if success:
                            results["updated"] += 1
                        else:
                            results["errors"].append(f"Failed to update product: {response}")
                    else:
                        # Create new product
                        success, response = self.api_client.create_product(ext_product)
                        if success:
                            # Update local product with external ID
                            external_id = response["product"]["id"]
                            db_sync.update_sync_status("product", local_product["id"], external_id)
                            results["created"] += 1
                        else:
                            results["errors"].append(f"Failed to create product: {response}")
                            
                except Exception as e:
                    results["errors"].append(f"Error pushing product {local_product['id']}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def sync_categories(self, direction: str = "bidirectional") -> Dict[str, Any]:
        """Synchronize categories with external system"""
        results = {
            "success": True,
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "conflicts": 0,
            "errors": []
        }
        
        try:
            if direction in ["pull", "bidirectional"]:
                pull_results = self._pull_categories()
                results["created"] += pull_results["created"]
                results["updated"] += pull_results["updated"]
                results["errors"].extend(pull_results["errors"])
                
            if direction in ["push", "bidirectional"]:
                push_results = self._push_categories()
                results["created"] += push_results["created"]
                results["updated"] += push_results["updated"]
                results["deleted"] += push_results["deleted"]
                results["errors"].extend(push_results["errors"])
                
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            
        return results
    
    def _pull_categories(self) -> Dict[str, Any]:
        """Pull categories from external system"""
        results = {"created": 0, "updated": 0, "errors": []}
        
        try:
            success, response = self.api_client.get_categories()
            if not success:
                results["errors"].append(f"Failed to fetch categories: {response}")
                return results
            
            external_categories = response.get("categories", [])
            
            for ext_category in external_categories:
                try:
                    # Check if category exists locally
                    local_category = db.get_category_by_name(ext_category["name"])
                    
                    if local_category:
                        # Update existing category
                        db_sync.update_sync_status("category", local_category["id"], ext_category["id"])
                        results["updated"] += 1
                    else:
                        # Create new category
                        db.add_category(ext_category["name"])
                        # Get the newly created category
                        new_category = db.get_category_by_name(ext_category["name"])
                        if new_category:
                            db_sync.update_sync_status("category", new_category["id"], ext_category["id"])
                        results["created"] += 1
                        
                except Exception as e:
                    results["errors"].append(f"Error processing category {ext_category.get('id')}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def _push_categories(self) -> Dict[str, Any]:
        """Push local categories to external system"""
        results = {"created": 0, "updated": 0, "deleted": 0, "errors": []}
        
        try:
            pending_categories = db_sync.get_pending_sync_items("category", self.config.sync.batch_size)
            
            for local_category in pending_categories:
                try:
                    category_data = {"name": local_category["name"]}
                    
                    if local_category["external_id"]:
                        success, response = self.api_client.update_category(
                            local_category["external_id"],
                            category_data
                        )
                        if success:
                            results["updated"] += 1
                        else:
                            results["errors"].append(f"Failed to update category: {response}")
                    else:
                        success, response = self.api_client.create_category(category_data)
                        if success:
                            external_id = response["category"]["id"]
                            db_sync.update_sync_status("category", local_category["id"], external_id)
                            results["created"] += 1
                        else:
                            results["errors"].append(f"Failed to create category: {response}")
                            
                except Exception as e:
                    results["errors"].append(f"Error pushing category {local_category['id']}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def sync_suppliers(self, direction: str = "bidirectional") -> Dict[str, Any]:
        """Synchronize suppliers with external system"""
        results = {
            "success": True,
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "conflicts": 0,
            "errors": []
        }
        
        try:
            if direction in ["pull", "bidirectional"]:
                pull_results = self._pull_suppliers()
                results["created"] += pull_results["created"]
                results["updated"] += pull_results["updated"]
                results["errors"].extend(pull_results["errors"])
                
            if direction in ["push", "bidirectional"]:
                push_results = self._push_suppliers()
                results["created"] += push_results["created"]
                results["updated"] += push_results["updated"]
                results["deleted"] += push_results["deleted"]
                results["errors"].extend(push_results["errors"])
                
        except Exception as e:
            results["success"] = False
            results["errors"].append(str(e))
            
        return results
    
    def _pull_suppliers(self) -> Dict[str, Any]:
        """Pull suppliers from external system"""
        results = {"created": 0, "updated": 0, "errors": []}
        
        try:
            success, response = self.api_client.get_suppliers()
            if not success:
                results["errors"].append(f"Failed to fetch suppliers: {response}")
                return results
            
            external_suppliers = response.get("suppliers", [])
            
            for ext_supplier in external_suppliers:
                try:
                    # Check if supplier exists locally
                    conn = db.connect_db()
                    if conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT * FROM suppliers WHERE name = ?",
                            (ext_supplier["name"],)
                        )
                        local_supplier = cursor.fetchone()
                        conn.close()
                    
                    supplier_data = {
                        "name": ext_supplier["name"],
                        "contact_person": ext_supplier.get("contact_person"),
                        "phone": ext_supplier.get("phone")
                    }
                    
                    if local_supplier:
                        # Update existing supplier
                        db.update_supplier(local_supplier["id"], **supplier_data)
                        db_sync.update_sync_status("supplier", local_supplier["id"], ext_supplier["id"])
                        results["updated"] += 1
                    else:
                        # Create new supplier
                        db.add_supplier(**supplier_data)
                        # Get the newly created supplier
                        conn = db.connect_db()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                "SELECT * FROM suppliers WHERE name = ?",
                                (ext_supplier["name"],)
                            )
                            new_supplier = cursor.fetchone()
                            conn.close()
                            if new_supplier:
                                db_sync.update_sync_status("supplier", new_supplier["id"], ext_supplier["id"])
                        results["created"] += 1
                        
                except Exception as e:
                    results["errors"].append(f"Error processing supplier {ext_supplier.get('id')}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def _push_suppliers(self) -> Dict[str, Any]:
        """Push local suppliers to external system"""
        results = {"created": 0, "updated": 0, "deleted": 0, "errors": []}
        
        try:
            pending_suppliers = db_sync.get_pending_sync_items("supplier", self.config.sync.batch_size)
            
            for local_supplier in pending_suppliers:
                try:
                    supplier_data = {
                        "name": local_supplier["name"],
                        "contact_person": local_supplier["contact_person"],
                        "phone": local_supplier["phone"]
                    }
                    
                    if local_supplier["external_id"]:
                        success, response = self.api_client.update_supplier(
                            local_supplier["external_id"],
                            supplier_data
                        )
                        if success:
                            results["updated"] += 1
                        else:
                            results["errors"].append(f"Failed to update supplier: {response}")
                    else:
                        success, response = self.api_client.create_supplier(supplier_data)
                        if success:
                            external_id = response["supplier"]["id"]
                            db_sync.update_sync_status("supplier", local_supplier["id"], external_id)
                            results["created"] += 1
                        else:
                            results["errors"].append(f"Failed to create supplier: {response}")
                            
                except Exception as e:
                    results["errors"].append(f"Error pushing supplier {local_supplier['id']}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results
    
    def sync_all(self, direction: str = "bidirectional") -> Dict[str, Any]:
        """Synchronize all entities"""
        print(f"[{datetime.now().isoformat()}] --- Starting background sync cycle ---")
        results = {
            "products": {},
            "categories": {},
            "suppliers": {},
            "overall_success": True,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Sync categories first (dependencies)
            results["categories"] = self.sync_categories(direction)
            
            # Sync suppliers
            results["suppliers"] = self.sync_suppliers(direction)
            
            # Sync products last
            results["products"] = self.sync_products(direction)
            
            # Check overall success
            results["overall_success"] = all([
                results["products"].get("success", True),
                results["categories"].get("success", True),
                results["suppliers"].get("success", True)
            ])
            
            # Record sync history
            db_sync.record_sync_history(
                sync_type="bidirectional",
                entity_type="all",
                entity_id=None,
                external_id=None,
                action="sync_all",
                status="success" if results["overall_success"] else "failed",
                details=results
            )
            
        except Exception as e:
            results["overall_success"] = False
            results["error"] = str(e)
            
            
        print(f"[{datetime.now().isoformat()}] --- Background sync cycle finished. Success: {results['overall_success']} ---")    
        return results
    
    def _map_external_product(self, ext_product: Dict[str, Any]) -> Dict[str, Any]:
        """Map external product format to local format"""
        return {
            "name": ext_product.get("name", ""),
            "price": float(ext_product.get("price", 0.0)),
            "stock": int(ext_product.get("stock", 0)),
            "sku": ext_product.get("sku"),
            "description": ext_product.get("description", ""),
            "barcode": ext_product.get("barcode"),
            "buying_price": float(ext_product.get("buying_price", 0.0)),
            "low_stock_threshold": int(ext_product.get("low_stock_threshold", 10)),
            "category_id": ext_product.get("category_id")
        }
    
    def _map_local_product(self, local_product: Dict[str, Any]) -> Dict[str, Any]:
        """Map local product format to external format"""
        return {
            "name": local_product["name"],
            "price": float(local_product["price"]),
            "stock": int(local_product["stock"]),
            "sku": local_product["sku"],
            "description": local_product.get("description", ""),
            "barcode": local_product.get("barcode"),
            "buying_price": float(local_product.get("buying_price", 0.0)),
            "low_stock_threshold": int(local_product.get("low_stock_threshold", 10)),
            "category_id": local_product.get("category_id")
        }
    
    def start_auto_sync(self):
        """Start automatic synchronization in background thread"""
        if self.running:
            return
        
        self.running = True
        self.sync_thread = threading.Thread(target=self._auto_sync_worker, daemon=True)
        self.sync_thread.start()
        
    def stop_auto_sync(self):
        """Stop automatic synchronization"""
        self.running = False
        
    def _auto_sync_worker(self):
        """Background worker for automatic synchronization"""
        schedule.every(self.config.sync.sync_interval_minutes).minutes.do(self.sync_all)
        
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get current synchronization status"""
        return {
            "running": self.running,
            "mock_mode": self.mock_mode,
            "config": self.config.to_dict(),
            "statistics": db_sync.get_sync_statistics(),
            "last_sync_history": db_sync.get_sync_history(10)
        }
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to external API"""
        try:
            success, response = self.api_client.health_check()
            return {
                "success": success,
                "response": response,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }

# Global sync service instance
sync_service = SyncService()

if __name__ == "__main__":
    # Test the sync service
    service = SyncService()
    service.enable_mock_mode()
    
    
