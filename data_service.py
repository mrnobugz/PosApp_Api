"""
Data Service Layer to eliminate database access duplication
"""
import database as db
from typing import List, Dict, Any, Optional

class DataService:
    """Centralized data access service"""
    
    @staticmethod
    def get_products_with_filters(**kwargs) -> List[Dict[str, Any]]:
        """Get products with filtering capabilities"""
        return db.get_products_with_filters(**kwargs)
    
    @staticmethod
    def get_all_products() -> List[Dict[str, Any]]:
        """Get all products"""
        return db.get_all_products()
    
    @staticmethod
    def get_product_by_id(product_id: int) -> Optional[Dict[str, Any]]:
        """Get product by ID"""
        return db.get_product_by_id(product_id)
    
    @staticmethod
    def add_product(**kwargs) -> bool:
        """Add new product"""
        return db.add_product(**kwargs)
    
    @staticmethod
    def update_product(**kwargs) -> bool:
        """Update existing product"""
        return db.update_product(**kwargs)
    
    @staticmethod
    def delete_product(product_id: int) -> bool:
        """Delete product"""
        return db.delete_product(product_id)
    
    @staticmethod
    def get_all_categories() -> List[Dict[str, Any]]:
        """Get all categories"""
        return db.get_all_categories()
    
    @staticmethod
    def add_category(name: str) -> bool:
        """Add new category"""
        return db.add_category(name)
    
    @staticmethod
    def delete_category(category_id: int) -> bool:
        """Delete category"""
        return db.delete_category(category_id)
    
    @staticmethod
    def get_all_suppliers() -> List[Dict[str, Any]]:
        """Get all suppliers"""
        return db.get_all_suppliers()
    
    @staticmethod
    def add_supplier(**kwargs) -> bool:
        """Add new supplier"""
        return db.add_supplier(**kwargs)
    
    @staticmethod
    def update_supplier(**kwargs) -> bool:
        """Update supplier"""
        return db.update_supplier(**kwargs)
    
    @staticmethod
    def delete_supplier(supplier_id: int) -> bool:
        """Delete supplier"""
        return db.delete_supplier(supplier_id)
    
    @staticmethod
    def get_all_sales() -> List[Dict[str, Any]]:
        """Get all sales"""
        return db.get_all_sales()
    
    @staticmethod
    def get_sale_details(sale_id: int) -> List[Dict[str, Any]]:
        """Get sale details"""
        return db.get_sale_details(sale_id)
    
    @staticmethod
    def record_sale(**kwargs) -> tuple[bool, Optional[int]]:
        """Record new sale"""
        return db.record_sale(**kwargs)
    
    @staticmethod
    def get_all_purchases() -> List[Dict[str, Any]]:
        """Get all purchases"""
        return db.get_all_purchases()
    
    @staticmethod
    def get_purchase_details(purchase_id: int) -> List[Dict[str, Any]]:
        """Get purchase details"""
        return db.get_purchase_details(purchase_id)
    
    @staticmethod
    def record_purchase(**kwargs) -> tuple[bool, Optional[int]]:
        """Record new purchase"""
        return db.record_purchase(**kwargs)
    
    @staticmethod
    def get_dashboard_stats() -> Dict[str, Any]:
        """Get dashboard statistics"""
        return {
            'total_sales': db.get_total_sales_amount(),
            'total_products': db.get_total_products_count(),
            'total_categories': db.get_total_categories_count(),
            'low_stock_count': db.get_low_stock_count()
        }
    
    @staticmethod
    def get_weekly_sales() -> List[Dict[str, Any]]:
        """Get weekly sales summary"""
        return db.get_weekly_sales_summary()
    
    @staticmethod
    def get_top_products(limit: int = 5) -> List[Dict[str, Any]]:
        """Get top selling products"""
        return db.get_top_selling_products(limit=limit)
