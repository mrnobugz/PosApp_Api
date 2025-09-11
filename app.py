from flask import Flask, request, jsonify
from flask_cors import CORS
import database as db
from werkzeug.security import generate_password_hash, check_password_hash
import datetime
import json

app = Flask(__name__)
CORS(app)

@app.route('/')
def index():
    return jsonify({"message": "POS System API is running"}), 200

# =========================
# AUTHENTICATION ENDPOINTS
# =========================
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    
    user = db.get_user_by_username(username)
    if user and check_password_hash(user['password_hash'], password):
        return jsonify({
            'id': user['id'],
            'username': user['username'],
            'role': user['role']
        }), 200
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/users', methods=['GET'])
def get_users():
    users = db.get_all_users()
    return jsonify(users), 200

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')
    role = data.get('role', 'cashier')
    
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    
    password_hash = generate_password_hash(password)
    success = db.add_user(username, password_hash, role)
    
    if success:
        return jsonify({'message': 'User created successfully'}), 201
    else:
        return jsonify({'error': 'Username already exists'}), 409

@app.route('/api/users/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    success = db.delete_user(user_id)
    if success:
        return jsonify({'message': 'User deleted successfully'}), 200
    else:
        return jsonify({'error': 'Failed to delete user'}), 400

# =========================
# PRODUCT ENDPOINTS
# =========================
@app.route('/api/products', methods=['GET'])
def get_products():
    category = request.args.get('category')
    price_min = request.args.get('price_min')
    price_max = request.args.get('price_max')
    stock_status = request.args.get('stock_status')
    search_term = request.args.get('search')
    
    products = db.get_products_with_filters(
        category=category,
        price_min=price_min,
        price_max=price_max,
        stock_status=stock_status,
        search_term=search_term
    )
    return jsonify(products), 200

@app.route('/api/products', methods=['POST'])
def create_product():
    data = request.get_json()
    
    required_fields = ['name', 'price', 'stock']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'{field} is required'}), 400

    image_data = None
    image_data_base64 = data.get('image_data')
    if image_data_base64:
        try:
            # Decode the Base64 string into binary data
            image_data = base64.b64decode(image_data_base64)
        except (ValueError, TypeError):
            return jsonify({'error': 'Invalid Base64 image data'}), 400
            
    success = db.add_product(
        name=data['name'],
        price=data['price'],
        stock=data['stock'],
        category_id=data.get('category_id'),
        sku=data.get('sku'),
        description=data.get('description'),
        barcode=data.get('barcode'),
        buying_price=data.get('buying_price', 0.0),
        low_stock_threshold=data.get('low_stock_threshold', 10)
    )
    
    if success:
        return jsonify({'message': 'Product created successfully'}), 201
    else:
        return jsonify({'error': 'Product already exists or invalid data'}), 409

@app.route('/api/products/<int:product_id>', methods=['GET'])
def get_product(product_id):
    product = db.get_product_by_id(product_id)
    if product:
        return jsonify(product), 200
    else:
        return jsonify({'error': 'Product not found'}), 404

@app.route('/api/products/<int:product_id>', methods=['PUT'])
def update_product(product_id):
    data = request.get_json()

    image_data = None
    if 'image_data' in data: # Check if the key is present
        image_data_base64 = data.get('image_data')
        if image_data_base64:
            try:
                image_data = base64.b64decode(image_data_base64)
            except (ValueError, TypeError):
                return jsonify({'error': 'Invalid Base64 image data'}), 400
    success = db.update_product(
        product_id=product_id,
        name=data['name'],
        price=data['price'],
        stock=data['stock'],
        category_id=data.get('category_id'),
        sku=data.get('sku'),
        description=data.get('description'),
        barcode=data.get('barcode'),
        buying_price=data.get('buying_price', 0.0),
        low_stock_threshold=data.get('low_stock_threshold', 10)
    )
    
    if success:
        return jsonify({'message': 'Product updated successfully'}), 200
    else:
        return jsonify({'error': 'Failed to update product'}), 400

@app.route('/api/products/<int:product_id>', methods=['DELETE'])
def delete_product(product_id):
    success = db.delete_product(product_id)
    if success:
        return jsonify({'message': 'Product deleted successfully'}), 200
    else:
        return jsonify({'error': 'Failed to delete product'}), 400

@app.route('/api/products/<int:product_id>/image', methods=['GET'])
def get_product_image(product_id):
    image_data = db.get_product_image_data(product_id)
    
    if image_data:
        # Use send_file to correctly serve the binary image data
        return send_file(
            io.BytesIO(image_data),
            mimetype='image/jpeg', # A common default, adjust if you store mime types
            as_attachment=False
        )
    else:
        return jsonify({'error': 'Image not found for this product'}), 404

# =========================
# CATEGORY ENDPOINTS
# =========================
@app.route('/api/categories', methods=['GET'])
def get_categories():
    categories = db.get_all_categories()
    return jsonify(categories), 200

@app.route('/api/categories', methods=['POST'])
def create_category():
    data = request.get_json()
    name = data.get('name')
    
    if not name:
        return jsonify({'error': 'Category name is required'}), 400
    
    success = db.add_category(name)
    if success:
        return jsonify({'message': 'Category created successfully'}), 201
    else:
        return jsonify({'error': 'Category already exists'}), 409

@app.route('/api/categories/<int:category_id>', methods=['DELETE'])
def delete_category(category_id):
    success = db.delete_category(category_id)
    if success:
        return jsonify({'message': 'Category deleted successfully'}), 200
    else:
        return jsonify({'error': 'Failed to delete category'}), 400

# =========================
# SALES ENDPOINTS
# =========================
@app.route('/api/sales', methods=['GET'])
def get_sales():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    product_name = request.args.get('product_name')
    
    sales = db.get_all_sales(start_date=start_date, end_date=end_date, product_name=product_name)
    return jsonify(sales), 200

@app.route('/api/sales', methods=['POST'])
def create_sale():
    data = request.get_json()
    cart_items = data.get('cart_items')
    payments = data.get('payments', []) # Expect a list of payments
    discount_amount = data.get('discount_amount', 0.0)
    tax_rate = data.get('tax_rate', 0.0)
    customer_id = data.get('customer_id') 
    due_date = data.get('due_date') 

    if not cart_items:
        return jsonify({'error': 'Cart items are required'}), 400
    
    # Pass the new fields to your database function
    success, sale_id = db.record_sale(
        cart_items, 
        payments, 
        discount_amount, 
        tax_rate, 
        customer_id, # <-- PASS THIS
        due_date # <-- PASS THIS
    )
    
    if success:
        return jsonify({'message': 'Sale recorded successfully', 'sale_id': sale_id}), 201
    else:
        return jsonify({'error': 'Failed to record sale'}), 400


@app.route('/api/sales/<int:sale_id>/payments', methods=['POST'])
def add_sale_payment(sale_id):
    data = request.get_json()
    payment_method = data.get('payment_method')
    amount = data.get('amount')

    if not payment_method or amount is None:
        return jsonify({'error': 'payment_method and amount are required'}), 400

    try:
        amount = float(amount)
        if amount <= 0:
            raise ValueError()
    except (ValueError, TypeError):
        return jsonify({'error': 'Invalid amount'}), 400

    success = db.add_payment_to_sale(sale_id, payment_method, amount)

    if success:
        return jsonify({'message': 'Payment added successfully'}), 201
    else:
        return jsonify({'error': 'Failed to add payment. Sale may not exist or payment exceeds balance.'}), 400

@app.route('/api/sales/<int:sale_id>', methods=['GET'])
def get_sale_details(sale_id):
    sale = db.get_sale_by_id(sale_id)
    if not sale:
        return jsonify({'error': 'Sale not found'}), 404
    
    items = db.get_sale_details(sale_id)
    return jsonify({
        'sale': sale,
        'items': items
    }), 200

# =========================
# SUPPLIER ENDPOINTS
# =========================
@app.route('/api/suppliers', methods=['GET'])
def get_suppliers():
    suppliers = db.get_all_suppliers()
    return jsonify(suppliers), 200

@app.route('/api/suppliers', methods=['POST'])
def create_supplier():
    data = request.get_json()
    name = data.get('name')
    
    if not name:
        return jsonify({'error': 'Supplier name is required'}), 400
    
    success = db.add_supplier(
        name=name,
        contact_person=data.get('contact_person'),
        phone=data.get('phone')
    )
    
    if success:
        return jsonify({'message': 'Supplier created successfully'}), 201
    else:
        return jsonify({'error': 'Supplier already exists'}), 409

@app.route('/api/suppliers/<int:supplier_id>', methods=['GET'])
def get_supplier(supplier_id):
    supplier = db.get_supplier_by_id(supplier_id)
    if supplier:
        return jsonify(supplier), 200
    else:
        return jsonify({'error': 'Supplier not found'}), 404

@app.route('/api/suppliers/<int:supplier_id>', methods=['PUT'])
def update_supplier(supplier_id):
    data = request.get_json()
    
    success = db.update_supplier(
        supplier_id=supplier_id,
        name=data['name'],
        contact_person=data.get('contact_person'),
        phone=data.get('phone')
    )
    
    if success:
        return jsonify({'message': 'Supplier updated successfully'}), 200
    else:
        return jsonify({'error': 'Failed to update supplier'}), 400

@app.route('/api/suppliers/<int:supplier_id>', methods=['DELETE'])
def delete_supplier(supplier_id):
    success = db.delete_supplier(supplier_id)
    if success:
        return jsonify({'message': 'Supplier deleted successfully'}), 200
    else:
        return jsonify({'error': 'Failed to delete supplier'}), 400
    
    
# =========================
# CUSTOMER ENDPOINTS
# =========================
@app.route('/api/customers', methods=['GET'])
def get_customers():
    customers = db.get_all_customers()
    return jsonify(customers), 200

@app.route('/api/customers', methods=['POST'])
def create_customer():
    data = request.get_json()
    name = data.get('name')
    if not name:
        return jsonify({'error': 'Customer name is required'}), 400
    
    customer_id = db.add_customer(
        name=name,
        phone=data.get('phone'),
        email=data.get('email')
    )
    
    if customer_id:
        new_customer = {'id': customer_id, **data}
        return jsonify({'message': 'Customer created successfully', 'customer': new_customer}), 201
    else:
        return jsonify({'error': 'Customer with that name already exists'}), 409

@app.route('/api/customers/<int:customer_id>', methods=['GET'])
def get_customer(customer_id):
    customer = db.get_customer_by_id(customer_id) # You will need to create this function in database.py
    if customer:
        return jsonify(customer), 200
    else:
        return jsonify({'error': 'Customer not found'}), 404

@app.route('/api/customers/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    data = request.get_json()
    name = data.get('name')
    if not name:
        return jsonify({'error': 'Customer name is required'}), 400

    success = db.update_customer( # You will need to create this function in database.py
        customer_id=customer_id,
        name=name,
        phone=data.get('phone'),
        email=data.get('email')
    )
    
    if success:
        return jsonify({'message': 'Customer updated successfully'}), 200
    else:
        return jsonify({'error': 'Failed to update customer or customer not found'}), 400

@app.route('/api/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    success = db.delete_customer(customer_id) # You will need to create this function in database.py
    if success:
        return jsonify({'message': 'Customer deleted successfully'}), 200
    else:
        return jsonify({'error': 'Failed to delete customer, it may be linked to sales'}), 400    

# =========================
# PURCHASE ENDPOINTS
# =========================
@app.route('/api/purchases', methods=['GET'])
def get_purchases():
    purchases = db.get_all_purchases()
    return jsonify(purchases), 200

@app.route('/api/purchases', methods=['POST'])
def create_purchase():
    data = request.get_json()
    supplier_id = data.get('supplier_id')
    purchase_items = data.get('purchase_items')
    
    if not supplier_id or not purchase_items:
        return jsonify({'error': 'Supplier ID and purchase items are required'}), 400
    
    success, purchase_id = db.record_purchase(supplier_id, purchase_items)
    if success:
        return jsonify({'message': 'Purchase recorded successfully', 'purchase_id': purchase_id}), 201
    else:
        return jsonify({'error': 'Failed to record purchase'}), 400

@app.route('/api/purchases/<int:purchase_id>', methods=['GET'])
def get_purchase_details(purchase_id):
    details = db.get_purchase_details(purchase_id)
    if details:
        return jsonify(details), 200
    else:
        return jsonify({'error': 'Purchase not found'}), 404

# =========================
# REPORTING ENDPOINTS
# =========================
@app.route('/api/reports/sales-summary', methods=['GET'])
def get_sales_summary():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    summary = db.get_sales_summary(start_date, end_date)
    return jsonify(summary), 200

@app.route('/api/reports/product-performance', methods=['GET'])
def get_product_performance():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    sort_by = request.args.get('sort_by', 'quantity')
    limit = int(request.args.get('limit', 100))
    
    report = db.get_product_performance_report(start_date, end_date, sort_by, limit)
    return jsonify(report), 200

@app.route('/api/reports/inventory-summary', methods=['GET'])
def get_inventory_summary():
    report = db.get_inventory_summary_report()
    return jsonify(report), 200

@app.route('/api/reports/sales-by-category', methods=['GET'])
def get_sales_by_category():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    report = db.get_sales_by_category_report(start_date, end_date)
    return jsonify(report), 200

@app.route('/api/reports/profit-loss', methods=['GET'])
def get_profit_loss():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    report = db.get_profit_and_loss_statement(start_date, end_date)
    return jsonify(report), 200

@app.route('/api/reports/balance-sheet', methods=['GET'])
def get_balance_sheet():
    report = db.get_balance_sheet()
    return jsonify(report), 200

# =========================
# ACCOUNTING ENDPOINTS
# =========================
@app.route('/api/chart-of-accounts', methods=['GET'])
def get_chart_of_accounts():
    accounts = db.get_chart_of_accounts()
    return jsonify(accounts), 200

@app.route('/api/accounts/<account_type>', methods=['GET'])
def get_accounts_by_type(account_type):
    accounts = db.get_accounts_by_type(account_type)
    return jsonify(accounts), 200

@app.route('/api/journal-entries', methods=['GET'])
def get_journal_entries():
    entries = db.get_journal_entries()
    return jsonify(entries), 200

@app.route('/api/expenses', methods=['POST'])
def create_expense():
    data = request.get_json()
    
    required_fields = ['description', 'amount', 'expense_account', 'payment_account']
    for field in required_fields:
        if field not in data:
            return jsonify({'error': f'{field} is required'}), 400
    
    success = db.add_expense(
        description=data['description'],
        amount=data['amount'],
        expense_account_name=data['expense_account'],
        payment_account_name=data['payment_account']
    )
    
    if success:
        return jsonify({'message': 'Expense recorded successfully'}), 201
    else:
        return jsonify({'error': 'Failed to record expense'}), 400

# =========================
# DASHBOARD ENDPOINTS
# =========================
@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    total_sales = db.get_total_sales_amount()
    total_products = db.get_total_products_count()
    total_categories = db.get_total_categories_count()
    low_stock_count = db.get_low_stock_count()
    weekly_sales = db.get_weekly_sales_summary()
    top_products = db.get_top_selling_products(limit=5)
    
    return jsonify({
        'total_sales': total_sales,
        'total_products': total_products,
        'total_categories': total_categories,
        'low_stock_count': low_stock_count,
        'weekly_sales': weekly_sales,
        'top_products': top_products
    }), 200

# =========================
# ERROR HANDLERS
# =========================
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Resource not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500




