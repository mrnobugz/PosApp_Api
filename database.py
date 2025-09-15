import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys
from datetime import datetime, timedelta
import random
from werkzeug.security import generate_password_hash, check_password_hash
from psycopg2.extras import RealDictCursor

def connect_db():
    """
    Connects to the PostgreSQL database using the DATABASE_URL environment variable.
    """
    try:
        # Connect using the single URL from the environment.
        # This is more secure and standard for cloud apps.
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        
        # This makes the cursor return dictionary-like rows (e.g., row['name'])
        # which is needed for the rest of your code to work as intended.
        conn.cursor_factory = psycopg2.extras.DictCursor
        
        return conn
        
    except (Exception, psycopg2.DatabaseError) as error:
        # This prevents your app from crashing if the database is unavailable.
        print(f"Error connecting to PostgreSQL database: {error}")
        return None

def populate_chart_of_accounts():
    """Populates the CoA with standard accounts for a retail business."""
    conn = connect_db()
    if not conn: return
    try:
        cursor = conn.cursor()
        accounts = [
            # Assets
            ('Cash', 'Asset', None),
            ('Bank', 'Asset', None),
            ('Accounts Receivable', 'Asset', None),
            ('Inventory', 'Asset', None),
            # Liabilities
            ('Accounts Payable', 'Liability', None),
            ('Sales Tax Payable', 'Liability', None),
            # Equity
            ("Owner's Equity", 'Equity', None),
            ('Retained Earnings', 'Equity', None),
            # Revenue
            ('Sales Revenue', 'Revenue', None),
            ('Sales Discounts', 'Revenue', None), # Contra-revenue account
            # Expenses (Cost of Goods Sold)
            ('Cost of Goods Sold', 'Expense', None),
            # Expenses (Operating)
            ('Rent Expense', 'Expense', None),
            ('Utilities Expense', 'Expense', None),
            ('Salaries Expense', 'Expense', None),
            ('Office Supplies Expense', 'Expense', None),
            ('Miscellaneous Expense', 'Expense', None)
        ]
        cursor.executemany(
            "INSERT INTO chart_of_accounts (name, type, parent_id) VALUES (%s, %s, %s)", 
            accounts
        )
        conn.commit()
        print("Default Chart of Accounts populated.")
    except psycopg2.Error as e:
        print(f"Error populating Chart of Accounts: {e}")
        conn.rollback()
    finally:
        conn.close()

def init_db():
    """Initializes the database with all necessary tables if they don't exist."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()

            # --- Customers Table ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    phone TEXT,
                    email TEXT,
                    credit_limit REAL DEFAULT 0.0
                )
            ''')

            # --- Business & Sales Tables ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    price REAL NOT NULL,
                    stock INTEGER NOT NULL,
                    category_id INTEGER,
                    sku TEXT,
                    description TEXT,
                    image_data BYTEA,
                    barcode TEXT UNIQUE,
                    buying_price REAL NOT NULL,
                    low_stock_threshold INTEGER DEFAULT 10,
                    FOREIGN KEY (category_id) REFERENCES categories(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sales (
                    id SERIAL PRIMARY KEY,
                    total_amount REAL NOT NULL,
                    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    discount_amount REAL DEFAULT 0.0,
                    tax_amount REAL DEFAULT 0.0,
                    customer_id INTEGER,
                    status TEXT NOT NULL DEFAULT 'Paid',
                    due_date DATE,
                    FOREIGN KEY (customer_id) REFERENCES customers(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sale_items (
                    id SERIAL PRIMARY KEY,
                    sale_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price_at_sale REAL NOT NULL,
                    FOREIGN KEY (sale_id) REFERENCES sales(id),
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sale_payments (
                    id SERIAL PRIMARY KEY,
                    sale_id INTEGER NOT NULL,
                    payment_method TEXT NOT NULL, -- e.g., 'Cash', 'Credit Card', 'On Credit'
                    amount REAL NOT NULL,
                    FOREIGN KEY (sale_id) REFERENCES sales(id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS suppliers (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    contact_person TEXT,
                    phone TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS purchases (
                    id SERIAL PRIMARY KEY,
                    supplier_id INTEGER NOT NULL,
                    total_cost REAL NOT NULL,
                    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS purchase_items (
                    id SERIAL PRIMARY KEY,
                    purchase_id INTEGER NOT NULL,
                    product_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    cost_at_purchase REAL NOT NULL,
                    FOREIGN KEY (purchase_id) REFERENCES purchases(id),
                    FOREIGN KEY (product_id) REFERENCES products(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'cashier'
                )
            ''')

            # --- Accounting Tables ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chart_of_accounts (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    type TEXT NOT NULL, -- Asset, Liability, Equity, Revenue, Expense
                    parent_id INTEGER,
                    FOREIGN KEY (parent_id) REFERENCES chart_of_accounts(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS journal_entries (
                    id SERIAL PRIMARY KEY,
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT NOT NULL,
                    debit_account_id INTEGER NOT NULL,
                    credit_account_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    reference_id INTEGER, -- e.g., sale_id, purchase_id
                    reference_type TEXT, -- 'sale', 'purchase', 'expense'
                    FOREIGN KEY (debit_account_id) REFERENCES chart_of_accounts(id),
                    FOREIGN KEY (credit_account_id) REFERENCES chart_of_accounts(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS expenses (
                    id SERIAL PRIMARY KEY,
                    description TEXT NOT NULL,
                    amount REAL NOT NULL,
                    expense_account_id INTEGER NOT NULL,
                    payment_account_id INTEGER NOT NULL,
                    expense_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (expense_account_id) REFERENCES chart_of_accounts(id),
                    FOREIGN KEY (payment_account_id) REFERENCES chart_of_accounts(id)
                )
            ''')

            conn.commit()
            print("Database initialized successfully.")

            # Populate Chart of Accounts if it's empty
            cursor.execute("SELECT COUNT(*) FROM chart_of_accounts")
            if cursor.fetchone()['count'] == 0:
                print("Chart of Accounts is empty. Populating with default accounts...")
                populate_chart_of_accounts()

        except psycopg2.Error as e:
            print(f"Error initializing database: {e}")
            conn.rollback()
        finally:
            conn.close()

def get_product_image_data(product_id):
    """Fetches only the BLOB image data for a product."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT image_data FROM products WHERE id = %s", (product_id,))
            row = cursor.fetchone()
            return row['image_data'] if row else None
        except psycopg2.Error as e:
            print(f"Error fetching product image data: {e}")
            return None
        finally:
            conn.close()

def _create_journal_entry(cursor, description, debit_account_name, credit_account_name, amount, ref_id=None, ref_type=None):
    """Internal helper to create a journal entry. Assumes cursor is passed."""
    try:
        # Get account IDs
        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = %s", (debit_account_name,))
        debit_id_row = cursor.fetchone()
        if not debit_id_row: raise Exception(f"Debit account '{debit_account_name}' not found.")
        debit_id = debit_id_row['id']

        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = %s", (credit_account_name,))
        credit_id_row = cursor.fetchone()
        if not credit_id_row: raise Exception(f"Credit account '{credit_account_name}' not found.")
        credit_id = credit_id_row['id']

        entry_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current local time
        cursor.execute("""
            INSERT INTO journal_entries (description, debit_account_id, credit_account_id, amount, reference_id, reference_type, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (description, debit_id, credit_id, amount, ref_id, ref_type, entry_timestamp))
    except Exception as e:
        # Re-raise the exception to be caught by the calling function's transaction block
        raise Exception(f"Failed to create journal entry: {e}")



def get_products_with_filters(category=None, price_min=None, price_max=None, stock_status=None, search_term=None):
    """Get products with advanced filtering capabilities."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        
        # Start with a base query
        query = """
            SELECT p.*, c.name as category_name
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE 1=1
        """
        params = []

        if category and category != "All":
            query += " AND c.name = %s"
            params.append(category)
        
        # Add other filters as needed...

        if search_term:
            query += " AND (p.name ILIKE %s OR p.sku ILIKE %s)"
            params.append(f"%{search_term}%")
            params.append(f"%{search_term}%")

        query += " ORDER BY p.name"
        cursor.execute(query, tuple(params))
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching products with filters: {e}")
        return []
    finally:
        if conn: conn.close()

def record_sale(cart_items, payments, discount_amount=0.0, tax_rate=0.0, customer_id=None, due_date=None):
    """
    Records a sale with tax-inclusive pricing, its payments, and creates corresponding journal entries.
    """
    conn = connect_db()
    if not conn: return False, None
    try:
        cursor = conn.cursor()

        if not cart_items:
             raise Exception("Cart cannot be empty.")
        total_paid = sum(p['amount'] for p in payments) if payments else 0

        # In tax-inclusive pricing, the subtotal is the gross amount before discounts.
        subtotal = sum(item['quantity'] * item['price_at_sale'] for item in cart_items)
        # The final amount the customer owes is the subtotal minus the discount.
        final_total = subtotal - discount_amount
        
        # Determine sale status
        status = 'Paid'
        if customer_id:
            if abs(total_paid - final_total) > 0.01:
                status = 'Partial' if total_paid > 0 else 'Due'
            else:
                status = 'Paid'
        
        # Record the main sale record. 
        # `total_amount` is the final, tax-inclusive price after discount.
        # The `tax_amount` column in the DB stores the tax RATE.
        sale_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            "INSERT INTO sales (total_amount, discount_amount, tax_amount, sale_date, customer_id, status, due_date) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (final_total, discount_amount, tax_rate, sale_timestamp, customer_id, status, due_date)
        )
        sale_id = cursor.fetchone()['id']

        # Record sale items, update stock, and calculate COGS
        total_cogs = 0
        for item in cart_items:
            cursor.execute("INSERT INTO sale_items (sale_id, product_id, quantity, price_at_sale) VALUES (%s, %s, %s, %s)",
                           (sale_id, item['product_id'], item['quantity'], item['price_at_sale']))
            cursor.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (item['quantity'], item['product_id']))

            cursor.execute("SELECT buying_price FROM products WHERE id = %s", (item['product_id'],))
            buying_price = cursor.fetchone()['buying_price'] or 0
            total_cogs += item['quantity'] * buying_price

        # Record each payment method
        if payments:
            for payment in payments:
                cursor.execute("INSERT INTO sale_payments (sale_id, payment_method, amount) VALUES (%s, %s, %s)",
                               (sale_id, payment['method'], payment['amount']))

        # --- Create Corrected Double-Entry Journal Entries for Tax-Inclusive Pricing ---
        
        # 1. Back-calculate gross revenue and tax from the pre-discount subtotal.
        # The total value of the sale (before discount) is credited to revenue and tax accounts,
        # and the full amount is debited to Accounts Receivable.
        gross_revenue = subtotal / (1 + tax_rate) if tax_rate > 0 else subtotal
        gross_tax = subtotal - gross_revenue

        _create_journal_entry(cursor, f"Gross Sale ID: {sale_id}", "Accounts Receivable", "Sales Revenue", gross_revenue, sale_id, 'sale')
        if gross_tax > 0:
            _create_journal_entry(cursor, f"Tax on Sale ID: {sale_id}", "Accounts Receivable", "Sales Tax Payable", gross_tax, sale_id, 'sale')

        # 2. If a discount was given, it reduces the amount receivable and is recorded as a contra-revenue expense.
        if discount_amount > 0:
            _create_journal_entry(cursor, f"Discount for Sale ID: {sale_id}", "Sales Discounts", "Accounts Receivable", discount_amount, sale_id, 'sale')

        # 3. Record payments received, which reduce Accounts Receivable and increase cash/bank assets.
        if payments:
            for payment in payments:
                payment_account_name = 'Bank' if payment['method'].lower() != 'cash' else 'Cash'
                if payment['method'].lower() in ['credit card', 'bank transfer', 'mobile money', 'm-pesa', 'halopesa', 'mix by yas']:
                     payment_account_name = 'Bank'
                
                if payment_account_name:
                    _create_journal_entry(cursor, f"Payment for Sale ID: {sale_id} ({payment['method']})", payment_account_name, "Accounts Receivable", payment['amount'], sale_id, 'sale')

        # 4. Record Cost of Goods Sold, which is an expense and reduces inventory asset.
        if total_cogs > 0:
            _create_journal_entry(cursor, f"COGS for Sale ID: {sale_id}", "Cost of Goods Sold", "Inventory", total_cogs, sale_id, 'sale')

        conn.commit()
        return True, sale_id
    except Exception as e:
        print(f"Error recording sale with journal entry: {e}")
        if conn: conn.rollback()
        return False, None
    finally:
        if conn: conn.close()

def add_payment_to_sale(sale_id, payment_method, amount):
    """Adds a new payment to an existing sale and updates the sale's status."""
    conn = connect_db()
    if not conn: return False
    try:
        cursor = conn.cursor()
        
        cursor.execute("INSERT INTO sale_payments (sale_id, payment_method, amount) VALUES (%s, %s, %s)",
                       (sale_id, payment_method, amount))

        # Update the sale's status
        cursor.execute("SELECT total_amount FROM sales WHERE id = %s", (sale_id,))
        total_amount = cursor.fetchone()['total_amount']
        
        cursor.execute("SELECT SUM(amount) FROM sale_payments WHERE sale_id = %s", (sale_id,))
        total_paid = cursor.fetchone()['sum'] or 0.0

        new_status = 'Partial'
        if abs(total_paid - total_amount) < 0.01:
            new_status = 'Paid'
        
        cursor.execute("UPDATE sales SET status = %s WHERE id = %s", (new_status, sale_id))

        payment_account_name = 'Bank' if payment_method.lower() != 'cash' else 'Cash'
        _create_journal_entry(cursor, f"Additional payment for Sale ID: {sale_id}",
                              payment_account_name, "Accounts Receivable", amount, sale_id, 'sale_payment')
        
        conn.commit()
        return True
    except Exception as e:
        print(f"Database error while adding payment to sale {sale_id}: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

def get_sales_details_for_history(start_date, end_date):
    """
    Fetches sales for history, now including customer name and persisted status.
    """
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                s.id,
                s.sale_date,
                s.total_amount,
                s.status,
                COALESCE(c.name, 'N/A') as customer_name,
                (SELECT SUM(amount) FROM sale_payments WHERE sale_id = s.id) as paid_due,
                (SELECT COUNT(id) FROM sale_items WHERE sale_id = s.id) as total_items
            FROM sales s
            LEFT JOIN customers c ON s.customer_id = c.id
            WHERE s.sale_date BETWEEN %s AND %s
            ORDER BY s.sale_date DESC
        """
        cursor.execute(query, (start_date, f"{end_date} 23:59:59"))
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching detailed sales history: {e}")
        return []
    finally:
        if conn: conn.close()

def get_sale_payments(sale_id):
    """Fetches all payments associated with a specific sale ID."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT payment_method, amount FROM sale_payments WHERE sale_id = %s", (sale_id,))
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching payments for sale ID {sale_id}: {e}")
        return []
    finally:
        if conn: conn.close()
        
def get_customer_by_id(customer_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM customers WHERE id = %s", (customer_id,))
            return cursor.fetchone()
        except psycopg2.Error as e:
            print(f"Error fetching customer by ID: {e}")
            return None
        finally:
            conn.close()

def update_customer(customer_id, name, phone=None, email=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE customers SET name = %s, phone = %s, email = %s WHERE id = %s
            """, (name, phone, email, customer_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.IntegrityError:
            return False # Name likely exists
        except psycopg2.Error as e:
            print(f"Error updating customer: {e}")
            return False
        finally:
            conn.close()

def delete_customer(customer_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Check if customer is linked to sales
            cursor.execute("SELECT 1 FROM sales WHERE customer_id = %s LIMIT 1", (customer_id,))
            if cursor.fetchone():
                return False # Cannot delete if linked
            
            cursor.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error deleting customer: {e}")
            return False
        finally:
            conn.close()        

def record_purchase(supplier_id, purchase_items_list):
    """Records a purchase and creates corresponding double-entry journal records."""
    conn = connect_db()
    if not conn: return False, None
    try:
        cursor = conn.cursor()
        total_cost = sum(item['quantity'] * item['cost'] for item in purchase_items_list)
        purchase_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current local time

        cursor.execute("INSERT INTO purchases (supplier_id, total_cost, purchase_date) VALUES (%s, %s, %s) RETURNING id", 
                      (supplier_id, total_cost, purchase_timestamp))
        purchase_id = cursor.fetchone()['id']

        for item in purchase_items_list:
            cursor.execute("INSERT INTO purchase_items (purchase_id, product_id, quantity, cost_at_purchase) VALUES (%s, %s, %s, %s)",
                        (purchase_id, item['product_id'], item['quantity'], item['cost']))
            
            cursor.execute("""
                    UPDATE products 
                    SET stock = stock + %s, 
                        price = %s, 
                        buying_price = %s 
                    WHERE id = %s
                """, (item['quantity'], item['new_price'], item['cost'], item['product_id']))
                        
        # Create Journal Entry for the purchase
        # A purchase increases the Inventory asset and creates a liability (Accounts Payable)
        _create_journal_entry(cursor, f"Purchase from supplier - ID: {purchase_id}", "Inventory", "Accounts Payable", total_cost, purchase_id, 'purchase')

        conn.commit()
        return True, purchase_id
    except Exception as e:
        print(f"Error recording purchase with journal entry: {e}")
        if conn: conn.rollback()
        return False, None
    finally:
        if conn: conn.close()

def delete_sale_by_id(sale_id):
    """
    Deletes a sale and all related records, including items, payments,
    and journal entries, and correctly adjusts product stock.
    """
    conn = connect_db()
    if not conn: return False
    try:
        cursor = conn.cursor()
        
        # 1. Get sale items to revert stock before deleting them
        cursor.execute("SELECT product_id, quantity FROM sale_items WHERE sale_id = %s", (sale_id,))
        items_to_revert = cursor.fetchall()
        
        # 2. Revert product stock
        for item in items_to_revert:
            cursor.execute("UPDATE products SET stock = stock + %s WHERE id = %s", (item['quantity'], item['product_id']))

        # 3. Delete related records in the correct order (children first)
        cursor.execute("DELETE FROM journal_entries WHERE reference_type = 'sale' AND reference_id = %s", (sale_id,))
        cursor.execute("DELETE FROM sale_payments WHERE sale_id = %s", (sale_id,))
        cursor.execute("DELETE FROM sale_items WHERE sale_id = %s", (sale_id,))
        
        # 4. Finally, delete the main sale record
        cursor.execute("DELETE FROM sales WHERE id = %s", (sale_id,))
        
        conn.commit()
        return cursor.rowcount > 0
    except psycopg2.Error as e:
        print(f"Error deleting sale ID {sale_id}: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()        

def add_expense(description, amount, expense_account_name, payment_account_name):
    """Adds an expense and creates the corresponding journal entry."""
    conn = connect_db()
    if not conn: return False
    try:
        cursor = conn.cursor()

        # Get account IDs
        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = %s", (expense_account_name,))
        expense_account_id_row = cursor.fetchone()
        if not expense_account_id_row: raise Exception(f"Expense account '{expense_account_name}' not found.")
        expense_account_id = expense_account_id_row['id']

        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = %s", (payment_account_name,))
        payment_account_id_row = cursor.fetchone()
        if not payment_account_id_row: raise Exception(f"Payment account '{payment_account_name}' not found.")
        payment_account_id = payment_account_id_row['id']

        # Insert into expenses table
        expense_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current local time
        cursor.execute("INSERT INTO expenses (description, amount, expense_account_id, payment_account_id, expense_date) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (description, amount, expense_account_id, payment_account_id, expense_timestamp))
        expense_id = cursor.fetchone()['id']

        # Create the journal entry
        # The expense account is debited, and the payment account (an asset) is credited
        _create_journal_entry(cursor, description, expense_account_name, payment_account_name, amount, expense_id, 'expense')

        conn.commit()
        return True
    except Exception as e:
        print(f"Error adding expense with journal entry: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

def get_chart_of_accounts():
    """Fetches all accounts from the Chart of Accounts."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id, name, type, parent_id FROM chart_of_accounts ORDER BY type, name")
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching chart of accounts: {e}")
        return []
    finally:
        conn.close()

def get_accounts_by_type(account_type):
    """Fetches all accounts of a specific type."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT id, name FROM chart_of_accounts WHERE type = %s ORDER BY name", (account_type,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching accounts by type: {e}")
        return []
    finally:
        conn.close()

def get_journal_entries():
    """Fetches all journal entries for display in the General Ledger."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT
                je.date,
                je.description,
                je.amount,
                (SELECT name FROM chart_of_accounts WHERE id = je.debit_account_id) as debit_account,
                (SELECT name FROM chart_of_accounts WHERE id = je.credit_account_id) as credit_account
            FROM journal_entries je
            ORDER BY je.date DESC, je.id DESC
        """)

        # Unwind the single entry into two rows for traditional ledger view
        ledger_lines = []
        for row in cursor.fetchall():
            ledger_lines.append({
                'date': row['date'],
                'account_name': row['debit_account'],
                'description': row['description'],
                'debit': row['amount'],
                'credit': None
            })
            ledger_lines.append({
                'date': row['date'],
                'account_name': row['credit_account'],
                'description': row['description'],
                'debit': None,
                'credit': row['amount']
            })
        return ledger_lines
    except Exception as e:
        print(f"Error fetching journal entries: {e}")
        return []
    finally:
        conn.close()

def get_account_balance(account_names, start_date=None, end_date=None):
    """Calculates the balance for a given list of account names."""
    conn = connect_db()
    if not conn: return 0.0
    try:
        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(account_names))
        cursor.execute(f"SELECT id, name, type FROM chart_of_accounts WHERE name IN ({placeholders})", account_names)
        accounts = cursor.fetchall()
        if not accounts: return 0.0

        account_ids = [acc['id'] for acc in accounts]
        account_type = accounts[0]['type'] # Assume all passed accounts are of the same base type

        # Build date conditions if provided
        date_condition = ""
        params_debit = list(account_ids)
        params_credit = list(account_ids)

        if start_date:
            date_condition += " AND date >= %s"
            params_debit.append(start_date)
            params_credit.append(start_date)
        if end_date:
            # For Balance Sheet, end_date is inclusive of the whole day
            date_condition += " AND date <= %s"
            params_debit.append(f"{end_date} 23:59:59")
            params_credit.append(f"{end_date} 23:59:59")

        # Calculate debits
        debit_query = f"""
            SELECT SUM(amount) FROM journal_entries
            WHERE debit_account_id IN ({','.join(['%s'] * len(account_ids))}) {date_condition}
        """
        cursor.execute(debit_query, params_debit)
        total_debits = cursor.fetchone()['sum'] or 0.0

        # Calculate credits
        credit_query = f"""
            SELECT SUM(amount) FROM journal_entries
            WHERE credit_account_id IN ({','.join(['%s'] * len(account_ids))}) {date_condition}
        """
        cursor.execute(credit_query, params_credit)
        total_credits = cursor.fetchone()['id'] or 0.0

        # Balance calculation depends on the account type
        if account_type in ['Asset', 'Expense']:
            return total_debits - total_credits
        elif account_type in ['Liability', 'Equity', 'Revenue']:
            # For contra-revenue like 'Sales Discounts', this will correctly show a negative value
            return total_credits - total_debits

        return 0.0

    except Exception as e:
        print(f"Error calculating account balance for {account_names}: {e}")
        return 0.0
    finally:
        if conn: conn.close()

def get_profit_and_loss_statement(start_date=None, end_date=None):
    """Generates data for a Profit and Loss statement."""
    total_revenue = get_account_balance(['Sales Revenue'], start_date, end_date)
    # get_account_balance for a debit-balance account like 'Sales Discounts' will return a positive value.
    sales_discounts = get_account_balance(['Sales Discounts'], start_date, end_date)
    net_revenue = total_revenue - sales_discounts

    total_cogs = get_account_balance(['Cost of Goods Sold'], start_date, end_date)
    gross_profit = net_revenue - total_cogs

    expense_accounts = [acc['name'] for acc in get_accounts_by_type('Expense') if acc['name'] != 'Cost of Goods Sold']

    expense_details = []
    total_expenses = 0
    for acc_name in expense_accounts:
        balance = get_account_balance([acc_name], start_date, end_date)
        if balance > 0:
            expense_details.append({'category': acc_name, 'amount': balance})
            total_expenses += balance

    net_profit = gross_profit - total_expenses

    return {
        'revenue': {'total_revenue': total_revenue, 'discounts': sales_discounts},
        'net_revenue': net_revenue,
        'cogs': {'total_cogs': total_cogs},
        'gross_profit': gross_profit,
        'expenses': {'details': expense_details, 'total_expenses': total_expenses},
        'net_profit': net_profit
    }

def get_balance_sheet(end_date=None):
    """Generates data for a Balance Sheet as of a specific end date."""
    # Assets
    asset_accounts = [acc['name'] for acc in get_accounts_by_type('Asset')]
    asset_details = []
    total_assets = 0
    for acc_name in asset_accounts:
        balance = get_account_balance([acc_name], end_date=end_date)
        asset_details.append({'name': acc_name, 'balance': balance})
        total_assets += balance

    # Liabilities
    liability_accounts = [acc['name'] for acc in get_accounts_by_type('Liability')]
    liability_details = []
    total_liabilities = 0
    for acc_name in liability_accounts:
        balance = get_account_balance([acc_name], end_date=end_date)
        liability_details.append({'name': acc_name, 'balance': balance})
        total_liabilities += balance

    # Equity
    # Calculate the net profit for the period, which represents the Retained Earnings.
    pnl_data = get_profit_and_loss_statement(end_date=end_date)
    current_period_profit = pnl_data['net_profit']

    equity_accounts = [acc['name'] for acc in get_accounts_by_type('Equity')]
    equity_details = []
    total_equity = 0
    for acc_name in equity_accounts:
        # Get the base balance (e.g., for Owner's initial contribution)
        balance = get_account_balance([acc_name], end_date=end_date)
        
        # Specifically assign the calculated profit to the 'Retained Earnings' line
        if acc_name == 'Retained Earnings':
            balance = current_period_profit
        
        equity_details.append({'name': acc_name, 'balance': balance})
        total_equity += balance

    return {
        'assets': {'details': asset_details, 'total': total_assets},
        'liabilities': {'details': liability_details, 'total': total_liabilities},
        'equity': {'details': equity_details, 'total': total_equity},
        'total_liabilities_and_equity': total_liabilities + total_equity
    }
    
# --- Customer Management Functions ---
def add_customer(name, phone=None, email=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO customers (name, phone, email) VALUES (%s, %s, %s) RETURNING id",
                           (name, phone, email))
            customer_id = cursor.fetchone()['id']
            conn.commit()
            return customer_id
        except psycopg2.IntegrityError:
            print(f"Customer with name '{name}' already exists.")
            return None
        except psycopg2.Error as e:
            print(f"Error adding customer: {e}")
            return None
        finally:
            conn.close()

def get_all_customers():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM customers ORDER BY name")
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching customers: {e}")
            return []
        finally:
            conn.close()

def get_customer_ledger_summary():
    """
    Calculates the ledger summary for all customers with outstanding balances.
    Returns a list of dictionaries with customer info and their debt summary.
    """
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
            SELECT
                c.id as customer_id,
                c.name as customer_name,
                c.phone as customer_phone,
                SUM(s.total_amount) as total_debt,
                (SELECT SUM(p.amount)
                 FROM sale_payments p
                 JOIN sales s_inner ON p.sale_id = s_inner.id
                 WHERE s_inner.customer_id = c.id) as total_paid,
                MIN(s.due_date) as earliest_due_date
            FROM customers c
            JOIN sales s ON c.id = s.customer_id
            WHERE s.status IN ('Due', 'Partial')
            GROUP BY c.id, c.name, c.phone
            HAVING (total_debt - total_paid) > 0.01
            ORDER BY c.name;
        """
        cursor.execute(query)
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching customer ledger summary: {e}")
        return []
    finally:
        if conn: conn.close()

# --- Product Management Functions ---
def add_product(name, price, stock, category_id=None, sku=None, description=None, image_data=None, barcode=None, buying_price=0.0, low_stock_threshold=10):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO products (name, price, stock, category_id, sku, description, image_data, barcode, buying_price, low_stock_threshold)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (name, price, stock, category_id, sku, description, image_data, barcode, buying_price, low_stock_threshold))
            product_id = cursor.fetchone()['id']
            conn.commit()
            return product_id
        except psycopg2.IntegrityError:
            print(f"Product with name '{name}' or barcode '{barcode}' already exists.")
            return None
        except psycopg2.Error as e:
            print(f"Error adding product: {e}")
            return None
        finally:
            conn.close()

def get_all_products():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT p.*, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                ORDER BY p.name
            """)
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching products: {e}")
            return []
        finally:
            conn.close()

def update_product(product_id, name, price, stock, category_id=None, sku=None, description=None, image_data=None, barcode=None, buying_price=0.0, low_stock_threshold=10):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            if image_data is not None:
                cursor.execute("""
                    UPDATE products SET name=%s, price=%s, stock=%s, category_id=%s, sku=%s, description=%s, image_data=%s, barcode=%s, buying_price=%s, low_stock_threshold=%s
                    WHERE id=%s
                """, (name, price, stock, category_id, sku, description, image_data, barcode, buying_price, low_stock_threshold, product_id))
            else:
                cursor.execute("""
                    UPDATE products SET name=%s, price=%s, stock=%s, category_id=%s, sku=%s, description=%s, barcode=%s, buying_price=%s, low_stock_threshold=%s
                    WHERE id=%s
                """, (name, price, stock, category_id, sku, description, barcode, buying_price, low_stock_threshold, product_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.IntegrityError:
            print(f"Product with name '{name}' or barcode '{barcode}' already exists.")
            return False
        except psycopg2.Error as e:
            print(f"Error updating product: {e}")
            return False
        finally:
            conn.close()

def delete_product(product_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Check if product is linked to sales
            cursor.execute("SELECT 1 FROM sale_items WHERE product_id = %s LIMIT 1", (product_id,))
            if cursor.fetchone():
                return False # Cannot delete if linked
            
            cursor.execute("DELETE FROM products WHERE id = %s", (product_id,))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error deleting product: {e}")
            return False
        finally:
            conn.close()

def get_low_stock_products():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT p.*, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.stock <= p.low_stock_threshold
                ORDER BY p.stock ASC
            """)
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching low stock products: {e}")
            return []
        finally:
            conn.close()

# --- Category Management Functions ---
def add_category(name):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO categories (name) VALUES (%s) RETURNING id", (name,))
            category_id = cursor.fetchone()['id']
            conn.commit()
            return category_id
        except psycopg2.IntegrityError:
            print(f"Category with name '{name}' already exists.")
            return None
        except psycopg2.Error as e:
            print(f"Error adding category: {e}")
            return None
        finally:
            conn.close()

def get_all_categories():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM categories ORDER BY name")
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching categories: {e}")
            return []
        finally:
            conn.close()

def update_category(category_id, name):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE categories SET name = %s WHERE id = %s", (name, category_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.IntegrityError:
            print(f"Category with name '{name}' already exists.")
            return False
        except psycopg2.Error as e:
            print(f"Error updating category: {e}")
            return False
        finally:
            conn.close()

def delete_category(category_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Check if category is linked to products
            cursor.execute("SELECT 1 FROM products WHERE category_id = %s LIMIT 1", (category_id,))
            if cursor.fetchone():
                return False # Cannot delete if linked
            
            cursor.execute("DELETE FROM categories WHERE id = %s", (category_id,))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error deleting category: {e}")
            return False
        finally:
            conn.close()

# --- Supplier Management Functions ---
def add_supplier(name, contact_person=None, phone=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO suppliers (name, contact_person, phone) VALUES (%s, %s, %s) RETURNING id",
                           (name, contact_person, phone))
            supplier_id = cursor.fetchone()['id']
            conn.commit()
            return supplier_id
        except psycopg2.IntegrityError:
            print(f"Supplier with name '{name}' already exists.")
            return None
        except psycopg2.Error as e:
            print(f"Error adding supplier: {e}")
            return None
        finally:
            conn.close()

def get_all_suppliers():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM suppliers ORDER BY name")
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching suppliers: {e}")
            return []
        finally:
            conn.close()

def update_supplier(supplier_id, name, contact_person=None, phone=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE suppliers SET name = %s, contact_person = %s, phone = %s WHERE id = %s",
                           (name, contact_person, phone, supplier_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.IntegrityError:
            print(f"Supplier with name '{name}' already exists.")
            return False
        except psycopg2.Error as e:
            print(f"Error updating supplier: {e}")
            return False
        finally:
            conn.close()

def delete_supplier(supplier_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Check if supplier is linked to purchases
            cursor.execute("SELECT 1 FROM purchases WHERE supplier_id = %s LIMIT 1", (supplier_id,))
            if cursor.fetchone():
                return False # Cannot delete if linked
            
            cursor.execute("DELETE FROM suppliers WHERE id = %s", (supplier_id,))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error deleting supplier: {e}")
            return False
        finally:
            conn.close()

# --- User Management Functions ---
def add_user(username, password, role='cashier'):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            password_hash = generate_password_hash(password)
            cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id",
                           (username, password_hash, role))
            user_id = cursor.fetchone()['id']
            conn.commit()
            return user_id
        except psycopg2.IntegrityError:
            print(f"User with username '{username}' already exists.")
            return None
        except psycopg2.Error as e:
            print(f"Error adding user: {e}")
            return None
        finally:
            conn.close()

def get_user_by_username(username):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
            return cursor.fetchone()
        except psycopg2.Error as e:
            print(f"Error fetching user: {e}")
            return None
        finally:
            conn.close()

def verify_user(username, password):
    user = get_user_by_username(username)
    if user and check_password_hash(user['password_hash'], password):
        return user
    return None

def get_all_users():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT id, username, role FROM users ORDER BY username")
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching users: {e}")
            return []
        finally:
            conn.close()

def update_user_password(user_id, new_password):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            password_hash = generate_password_hash(new_password)
            cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (password_hash, user_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error updating user password: {e}")
            return False
        finally:
            conn.close()

def update_user_role(user_id, new_role):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET role = %s WHERE id = %s", (new_role, user_id))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error updating user role: {e}")
            return False
        finally:
            conn.close()

def delete_user(user_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
        except psycopg2.Error as e:
            print(f"Error deleting user: {e}")
            return False
        finally:
            conn.close()

# --- Dashboard Data Functions ---
def get_dashboard_data():
    """Fetches data for the dashboard including sales, stock alerts, and financial metrics."""
    conn = connect_db()
    if not conn: return {}
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Today's sales
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total FROM sales WHERE DATE(sale_date) = %s", (today,))
        today_sales = cursor.fetchone()
        
        # Low stock alerts
        cursor.execute("SELECT COUNT(*) as count FROM products WHERE stock <= low_stock_threshold")
        low_stock_count = cursor.fetchone()['count']
        
        # Total customers
        cursor.execute("SELECT COUNT(*) as count FROM customers")
        customer_count = cursor.fetchone()['count']
        
        # Recent sales
        cursor.execute("""
            SELECT s.id, s.sale_date, s.total_amount, c.name as customer_name
            FROM sales s
            LEFT JOIN customers c ON s.customer_id = c.id
            ORDER BY s.sale_date DESC LIMIT 5
        """)
        recent_sales = cursor.fetchall()
        
        # Top selling products
        cursor.execute("""
            SELECT p.name, SUM(si.quantity) as total_sold
            FROM sale_items si
            JOIN products p ON si.product_id = p.id
            GROUP BY p.name
            ORDER BY total_sold DESC LIMIT 5
        """)
        top_products = cursor.fetchall()
        
        return {
            'today_sales_count': today_sales['count'],
            'today_sales_total': today_sales['total'],
            'low_stock_count': low_stock_count,
            'customer_count': customer_count,
            'recent_sales': recent_sales,
            'top_products': top_products
        }
    except psycopg2.Error as e:
        print(f"Error fetching dashboard data: {e}")
        return {}
    finally:
        conn.close()

def get_sales_over_time(days=30):
    """Fetches sales data over time for charts."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        cursor.execute("""
            SELECT DATE(sale_date) as date, COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total
            FROM sales
            WHERE sale_date BETWEEN %s AND %s
            GROUP BY DATE(sale_date)
            ORDER BY date
        """, (start_date, end_date))
        
        return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Error fetching sales over time: {e}")
        return []
    finally:
        conn.close()

# --- Search Functions ---
def search_products(query):
    """Searches for products by name, SKU, or barcode."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            search_pattern = f"%{query}%"
            cursor.execute("""
                SELECT p.*, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.name ILIKE %s OR p.sku ILIKE %s OR p.barcode ILIKE %s
                ORDER BY p.name
            """, (search_pattern, search_pattern, search_pattern))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error searching products: {e}")
            return []
        finally:
            conn.close()

def search_customers(query):
    """Searches for customers by name, phone, or email."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            search_pattern = f"%{query}%"
            cursor.execute("""
                SELECT *
                FROM customers
                WHERE name ILIKE %s OR phone ILIKE %s OR email ILIKE %s
                ORDER BY name
            """, (search_pattern, search_pattern, search_pattern))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error searching customers: {e}")
            return []
        finally:
            conn.close()

def search_sales(query):
    """Searches for sales by ID, customer name, or date."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            search_pattern = f"%{query}%"
            cursor.execute("""
                SELECT s.*, c.name as customer_name
                FROM sales s
                LEFT JOIN customers c ON s.customer_id = c.id
                WHERE CAST(s.id AS TEXT) ILIKE %s OR c.name ILIKE %s OR DATE(s.sale_date)::TEXT ILIKE %s
                ORDER BY s.sale_date DESC
            """, (search_pattern, search_pattern, search_pattern))
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error searching sales: {e}")
            return []
        finally:
            conn.close()

# --- Backup and Restore Functions ---
def backup_database(backup_path):
    """Creates a backup of the database."""
    try:
        import subprocess
        # Using pg_dump to create a backup
        cmd = [
            'pg_dump',
            '-h', DB_HOST,
            '-U', DB_USER,
            '-d', DB_NAME,
            '-f', backup_path
        ]
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_PASSWORD
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Backup failed: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"Error creating backup: {e}")
        return False

def restore_database(backup_path):
    """Restores the database from a backup."""
    try:
        import subprocess
        # Using psql to restore from backup
        cmd = [
            'psql',
            '-h', DB_HOST,
            '-U', DB_USER,
            '-d', DB_NAME,
            '-f', backup_path
        ]
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_PASSWORD
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Restore failed: {result.stderr}")
            return False
        return True
    except Exception as e:
        print(f"Error restoring database: {e}")
        return False

if __name__ == "__main__":
    # Initialize the database when this script is run directly
    init_db()
    print("Database setup complete.")
