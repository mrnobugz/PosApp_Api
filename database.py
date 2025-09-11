import sqlite3
import os
import sys
from datetime import datetime, timedelta
import random
from werkzeug.security import generate_password_hash, check_password_hash

DATABASE_NAME = 'pos_system.db'

def get_db_path():
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, DATABASE_NAME)
    else:
        return os.path.join(os.getcwd(), DATABASE_NAME)


def connect_db():
    """Connects to the SQLite database."""
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row # This allows accessing columns by name
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
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
        cursor.executemany("INSERT INTO chart_of_accounts (name, type, parent_id) VALUES (?, ?, ?)", accounts)
        conn.commit()
        print("Default Chart of Accounts populated.")
    except sqlite3.Error as e:
        print(f"Error populating Chart of Accounts: {e}")
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    phone TEXT,
                    email TEXT,
                    credit_limit REAL DEFAULT 0.0
                )
            ''')

            # --- Business & Sales Tables ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    price REAL NOT NULL,
                    stock INTEGER NOT NULL,
                    category_id INTEGER,
                    sku TEXT,
                    description TEXT,
                    image_data BLOB,
                    barcode TEXT UNIQUE,
                    buying_price REAL NOT NULL,
                    low_stock_threshold INTEGER DEFAULT 10,
                    FOREIGN KEY (category_id) REFERENCES categories(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sale_id INTEGER NOT NULL,
                    payment_method TEXT NOT NULL, -- e.g., 'Cash', 'Credit Card', 'On Credit'
                    amount REAL NOT NULL,
                    FOREIGN KEY (sale_id) REFERENCES sales(id)
                )
            ''')


            cursor.execute('''
                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    contact_person TEXT,
                    phone TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS purchases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    supplier_id INTEGER NOT NULL,
                    total_cost REAL NOT NULL,
                    purchase_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS purchase_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'cashier'
                )
            ''')

            # --- Accounting Tables ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS chart_of_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    type TEXT NOT NULL, -- Asset, Liability, Equity, Revenue, Expense
                    parent_id INTEGER,
                    FOREIGN KEY (parent_id) REFERENCES chart_of_accounts(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS journal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            if cursor.fetchone()[0] == 0:
                print("Chart of Accounts is empty. Populating with default accounts...")
                populate_chart_of_accounts()

        except sqlite3.Error as e:
            print(f"Error initializing database: {e}")
        finally:
            conn.close()



def get_product_image_data(product_id):
    """Fetches only the BLOB image data for a product."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT image_data FROM products WHERE id = ?", (product_id,))
            row = cursor.fetchone()
            return row['image_data'] if row else None
        except sqlite3.Error as e:
            print(f"Error fetching product image data: {e}")
            return None
        finally:
            conn.close()

def _create_journal_entry(cursor, description, debit_account_name, credit_account_name, amount, ref_id=None, ref_type=None):
    """Internal helper to create a journal entry. Assumes cursor is passed."""
    try:
        # Get account IDs
        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = ?", (debit_account_name,))
        debit_id_row = cursor.fetchone()
        if not debit_id_row: raise Exception(f"Debit account '{debit_account_name}' not found.")
        debit_id = debit_id_row[0]

        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = ?", (credit_account_name,))
        credit_id_row = cursor.fetchone()
        if not credit_id_row: raise Exception(f"Credit account '{credit_account_name}' not found.")
        credit_id = credit_id_row[0]

        entry_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current local time
        cursor.execute("""
            INSERT INTO journal_entries (description, debit_account_id, credit_account_id, amount, reference_id, reference_type, date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (description, debit_id, credit_id, amount, ref_id, ref_type, entry_timestamp))
    except Exception as e:
        # Re-raise the exception to be caught by the calling function's transaction block
        raise Exception(f"Failed to create journal entry: {e}")


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
            "INSERT INTO sales (total_amount, discount_amount, tax_amount, sale_date, customer_id, status, due_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (final_total, discount_amount, tax_rate, sale_timestamp, customer_id, status, due_date)
        )
        sale_id = cursor.lastrowid

        # Record sale items, update stock, and calculate COGS
        total_cogs = 0
        for item in cart_items:
            cursor.execute("INSERT INTO sale_items (sale_id, product_id, quantity, price_at_sale) VALUES (?, ?, ?, ?)",
                           (sale_id, item['product_id'], item['quantity'], item['price_at_sale']))
            cursor.execute("UPDATE products SET stock = stock - ? WHERE id = ?", (item['quantity'], item['product_id']))

            cursor.execute("SELECT buying_price FROM products WHERE id = ?", (item['product_id'],))
            buying_price = cursor.fetchone()['buying_price'] or 0
            total_cogs += item['quantity'] * buying_price

        # Record each payment method
        if payments:
            for payment in payments:
                cursor.execute("INSERT INTO sale_payments (sale_id, payment_method, amount) VALUES (?, ?, ?)",
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
        
        cursor.execute("INSERT INTO sale_payments (sale_id, payment_method, amount) VALUES (?, ?, ?)",
                       (sale_id, payment_method, amount))

        # Update the sale's status
        cursor.execute("SELECT total_amount FROM sales WHERE id = ?", (sale_id,))
        total_amount = cursor.fetchone()['total_amount']
        
        cursor.execute("SELECT SUM(amount) FROM sale_payments WHERE sale_id = ?", (sale_id,))
        total_paid = cursor.fetchone()[0] or 0.0

        new_status = 'Partial'
        if abs(total_paid - total_amount) < 0.01:
            new_status = 'Paid'
        
        cursor.execute("UPDATE sales SET status = ? WHERE id = ?", (new_status, sale_id))

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
        cursor = conn.cursor()
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
            WHERE s.sale_date BETWEEN ? AND ?
            ORDER BY s.sale_date DESC
        """
        cursor.execute(query, (start_date, f"{end_date} 23:59:59"))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching detailed sales history: {e}")
        return []
    finally:
        if conn: conn.close()

def get_sale_payments(sale_id):
    """Fetches all payments associated with a specific sale ID."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT payment_method, amount FROM sale_payments WHERE sale_id = ?", (sale_id,))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching payments for sale ID {sale_id}: {e}")
        return []
    finally:
        if conn: conn.close()
        
        
        
def get_customer_by_id(customer_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM customers WHERE id = ?", (customer_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
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
                UPDATE customers SET name = ?, phone = ?, email = ? WHERE id = ?
            """, (name, phone, email, customer_id))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            return False # Name likely exists
        except sqlite3.Error as e:
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
            cursor.execute("SELECT 1 FROM sales WHERE customer_id = ? LIMIT 1", (customer_id,))
            if cursor.fetchone():
                return False # Cannot delete if linked
            
            cursor.execute("DELETE FROM customers WHERE id = ?", (customer_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
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

        cursor.execute("INSERT INTO purchases (supplier_id, total_cost, purchase_date) VALUES (?, ?, ?)", (supplier_id, total_cost, purchase_timestamp))
        purchase_id = cursor.lastrowid


        for item in purchase_items_list:
            cursor.execute("INSERT INTO purchase_items (purchase_id, product_id, quantity, cost_at_purchase) VALUES (?, ?, ?, ?)",
                        (purchase_id, item['product_id'], item['quantity'], item['cost']))
            
            cursor.execute("""
                    UPDATE products 
                    SET stock = stock + ?, 
                        price = ?, 
                        buying_price = ? 
                    WHERE id = ?
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
        cursor.execute("SELECT product_id, quantity FROM sale_items WHERE sale_id = ?", (sale_id,))
        items_to_revert = cursor.fetchall()
        
        # 2. Revert product stock
        for item in items_to_revert:
            cursor.execute("UPDATE products SET stock = stock + ? WHERE id = ?", (item['quantity'], item['product_id']))

        # 3. Delete related records in the correct order (children first)
        cursor.execute("DELETE FROM journal_entries WHERE reference_type = 'sale' AND reference_id = ?", (sale_id,))
        cursor.execute("DELETE FROM sale_payments WHERE sale_id = ?", (sale_id,))
        cursor.execute("DELETE FROM sale_items WHERE sale_id = ?", (sale_id,))
        
        # 4. Finally, delete the main sale record
        cursor.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
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
        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = ?", (expense_account_name,))
        expense_account_id_row = cursor.fetchone()
        if not expense_account_id_row: raise Exception(f"Expense account '{expense_account_name}' not found.")
        expense_account_id = expense_account_id_row[0]

        cursor.execute("SELECT id FROM chart_of_accounts WHERE name = ?", (payment_account_name,))
        payment_account_id_row = cursor.fetchone()
        if not payment_account_id_row: raise Exception(f"Payment account '{payment_account_name}' not found.")
        payment_account_id = payment_account_id_row[0]

        # Insert into expenses table
        expense_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Get current local time
        cursor.execute("INSERT INTO expenses (description, amount, expense_account_id, payment_account_id, expense_date) VALUES (?, ?, ?, ?, ?)",
                    (description, amount, expense_account_id, payment_account_id, expense_timestamp))
        expense_id = cursor.lastrowid

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
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, type, parent_id FROM chart_of_accounts ORDER BY type, name")
        return [dict(row) for row in cursor.fetchall()]
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
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM chart_of_accounts WHERE type = ? ORDER BY name", (account_type,))
        return [dict(row) for row in cursor.fetchall()]
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
        cursor = conn.cursor()
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

        placeholders = ','.join('?' for name in account_names)
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
            date_condition += " AND date >= ?"
            params_debit.append(start_date)
            params_credit.append(start_date)
        if end_date:
            # For Balance Sheet, end_date is inclusive of the whole day
            date_condition += " AND date <= ?"
            params_debit.append(f"{end_date} 23:59:59")
            params_credit.append(f"{end_date} 23:59:59")

        # Calculate debits
        debit_query = f"""
            SELECT SUM(amount) FROM journal_entries
            WHERE debit_account_id IN ({','.join('?' for _ in account_ids)}) {date_condition}
        """
        cursor.execute(debit_query, params_debit)
        total_debits = cursor.fetchone()[0] or 0.0

        # Calculate credits
        credit_query = f"""
            SELECT SUM(amount) FROM journal_entries
            WHERE credit_account_id IN ({','.join('?' for _ in account_ids)}) {date_condition}
        """
        cursor.execute(credit_query, params_credit)
        total_credits = cursor.fetchone()[0] or 0.0

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
            cursor.execute("INSERT INTO customers (name, phone, email) VALUES (?, ?, ?)",
                           (name, phone, email))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            print(f"Customer with name '{name}' already exists.")
            return None
        except sqlite3.Error as e:
            print(f"Error adding customer: {e}")
            return None
        finally:
            conn.close()

def get_all_customers():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM customers ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
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
        cursor = conn.cursor()
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
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, price, stock, category_id, sku, description, image_data, barcode, buying_price, low_stock_threshold))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"Product with name '{name}' or barcode '{barcode}' already exists or invalid category_id.")
            return False
        except sqlite3.Error as e:
            print(f"Error adding product: {e}")
            return False
        finally:
            conn.close()

def get_all_products():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.id, p.name, p.price, p.stock, p.sku, p.description, p.image_data, p.barcode, p.buying_price, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                ORDER BY p.name
            ''')
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching products: {e}")
            return []
        finally:
            conn.close()
            
            
def get_paginated_products(category=None, stock_status=None, search_term=None, page=1, limit=50):
    """
    Fetches a paginated list of products with advanced filtering capabilities.
    Returns a dictionary containing the products for the page and total counts.
    """
    conn = connect_db()
    if not conn:
        return {'products': [], 'total_products': 0, 'total_pages': 1}

    offset = (page - 1) * limit
    
    # --- Build Query Components ---
    base_query = """
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
    """
    
    conditions = []
    params = []

    if category and category != "All":
        conditions.append("c.name = ?")
        params.append(category)

    if stock_status and stock_status != "All":
        if stock_status == "In Stock":
            conditions.append("p.stock > 0")
        elif stock_status == "Low Stock":
            conditions.append("p.stock <= p.low_stock_threshold AND p.stock > 0")
        elif stock_status == "Out of Stock":
            conditions.append("p.stock = 0")

    if search_term and search_term.strip():
        term = f"%{search_term.strip()}%"
        conditions.append("(p.name LIKE ? OR p.sku LIKE ? OR p.description LIKE ? OR p.barcode LIKE ?)")
        params.extend([term, term, term, term])

    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    try:
        cursor = conn.cursor()

        # 1. Get the total count of matching products
        count_query = f"SELECT COUNT(p.id) {base_query} {where_clause}"
        cursor.execute(count_query, tuple(params))
        total_products = cursor.fetchone()[0]
        total_pages = (total_products + limit - 1) // limit

        # 2. Get the actual data for the current page
        data_query = f"""
            SELECT p.id, p.name, p.price, p.buying_price, p.stock, p.sku, 
                   p.description, p.image_data, p.barcode, p.low_stock_threshold, c.name as category_name
            {base_query}
            {where_clause}
            ORDER BY p.name
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        cursor.execute(data_query, tuple(params))
        
        products = [dict(row) for row in cursor.fetchall()]

        return {
            'products': products,
            'total_products': total_products,
            'total_pages': max(1, total_pages)
        }
    except sqlite3.Error as e:
        print(f"Error fetching paginated products: {e}")
        return {'products': [], 'total_products': 0, 'total_pages': 1}
    finally:
        if conn:
            conn.close()            

def get_products_with_filters(category=None, price_min=None, price_max=None, stock_status=None, search_term=None):
    """Get products with advanced filtering capabilities"""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            query = '''
                SELECT p.id, p.name, p.price,p.buying_price, p.stock, p.sku, p.description, p.image_data, p.barcode, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE 1=1
            '''
            params = []

            if category and category != "All":
                query += " AND c.name = ?"
                params.append(category)

            if price_min is not None and price_min != "":
                try:
                    price_min = float(price_min)
                    query += " AND p.price >= ?"
                    params.append(price_min)
                except ValueError:
                    pass

            if price_max is not None and price_max != "":
                try:
                    price_max = float(price_max)
                    query += " AND p.price <= ?"
                    params.append(price_max)
                except ValueError:
                    pass

            if stock_status and stock_status != "All":
                if stock_status == "In Stock":
                    query += " AND p.stock > 0"
                elif stock_status == "Low Stock":
                    query += " AND p.stock <= p.low_stock_threshold AND p.stock > 0"
                elif stock_status == "Out of Stock":
                    query += " AND p.stock = 0"

            if search_term and search_term.strip():
                search_term = search_term.strip()
                query += " AND (p.name LIKE ? OR p.sku LIKE ? OR p.description LIKE ? OR p.barcode LIKE ?)"
                params.extend([f"%{search_term}%", f"%{search_term}%", f"%{search_term}%", f"%{search_term}%"])

            query += " ORDER BY p.name"
            cursor.execute(query, tuple(params))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching products with filters: {e}")
            return []
        finally:
            conn.close()

def get_product_categories_summary():
    """Get summary of products by category for filter counts"""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT c.name as category_name, COUNT(p.id) as product_count
                FROM categories c
                LEFT JOIN products p ON c.id = p.category_id
                GROUP BY c.name
                ORDER BY c.name
            ''')
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching categories summary: {e}")
            return []
        finally:
            conn.close()

def get_product_by_id(product_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.id, p.name, p.price, p.stock, p.sku, p.description, p.image_data, p.barcode, p.buying_price, p.low_stock_threshold, c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.id = ?
            ''', (product_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching product by ID: {e}")
            return None
        finally:
            conn.close()

def update_product(product_id, name, price, stock, category_id=None, sku=None, description=None, image_data=None, barcode=None, buying_price=0.0, low_stock_threshold=10):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE products
                SET name = ?, price = ?, stock = ?, category_id = ?, sku = ?, description = ?, image_data = ?, barcode = ?, buying_price = ?, low_stock_threshold = ?
                WHERE id = ?
            ''', (name, price, stock, category_id, sku, description, image_data, barcode, buying_price, low_stock_threshold, product_id))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            print(f"Product with name '{name}' or barcode '{barcode}' already exists or invalid category_id.")
            return False
        except sqlite3.Error as e:
            print(f"Error updating product: {e}")
            return False
        finally:
            conn.close()

def delete_product(product_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sale_items WHERE product_id = ?", (product_id,))
            if cursor.fetchone()[0] > 0:
                print(f"Cannot delete product ID {product_id}: It is linked to existing sales.")
                return False
            cursor.execute("SELECT COUNT(*) FROM purchase_items WHERE product_id = ?", (product_id,))
            if cursor.fetchone()[0] > 0:
                print(f"Cannot delete product ID {product_id}: It is linked to existing purchases.")
                return False

            cursor.execute("DELETE FROM products WHERE id = ?", (product_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"Error deleting product: {e}")
            return False
        finally:
            conn.close()

# --- Category Management Functions ---
def add_category(name):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO categories (name) VALUES (?)", (name,))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"Category '{name}' already exists.")
            return False
        except sqlite3.Error as e:
            print(f"Error adding category: {e}")
            return False
        finally:
            conn.close()

def get_all_categories():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM categories ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching categories: {e}")
            return []
        finally:
            conn.close()

def get_category_by_name(name):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM categories WHERE name = ?", (name,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching category by name: {e}")
            return None
        finally:
            conn.close()

def delete_category(category_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM products WHERE category_id = ?", (category_id,))
            if cursor.fetchone()[0] > 0:
                print(f"Cannot delete category ID {category_id}: It is linked to existing products.")
                return False
            cursor.execute("DELETE FROM categories WHERE id = ?", (category_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"Error deleting category: {e}")
            return False
        finally:
            conn.close()

# --- Sales Management Functions ---

def get_supplier_by_name(name):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM suppliers WHERE name = ?", (name,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching supplier by name: {e}")
            return None
        finally:
            conn.close()

def get_all_sales(start_date=None, end_date=None, product_name=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            query = '''
                SELECT s.id, s.total_amount, s.sale_date, s.discount_amount, s.tax_amount
                FROM sales s
            '''
            params = []
            conditions = []

            if product_name:
                query += '''
                    JOIN sale_items si ON s.id = si.sale_id
                    JOIN products p ON si.product_id = p.id
                '''
                conditions.append("p.name LIKE ?")
                params.append(f"%{product_name}%")

            if start_date:
                conditions.append("s.sale_date >= ?")
                params.append(start_date)
            if end_date:
                conditions.append("s.sale_date <= ?")
                params.append(f"{end_date} 23:59:59") # Include whole end day

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY s.sale_date DESC"

            cursor.execute(query, tuple(params))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching sales history: {e}")
            return []
        finally:
            conn.close()

def get_sale_details(sale_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT si.quantity, si.price_at_sale, p.name as product_name, p.sku, p.description, p.image_data
                FROM sale_items si
                JOIN products p ON si.product_id = p.id
                WHERE si.sale_id = ?
            ''', (sale_id,))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching sale details: {e}")
            return []
        finally:
            conn.close()

# --- Inventory Management Functions ---
def get_low_stock_count():
    """Fetches the count of products where stock is below or equal to its specific threshold."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # This query now correctly compares stock against each product's own low_stock_threshold
            cursor.execute("SELECT COUNT(*) FROM products WHERE stock > 0 AND stock <= low_stock_threshold")
            return cursor.fetchone()[0]
        except sqlite3.Error as e:
            print(f"Error fetching low stock count: {e}")
            return 0
        finally:
            conn.close()

def get_total_products_count():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM products")
            return cursor.fetchone()[0]
        except sqlite3.Error as e:
            print(f"Error fetching total products count: {e}")
            return 0
        finally:
            conn.close()

def get_total_categories_count():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM categories")
            return cursor.fetchone()[0]
        except sqlite3.Error as e:
            print(f"Error fetching total categories count: {e}")
            return 0
        finally:
            conn.close()

def get_total_sales_amount():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT SUM(total_amount) FROM sales")
            total = cursor.fetchone()[0]
            return total if total is not None else 0.0
        except sqlite3.Error as e:
            print(f"Error fetching total sales amount: {e}")
            return 0.0
        finally:
            conn.close()

def get_weekly_sales_summary():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            today = datetime.now()
            weekly_sales = []
            for i in range(7):
                day = today - timedelta(days=i)
                start_of_day = day.replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_day = day.replace(hour=23, minute=59, second=59, microsecond=999999)

                cursor.execute("""
                    SELECT SUM(total_amount) FROM sales
                    WHERE sale_date BETWEEN ? AND ?
                """, (start_of_day.strftime('%Y-%m-%d %H:%M:%S'), end_of_day.strftime('%Y-%m-%d %H:%M:%S')))

                total_sales = cursor.fetchone()[0]
                weekly_sales.append({
                    'date': day.strftime('%a'), # Use abbreviated day name
                    'total_sales': total_sales if total_sales is not None else 0.0
                })
            return list(reversed(weekly_sales)) # Return in chronological order
        except sqlite3.Error as e:
            print(f"Error fetching weekly sales summary: {e}")
            return []
        finally:
            conn.close()

def get_top_selling_products(limit=5):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.name as product_name, SUM(si.quantity) as total_quantity_sold
                FROM sale_items si
                JOIN products p ON si.product_id = p.id
                GROUP BY p.name
                ORDER BY total_quantity_sold DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching top selling products: {e}")
            return []
        finally:
            conn.close()

# --- Supplier Management Functions ---
def add_supplier(name, contact_person=None, phone=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO suppliers (name, contact_person, phone) VALUES (?, ?, ?)",
                           (name, contact_person, phone))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"Supplier with name '{name}' already exists.")
            return False
        except sqlite3.Error as e:
            print(f"Error adding supplier: {e}")
            return False
        finally:
            conn.close()

def get_all_suppliers():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM suppliers ORDER BY name")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching suppliers: {e}")
            return []
        finally:
            conn.close()

def get_supplier_by_id(supplier_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM suppliers WHERE id = ?", (supplier_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching supplier by ID: {e}")
            return None
        finally:
            conn.close()

def update_supplier(supplier_id, name, contact_person=None, phone=None):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE suppliers
                SET name = ?, contact_person = ?, phone = ?
                WHERE id = ?
            ''', (name, contact_person, phone, supplier_id))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.IntegrityError:
            print(f"Supplier with name '{name}' already exists.")
            return False
        except sqlite3.Error as e:
            print(f"Error updating supplier: {e}")
            return False
        finally:
            conn.close()

def delete_supplier(supplier_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            # Check for dependencies in purchases
            cursor.execute("SELECT COUNT(*) FROM purchases WHERE supplier_id = ?", (supplier_id,))
            if cursor.fetchone()[0] > 0:
                print(f"Cannot delete supplier ID {supplier_id}: It is linked to existing purchases.")
                return False

            cursor.execute("DELETE FROM suppliers WHERE id = ?", (supplier_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"Error deleting supplier: {e}")
            return False
        finally:
            conn.close()

# --- Purchase Management Functions ---
def get_all_purchases():
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT p.id, p.total_cost, p.purchase_date, s.name as supplier_name
                FROM purchases p
                JOIN suppliers s ON p.supplier_id = s.id
                ORDER BY p.purchase_date DESC
            ''')
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching purchase history: {e}")
            return []
        finally:
            conn.close()

def get_purchase_details(purchase_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT pi.quantity, pi.cost_at_purchase, p.name as product_name, p.sku, p.description, p.image_data
                FROM purchase_items pi
                JOIN products p ON pi.product_id = p.id
                WHERE pi.purchase_id = ?
            ''', (purchase_id,))
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching purchase details: {e}")
            return []
        finally:
            conn.close()

def get_purchases_by_date_range(start_date, end_date):
    """Fetches purchase history within a specified date range."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        query = """
            SELECT p.id, p.total_cost, p.purchase_date, s.name as supplier_name
            FROM purchases p
            JOIN suppliers s ON p.supplier_id = s.id
            WHERE p.purchase_date BETWEEN ? AND ?
            ORDER BY p.purchase_date DESC
        """
        cursor.execute(query, (start_date, f"{end_date} 23:59:59"))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching purchases by date range: {e}")
        return []
    finally:
        if conn: conn.close()            

# --- User Management Functions ---
def add_user(username, password_hash, role='cashier'):
    """Add a new user to the database."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                           (username, password_hash, role))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"User with username '{username}' already exists.")
            return False
        except sqlite3.Error as e:
            print(f"Error adding user: {e}")
            return False
        finally:
            conn.close()

def get_user_by_username(username):
    """Get a user by username."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching user by username: {e}")
            return None
        finally:
            conn.close()

def get_all_users():
    """Get all users from the database."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, username, role FROM users ORDER BY username")
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error fetching users: {e}")
            return []
        finally:
            conn.close()

def delete_user(user_id):
    """Delete a user from the database."""
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error as e:
            print(f"Error deleting user: {e}")
            return False
        finally:
            conn.close()

def verify_user_password(username, password):
    """Verify a user's password."""
    user = get_user_by_username(username)
    if user and 'password_hash' in user:
        return check_password_hash(user['password_hash'], password)
    return False

# --- Reporting Functions ---
def get_sales_summary(start_date=None, end_date=None):
    """Gets a summary of sales metrics for a given date range."""
    conn = connect_db()
    if not conn: return None
    try:
        cursor = conn.cursor()
        
        # Conditions and params
        where_clause = ""
        params = []
        if start_date and end_date:
            where_clause = " WHERE sale_date BETWEEN ? AND ?"
            params.extend([start_date, f"{end_date} 23:59:59"])

        query_sales = f"SELECT SUM(total_amount), COUNT(id), SUM(discount_amount) FROM sales {where_clause}"
        query_tax = f"SELECT SUM(tax_amount) FROM sales {where_clause}"

        cursor.execute(query_sales, tuple(params))
        total_sales, transaction_count, total_discount = cursor.fetchone()

        # Tax calculation is now direct sum from the corrected sales table
        subtotal_before_tax = (total_sales or 0) + (total_discount or 0)
        total_tax = (subtotal_before_tax * get_tax_rate_from_sales(cursor, where_clause, params)) if subtotal_before_tax > 0 else 0


        return {
            "total_sales": total_sales or 0.0,
            "transaction_count": transaction_count or 0,
            "total_discount": total_discount or 0.0,
            "total_tax": total_tax
        }

    except sqlite3.Error as e:
        print(f"Error getting sales summary: {e}")
        return None
    finally:
        conn.close()
        
def get_advanced_sales_summary(start_date, end_date):
    """Gets a comprehensive summary of sales metrics for a given date range."""
    conn = connect_db()
    if not conn: return {}
    try:
        cursor = conn.cursor()
        
        # Prepare date clause and params
        where_clause = " WHERE s.sale_date BETWEEN ? AND ?"
        params = [start_date, f"{end_date} 23:59:59"]

        # 1. Gross Sales (Total value of items sold before discount)
        query_gross = f"""
            SELECT SUM(si.quantity * si.price_at_sale)
            FROM sale_items si
            JOIN sales s ON si.sale_id = s.id
            {where_clause}
        """
        cursor.execute(query_gross, tuple(params))
        gross_sales = cursor.fetchone()[0] or 0.0

        # 2. Discounts and Transactions from sales table
        query_sales_table = f"SELECT SUM(discount_amount), COUNT(id) FROM sales s {where_clause}"
        cursor.execute(query_sales_table, tuple(params))
        total_discount, transaction_count = cursor.fetchone()
        total_discount = total_discount or 0.0
        transaction_count = transaction_count or 0

        # 3. Net Sales
        net_sales = gross_sales - total_discount

        # 4. Cost of Goods Sold (COGS)
        query_cogs = f"""
            SELECT SUM(si.quantity * p.buying_price)
            FROM sale_items si
            JOIN products p ON si.product_id = p.id
            JOIN sales s ON si.sale_id = s.id
            {where_clause}
        """
        cursor.execute(query_cogs, tuple(params))
        total_cogs = cursor.fetchone()[0] or 0.0

        # 5. Gross Profit
        gross_profit = net_sales - total_cogs
        
        # 6. Tax (back-calculated from net sales as per tax-inclusive model)
        query_tax_rate = f"SELECT AVG(tax_amount) FROM sales s {where_clause}"
        cursor.execute(query_tax_rate, tuple(params))
        avg_tax_rate = cursor.fetchone()[0] or 0.0
        
        total_tax = 0
        if avg_tax_rate > 0:
            pre_tax_amount = net_sales / (1 + avg_tax_rate)
            total_tax = net_sales - pre_tax_amount

        return {
            "gross_sales": gross_sales,
            "total_discount": total_discount,
            "net_sales": net_sales,
            "transaction_count": transaction_count,
            "total_cogs": total_cogs,
            "gross_profit": gross_profit,
            "total_tax": total_tax
        }

    except sqlite3.Error as e:
        print(f"Error getting advanced sales summary: {e}")
        return {}
    finally:
        conn.close()        

def get_tax_rate_from_sales(cursor, where_clause, params):
    """Helper to get an average tax rate for summary purposes."""
    # This is a simplification; a real system might have varied tax rates.
    query = f"SELECT AVG(tax_amount) FROM sales {where_clause}"
    cursor.execute(query, tuple(params))
    avg_tax_rate = cursor.fetchone()[0]
    return avg_tax_rate or 0.0

def get_product_performance_report(start_date=None, end_date=None, sort_by='quantity', limit=100):
    """Gets a report on product performance, sorted by quantity sold or revenue."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()

        query = """
            SELECT
                p.id,
                p.name,
                p.sku,
                SUM(si.quantity) as total_quantity_sold,
                SUM(si.quantity * si.price_at_sale) as total_revenue
            FROM sale_items si
            JOIN products p ON si.product_id = p.id
        """
        params = []
        conditions = []

        if start_date and end_date:
            query += " JOIN sales s ON si.sale_id = s.id"
            conditions.append("s.sale_date BETWEEN ? AND ?")
            params.extend([start_date, f"{end_date} 23:59:59"])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " GROUP BY p.id, p.name, p.sku"

        if sort_by == 'revenue':
            query += " ORDER BY total_revenue DESC"
        else: # Default to quantity
            query += " ORDER BY total_quantity_sold DESC"

        query += " LIMIT ?"
        params.append(limit)

        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]

    except sqlite3.Error as e:
        print(f"Error getting product performance report: {e}")
        return []
    finally:
        conn.close()

def get_inventory_summary_report():
    """Gets a summary report of the current inventory."""
    conn = connect_db()
    if not conn: return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                p.id,
                p.name,
                p.sku,
                p.stock,
                p.buying_price,
                p.price as selling_price,
                (p.stock * p.buying_price) as total_cost_value,
                (p.stock * p.price) as total_retail_value,
                c.name as category_name
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            ORDER BY p.name
        """)
        products = [dict(row) for row in cursor.fetchall()]

        total_skus = len(products)
        total_units = sum(p['stock'] for p in products)
        total_cost_value = sum(p['total_cost_value'] for p in products)
        total_retail_value = sum(p['total_retail_value'] for p in products)

        return {
            "products_details": products,
            "total_skus": total_skus,
            "total_units": total_units,
            "total_cost_value": total_cost_value,
            "total_retail_value": total_retail_value
        }

    except sqlite3.Error as e:
        print(f"Error getting inventory summary report: {e}")
        return None
    finally:
        conn.close()

def get_sales_by_category_report(start_date=None, end_date=None):
    """Gets a report of total sales revenue broken down by product category."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        query = """
            SELECT
                COALESCE(c.name, 'Uncategorized') as category_name,
                SUM(si.quantity * si.price_at_sale) as total_revenue
            FROM sale_items si
            JOIN products p ON si.product_id = p.id
            LEFT JOIN categories c ON p.category_id = c.id
        """
        params = []
        conditions = []
        if start_date and end_date:
            query += " JOIN sales s ON si.sale_id = s.id"
            conditions.append("s.sale_date BETWEEN ? AND ?")
            params.extend([start_date, f"{end_date} 23:59:59"])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " GROUP BY category_name ORDER BY total_revenue DESC"

        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]

    except sqlite3.Error as e:
        print(f"Error getting sales by category report: {e}")
        return []
    finally:
        conn.close()

def get_sale_by_id(sale_id):
    conn = connect_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sales WHERE id = ?", (sale_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            print(f"Error fetching sale by ID: {e}")
            return None
        finally:
            conn.close()

def get_sales_by_date_range(start_date, end_date):
    """Fetches sales data within a specified date range."""
    conn = connect_db()
    if not conn:
        return []
    try:
        cursor = conn.cursor()
        query = """
            SELECT s.id, s.total_amount, s.sale_date, s.discount_amount, s.tax_amount
            FROM sales s
            WHERE s.sale_date BETWEEN ? AND ?
            ORDER BY s.sale_date DESC
        """
        cursor.execute(query, (start_date, f"{end_date} 23:59:59"))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching sales by date range: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_yearly_sales_summary():
    """Gets a summary of sales for each month of the current year."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        current_year = datetime.now().year
        
        query = """
            SELECT
                strftime('%m', sale_date) as month,
                SUM(total_amount) as total_sales
            FROM sales
            WHERE strftime('%Y', sale_date) = ?
            GROUP BY month
            ORDER BY month ASC
        """
        cursor.execute(query, (str(current_year),))
        
        sales_data = {row['month']: row['total_sales'] for row in cursor.fetchall()}
        
        # Ensure all 12 months are present
        monthly_summary = []
        for i in range(1, 13):
            month_str = f"{i:02d}" # Format month as '01', '02', etc.
            month_name = datetime(current_year, i, 1).strftime('%b') # 'Jan', 'Feb', etc.
            monthly_summary.append({
                'month': month_name,
                'total_sales': sales_data.get(month_str, 0.0)
            })
            
        return monthly_summary

    except sqlite3.Error as e:
        print(f"Error getting yearly sales summary: {e}")
        return []
    finally:
        conn.close()

def get_all_expenses():
    """Fetches all expense records for display."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                e.id,
                e.description,
                e.amount,
                e.expense_date,
                coa_exp.name as expense_account,
                coa_pay.name as payment_account
            FROM expenses e
            JOIN chart_of_accounts coa_exp ON e.expense_account_id = coa_exp.id
            JOIN chart_of_accounts coa_pay ON e.payment_account_id = coa_pay.id
            ORDER BY e.expense_date DESC
        """)
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching all expenses: {e}")
        return []
    finally:
        conn.close()

def get_all_expenses_by_date(start_date=None, end_date=None):
    """Fetches all expense records for display within a specific date range."""
    conn = connect_db()
    if not conn: return []
    try:
        cursor = conn.cursor()
        query = """
            SELECT
                e.id,
                e.description,
                e.amount,
                e.expense_date,
                coa_exp.name as expense_account,
                coa_pay.name as payment_account
            FROM expenses e
            JOIN chart_of_accounts coa_exp ON e.expense_account_id = coa_exp.id
            JOIN chart_of_accounts coa_pay ON e.payment_account_id = coa_pay.id
        """
        params = []
        conditions = []
        if start_date:
            conditions.append("e.expense_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("e.expense_date <= ?")
            params.append(f"{end_date} 23:59:59")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY e.expense_date DESC"

        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Error fetching all expenses by date: {e}")
        return []
    finally:
        conn.close()


def get_paginated_journal_entries(start_date=None, end_date=None, account_id=None, page=1, limit=50):
    """
    Fetches journal entries with filtering and pagination.
    Returns a dictionary with the entries for the current page and total counts.
    """
    conn = connect_db()
    if not conn: return {'entries': [], 'total_entries': 0, 'total_pages': 1}

    offset = (page - 1) * limit
    
    # Base query for unwinding journal entries
    base_query = """
        FROM (
            SELECT date, description, debit_account_id as account_id, amount as debit, NULL as credit, reference_id, reference_type
            FROM journal_entries
            UNION ALL
            SELECT date, description, credit_account_id as account_id, NULL as debit, amount as credit, reference_id, reference_type
            FROM journal_entries
        ) AS unwound
        JOIN chart_of_accounts coa ON unwound.account_id = coa.id
    """

    # Build WHERE clause and parameters
    conditions = []
    params = []
    if start_date:
        conditions.append("unwound.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("unwound.date <= ?")
        params.append(f"{end_date} 23:59:59")
    if account_id:
        conditions.append("unwound.account_id = ?")
        params.append(account_id)
        
    where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

    try:
        cursor = conn.cursor()

        # 1. Get the total count of matching entries for pagination info
        count_query = f"SELECT COUNT(*) {base_query} {where_clause}"
        cursor.execute(count_query, tuple(params))
        total_entries = cursor.fetchone()[0]
        total_pages = (total_entries + limit - 1) // limit  # Ceiling division

        # 2. Get the actual data for the current page
        data_query = f"""
            SELECT unwound.date, unwound.description, coa.name as account_name, unwound.debit, unwound.credit
            {base_query}
            {where_clause}
            ORDER BY unwound.date DESC, unwound.reference_id DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        cursor.execute(data_query, tuple(params))
        
        entries = [dict(row) for row in cursor.fetchall()]

        return {
            'entries': entries,
            'total_entries': total_entries,
            'total_pages': max(1, total_pages) # Ensure at least 1 page
        }
    except Exception as e:
        print(f"Error fetching paginated journal entries: {e}")
        return {'entries': [], 'total_entries': 0, 'total_pages': 1}
    finally:
        if conn: conn.close()

def populate_dummy_data():
    """Populates the database with a more comprehensive and realistic set of dummy data for testing."""
    conn = connect_db()
    if not conn:
        print("Could not connect to database. Aborting dummy data population.")
        return

    try:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM products")
        if cursor.fetchone()[0] > 0:
            print("Database already contains data. Skipping dummy data population.")
            return

        print("Adding dummy categories...")
        categories = ["Electronics", "Beverages", "Snacks", "Office Supplies", "Apparel", "Home Goods"]
        for category in categories:
            add_category(category)
        category_data = get_all_categories()
        category_ids = {c['name']: c['id'] for c in category_data}

        print("Adding dummy suppliers...")
        suppliers = [
            ("Mlimani City Electronics", "Jane Doe", "555-0101"),
            ("Bakhresa Food Distribution", "John Smith", "555-0102"),
            ("Kariakoo Office World", "Peter Jones", "555-0103"),
        ]
        for name, contact, phone in suppliers:
            add_supplier(name, contact, phone)
        supplier_data = get_all_suppliers()
        supplier_ids = {s['name']: s['id'] for s in supplier_data}
        
        print("Adding dummy customers...")
        customers = [
            ("Alpha Company", "255755123456", "alpha@test.com"),
            ("Bravo Logistics", "255655789012", "bravo@test.com"),
            ("Charlie Services", "255788345678", "charlie@test.com"),
        ]
        for name, phone, email in customers:
            add_customer(name, phone, email)
        customer_data = get_all_customers()
        customer_ids = {c['name']: c['id'] for c in customer_data}

        print("Adding dummy users...")
        add_user('admin', generate_password_hash('admin123'), 'admin')
        add_user('cashier', generate_password_hash('cashier123'), 'cashier')

        print("Adding a wide variety of dummy products...")
        product_list = [
            # Name, Selling Price, Buying Price, Category, Initial Stock, Low Stock Threshold
            ("Dell XPS 15 Laptop", 3500000, 2800000, "Electronics", 15, 5),
            ("Logitech MX Master 3 Mouse", 250000, 180000, "Electronics", 30, 10),
            ("4K 27-inch Monitor", 800000, 650000, "Electronics", 10, 3),
            ("Coca-Cola 500ml", 1000, 600, "Beverages", 200, 50),
            ("Azam Embe Juice 1L", 2500, 1800, "Beverages", 150, 40),
            ("Kilimanjaro Water 1.5L", 1000, 500, "Beverages", 300, 50),
            ("Pringles Original", 7000, 5000, "Snacks", 80, 20),
            ("Peanuts (Karanga)", 1500, 800, "Snacks", 9, 10), # Will be low stock
            ("A4 Paper Ream", 12000, 9000, "Office Supplies", 50, 15),
            ("Stapler", 8000, 5500, "Office Supplies", 0, 5), # Will be out of stock
            ("Polo T-Shirt", 25000, 15000, "Apparel", 60, 10),
            ("Men's Jeans", 45000, 30000, "Apparel", 40, 10),
            ("Cooking Pot Set", 120000, 95000, "Home Goods", 25, 5),
            ("LED Smart Bulb", 15000, 10000, "Home Goods", 70, 20),
        ]
        for name, price, buying_price, cat_name, stock, low_stock in product_list:
            add_product(name, price, stock, category_ids[cat_name], f"{cat_name[:3].upper()}{random.randint(100,999)}",
                        f"A high quality {name}.", None, f"789{random.randint(10000,99999)}", buying_price, low_stock)
        product_data = get_all_products()
        
        print("Generating realistic dummy sales across the past year...")
        for i in range(75): # Create 75 sale records
            sale_date = datetime.now() - timedelta(days=random.randint(1, 365), hours=random.randint(1,23), minutes=random.randint(1,59))
            
            # Create a cart with 1 to 4 random products
            cart = []
            num_items = random.randint(1, 4)
            for _ in range(num_items):
                p = random.choice(product_data)
                if p['stock'] > 0:
                     cart.append({'product_id': p['id'], 'quantity': 1, 'price_at_sale': p['price']})
            
            if not cart: continue # Skip if all chosen products were out of stock

            # Decide sale type
            sale_type = random.choice(['paid', 'paid', 'paid', 'partial', 'due'])
            
            discount = 0.0
            if random.random() > 0.8: # 20% chance of a discount
                discount = sum(c['price_at_sale'] for c in cart) * 0.1 # 10% discount

            # Temporarily modify the sale date in the database for this transaction
            # This is a bit of a hack to backdate sales.
            conn = connect_db()
            cursor = conn.cursor()
            
            if sale_type == 'paid':
                total = sum(c['price_at_sale'] for c in cart) - discount
                payments = [{'method': 'Cash', 'amount': total}]
                is_ok, sale_id = record_sale(cart, payments, discount, 0.18, None, None)
            
            elif sale_type == 'partial':
                customer = random.choice(customer_data)
                total = sum(c['price_at_sale'] for c in cart) - discount
                paid_amount = total * random.uniform(0.3, 0.7) # Pay 30-70%
                payments = [{'method': 'M-Pesa', 'amount': paid_amount}]
                is_ok, sale_id = record_sale(cart, payments, discount, 0.18, customer['id'], None)

            else: # Due
                customer = random.choice(customer_data)
                is_ok, sale_id = record_sale(cart, [], discount, 0.18, customer['id'], None)

            if is_ok:
                # Manually update the date of the created sale and its journal entries
                cursor.execute("UPDATE sales SET sale_date = ? WHERE id = ?", (sale_date, sale_id))
                cursor.execute("UPDATE journal_entries SET date = ? WHERE reference_type = 'sale' AND reference_id = ?", (sale_date, sale_id))
                conn.commit()
            
            conn.close()

        print("Adding dummy purchases...")
        purchase_cart = [{'product_id': 10, 'quantity': 30, 'cost': 5500, 'new_price': 8000}] 
        record_purchase(supplier_ids['Kariakoo Office World'], purchase_cart)
        print("Adding dummy expenses...")
        add_expense("Monthly Rent - August", 500000, "Rent Expense", "Bank")
        add_expense("LUKU Power Bill", 150000, "Utilities Expense", "Cash")
        add_expense("Staff Salaries - August", 1200000, "Salaries Expense", "Bank")

        print("\nDummy data generation complete!")

    except Exception as e:
        import traceback
        print(f"Database error during dummy data generation: {e}")
        traceback.print_exc()
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    db_file = get_db_path()
    if os.path.exists(db_file):
        os.remove(db_file)
        print("Removed old database file to ensure a clean start.")

    init_db()
    populate_dummy_data()

    print(f"\n--- Current System State ---")
    print(f"Total Sales Amount: {get_total_sales_amount():,.2f}")
    print(f"Total Products: {get_total_products_count()}")
    print(f"Total Categories: {get_total_categories_count()}")
    print(f"Low Stock Items: {get_low_stock_count()}")

    print("\n--- Balance Sheet (as of today) ---")
    bs_data = get_balance_sheet()
    if bs_data:
        print(f"  Total Assets: {bs_data['assets']['total']:,.2f}")
        print(f"  Total Liabilities: {bs_data['liabilities']['total']:,.2f}")
        print(f"  Total Equity: {bs_data['equity']['total']:,.2f}")
        check_val = bs_data['total_liabilities_and_equity']
        print(f"  Total Liabilities + Equity: {check_val:,.2f}")
        is_balanced = abs(bs_data['assets']['total'] - check_val) < 0.01
        print(f"  Is Balanced: {is_balanced}")

