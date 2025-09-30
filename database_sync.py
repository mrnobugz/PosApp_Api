"""
Database synchronization utilities for tracking sync state
"""
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import database as db

def init_sync_tables():
    """Initialize synchronization tracking tables"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Sync configuration table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Sync tracking table for entities
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                external_id TEXT,
                sync_status TEXT DEFAULT 'pending',
                last_sync TIMESTAMP,
                sync_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(entity_type, entity_id)
            )
        ''')
        
        # Sync history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id INTEGER,
                external_id TEXT,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Conflict resolution table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                external_id TEXT,
                local_data TEXT,
                remote_data TEXT,
                resolution TEXT,
                resolved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(entity_type, entity_id)
            )
        ''')
        
        conn.commit()
        print("Sync tables initialized successfully")
        
    except Exception as e:
        print(f"Error initializing sync tables: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_sync_config(key: str) -> Optional[str]:
    """Get sync configuration value"""
    conn = db.connect_db()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM sync_config WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"Error getting sync config: {e}")
        return None
    finally:
        conn.close()

def set_sync_config(key: str, value: str):
    """Set sync configuration value"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO sync_config (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        ''', (key, value))
        conn.commit()
    except Exception as e:
        print(f"Error setting sync config: {e}")
        conn.rollback()
    finally:
        conn.close()

def track_entity(entity_type: str, entity_id: int, external_id: str = None, sync_status: str = 'pending'):
    """Track an entity for synchronization"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Generate sync hash for change detection
        sync_hash = generate_sync_hash(entity_type, entity_id)
        
        cursor.execute('''
            INSERT OR REPLACE INTO sync_tracking 
            (entity_type, entity_id, external_id, sync_status, last_sync, sync_hash, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
        ''', (entity_type, entity_id, external_id, sync_status, sync_hash))
        
        conn.commit()
    except Exception as e:
        print(f"Error tracking entity: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_pending_sync_items(entity_type: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Get items pending synchronization"""
    conn = db.connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        # Get products pending sync
        if entity_type == "product":
            cursor.execute('''
                SELECT p.*, st.external_id, st.sync_status
                FROM products p
                LEFT JOIN sync_tracking st ON st.entity_type = 'product' AND st.entity_id = p.id
                WHERE st.sync_status IN ('pending', 'updated', 'deleted') OR st.sync_status IS NULL
                ORDER BY p.id
                LIMIT ?
            ''', (limit,))
        
        # Get categories pending sync
        elif entity_type == "category":
            cursor.execute('''
                SELECT c.*, st.external_id, st.sync_status
                FROM categories c
                LEFT JOIN sync_tracking st ON st.entity_type = 'category' AND st.entity_id = c.id
                WHERE st.sync_status IN ('pending', 'updated', 'deleted') OR st.sync_status IS NULL
                ORDER BY c.id
                LIMIT ?
            ''', (limit,))
        
        # Get suppliers pending sync
        elif entity_type == "supplier":
            cursor.execute('''
                SELECT s.*, st.external_id, st.sync_status
                FROM suppliers s
                LEFT JOIN sync_tracking st ON st.entity_type = 'supplier' AND st.entity_id = s.id
                WHERE st.sync_status IN ('pending', 'updated', 'deleted') OR st.sync_status IS NULL
                ORDER BY s.id
                LIMIT ?
            ''', (limit,))
        
        else:
            return []
        
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
        
    except Exception as e:
        print(f"Error getting pending sync items: {e}")
        return []
    finally:
        conn.close()

def update_sync_status(entity_type: str, entity_id: int, external_id: str = None, status: str = 'synced'):
    """Update synchronization status for an entity"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Generate new sync hash
        sync_hash = generate_sync_hash(entity_type, entity_id)
        
        if external_id:
            cursor.execute('''
                UPDATE sync_tracking 
                SET external_id = ?, sync_status = ?, last_sync = CURRENT_TIMESTAMP, 
                    sync_hash = ?, updated_at = CURRENT_TIMESTAMP
                WHERE entity_type = ? AND entity_id = ?
            ''', (external_id, status, sync_hash, entity_type, entity_id))
        else:
            cursor.execute('''
                UPDATE sync_tracking 
                SET sync_status = ?, last_sync = CURRENT_TIMESTAMP, 
                    sync_hash = ?, updated_at = CURRENT_TIMESTAMP
                WHERE entity_type = ? AND entity_id = ?
            ''', (status, sync_hash, entity_type, entity_id))
        
        conn.commit()
    except Exception as e:
        print(f"Error updating sync status: {e}")
        conn.rollback()
    finally:
        conn.close()

def generate_sync_hash(entity_type: str, entity_id: int) -> str:
    """Generate a hash for change detection"""
    conn = db.connect_db()
    if not conn:
        return ""
    
    try:
        cursor = conn.cursor()
        
        if entity_type == "product":
            cursor.execute('''
                SELECT name, price, stock, sku, description, barcode, buying_price, low_stock_threshold, category_id
                FROM products WHERE id = ?
            ''', (entity_id,))
        
        elif entity_type == "category":
            cursor.execute('SELECT name FROM categories WHERE id = ?', (entity_id,))
        
        elif entity_type == "supplier":
            cursor.execute('SELECT name, contact_person, phone FROM suppliers WHERE id = ?', (entity_id,))
        
        else:
            return ""
        
        row = cursor.fetchone()
        if row:
            import hashlib
            data_str = "|".join(str(val) if val is not None else "" for val in row)
            return hashlib.md5(data_str.encode()).hexdigest()
        
        return ""
        
    except Exception as e:
        print(f"Error generating sync hash: {e}")
        return ""
    finally:
        conn.close()

def record_sync_history(sync_type: str, entity_type: str, entity_id: int = None, 
                       external_id: str = None, action: str = None, status: str = "success", 
                       details: Dict[str, Any] = None):
    """Record synchronization history"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO sync_history 
            (sync_type, entity_type, entity_id, external_id, action, status, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            sync_type, entity_type, entity_id, external_id, action, status,
            json.dumps(details) if details else None
        ))
        conn.commit()
    except Exception as e:
        print(f"Error recording sync history: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_sync_history(limit: int = 50) -> List[Dict[str, Any]]:
    """Get synchronization history"""
    conn = db.connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM sync_history 
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        columns = [description[0] for description in cursor.description]
        history = []
        
        for row in cursor.fetchall():
            item = dict(zip(columns, row))
            if item['details']:
                try:
                    item['details'] = json.loads(item['details'])
                except:
                    item['details'] = {}
            history.append(item)
        
        return history
        
    except Exception as e:
        print(f"Error getting sync history: {e}")
        return []
    finally:
        conn.close()

def get_sync_statistics() -> Dict[str, Any]:
    """Get synchronization statistics"""
    conn = db.connect_db()
    if not conn:
        return {}
    
    try:
        cursor = conn.cursor()
        
        # Count by entity type and status
        cursor.execute('''
            SELECT entity_type, sync_status, COUNT(*) as count
            FROM sync_tracking
            GROUP BY entity_type, sync_status
        ''')
        
        stats = {
            "total_entities": 0,
            "pending_sync": 0,
            "synced": 0,
            "failed": 0,
            "by_type": {}
        }
        
        for row in cursor.fetchall():
            entity_type = row[0]
            status = row[1]
            count = row[2]
            
            if entity_type not in stats["by_type"]:
                stats["by_type"][entity_type] = {}
            
            stats["by_type"][entity_type][status] = count
            stats["total_entities"] += count
            
            if status == "pending":
                stats["pending_sync"] += count
            elif status == "synced":
                stats["synced"] += count
            elif status == "failed":
                stats["failed"] += count
        
        # Last sync times
        cursor.execute('''
            SELECT entity_type, MAX(last_sync) as last_sync
            FROM sync_tracking
            WHERE last_sync IS NOT NULL
            GROUP BY entity_type
        ''')
        
        last_sync_times = {}
        for row in cursor.fetchall():
            last_sync_times[row[0]] = row[1]
        
        stats["last_sync_times"] = last_sync_times
        
        return stats
        
    except Exception as e:
        print(f"Error getting sync statistics: {e}")
        return {}
    finally:
        conn.close()

def record_conflict(entity_type: str, entity_id: int, external_id: str = None,
                   local_data: Dict[str, Any] = None, remote_data: Dict[str, Any] = None,
                   resolution: str = None):
    """Record a synchronization conflict"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO sync_conflicts 
            (entity_type, entity_id, external_id, local_data, remote_data, resolution, resolved_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', (
            entity_type, entity_id, external_id,
            json.dumps(local_data) if local_data else None,
            json.dumps(remote_data) if remote_data else None,
            resolution
        ))
        conn.commit()
    except Exception as e:
        print(f"Error recording conflict: {e}")
        conn.rollback()
    finally:
        conn.close()

def get_conflicts(limit: int = 50) -> List[Dict[str, Any]]:
    """Get unresolved conflicts"""
    conn = db.connect_db()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM sync_conflicts 
            WHERE resolution IS NULL
            ORDER BY created_at DESC 
            LIMIT ?
        ''', (limit,))
        
        columns = [description[0] for description in cursor.description]
        conflicts = []
        
        for row in cursor.fetchall():
            conflict = dict(zip(columns, row))
            if conflict['local_data']:
                try:
                    conflict['local_data'] = json.loads(conflict['local_data'])
                except:
                    conflict['local_data'] = {}
            
            if conflict['remote_data']:
                try:
                    conflict['remote_data'] = json.loads(conflict['remote_data'])
                except:
                    conflict['remote_data'] = {}
            
            conflicts.append(conflict)
        
        return conflicts
        
    except Exception as e:
        print(f"Error getting conflicts: {e}")
        return []
    finally:
        conn.close()

def resolve_conflict(conflict_id: int, resolution: str, chosen_data: Dict[str, Any] = None):
    """Resolve a synchronization conflict"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE sync_conflicts 
            SET resolution = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (resolution, conflict_id))
        conn.commit()
        
        # Update the entity based on resolution
        if chosen_data and resolution in ["local_wins", "remote_wins"]:
            # This would need to be implemented based on entity type
            pass
            
    except Exception as e:
        print(f"Error resolving conflict: {e}")
        conn.rollback()
    finally:
        conn.close()

def cleanup_sync_data(days_to_keep: int = 30):
    """Clean up old sync data"""
    conn = db.connect_db()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        # Clean up old sync history
        cursor.execute('''
            DELETE FROM sync_history 
            WHERE created_at < datetime('now', '-{} days')
        '''.format(days_to_keep))
        
        # Clean up resolved conflicts
        cursor.execute('''
            DELETE FROM sync_conflicts 
            WHERE resolution IS NOT NULL 
            AND resolved_at < datetime('now', '-{} days')
        '''.format(days_to_keep))
        
        conn.commit()
        print(f"Cleaned up sync data older than {days_to_keep} days")
        
    except Exception as e:
        print(f"Error cleaning up sync data: {e}")
        conn.rollback()
    finally:
        conn.close()
