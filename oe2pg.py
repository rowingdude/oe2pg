# Progress to PostgreSQL Database Mirror, this was designed to enable users to retrieve the total data and structure in a Progress OpenEdge database. 
#  The tools works like this:
#   1. Connect to both databases
#   2. Query the SYS tables for the schema
#   3. In alphabetical order, query each table on the Progress side
#   4. Create a corresponding PostgreSQL table
#   5. Replicate the Progress table to the PostgreSQL table
#   6. For the user's information, this script takes a row count and provides a status indicator as it runs
#
#            === Mirroring table: SCHEMA.tables to tables ===
#            Fetching columns for table 'SCHEMA.tables'... Found 5 columns
#            Creating table 'tables' in PostgreSQL... Done
#            Counting rows in 'SCHEMA.tables'... Found 238,204 rows
#            Counting rows in PostgreSQL table 'tables'... Found 0 rows
#            Truncating table 'tables' in PostgreSQL... Done
#            Counting rows in 'SCHEMA.tables'... Found 238,204 rows
#            Mirroring 'SCHEMA.tables' (238,204 rows, ~239 batches)
#            Processing mirrored_table: Batch 187/187 (100.0%, 186,376/186,376 rows) - Est. remaining: 0h 0m 0s
#            Table 'SCHEMA.mirrored_table' mirroring complete - 186,376 total rows transferred
#              ✓ Table 'SCHEMA.mirrored_table' mirrored successfully
#
#   Make sure you install jpype and psychopg2, if you're using the .JAR files as I have, this was tested to work on OpenJDK 21
#
#   The PostgreSQL setup is pretty basic: Create a database, create a user, assign all rights to the created database to the user, and you're all set!


#!/usr/bin/env python
import os
import re
import sys
import time
import logging
import threading
import psycopg2
import jaydebeapi
import concurrent.futures
from typing import Dict, List, Any, Optional, Tuple, Set

    
JAR_FILE = ""
DRIVER_CLASS = ""
PROGRESS_HOST = ""
PROGRESS_PORT = ""  
PROGRESS_DB = ""
PROGRESS_USER = ""
PROGRESS_SCHEMA= ""
PROGRESS_PASS = ""
PG_CONN_STRING = ""
MAX_WORKERS = 10
LOG_FILE = ""
BATCH_SIZE = 15000
EXCLUDE_TABLES = []  
IGNORE_FILE = "/tmp/import_logs/ignored_tables.txt"

class CursorManager:
    
    def __init__(self, max_cursors=4):
        self.max_cursors = max_cursors
        self.semaphore = threading.Semaphore(max_cursors)
        self.active_cursors = {}  
        self.lock = threading.Lock()
        self.cursor_id_counter = 0
        self.logger = logging.getLogger("CursorManager")
    
    def acquire(self, description=""):
        """Acquire a cursor slot, blocking until one is available"""
        acquired = self.semaphore.acquire(blocking=True, timeout=60)
        if not acquired:
            raise TimeoutError(f"Could not acquire cursor slot for {description} after 60s timeout")
        
        with self.lock:
            self.cursor_id_counter += 1
            cursor_id = self.cursor_id_counter
            self.active_cursors[cursor_id] = description
            active_count = len(self.active_cursors)
            
            self.logger.info(f"Acquired cursor {cursor_id} ({description}). Active: {active_count}/{self.max_cursors}")
            
            # Double-check we haven't exceeded our limit
            if active_count > self.max_cursors:
                self.logger.error(f"ERROR: Active cursor count ({active_count}) exceeds limit ({self.max_cursors})!")
                # Force release one slot to maintain invariant
                self.semaphore.release()
                raise RuntimeError(f"Cursor accounting error: count {active_count} exceeds limit {self.max_cursors}")
            
            return cursor_id
    
    def release(self, cursor_id, description=""):
        """Release a cursor slot"""
        with self.lock:
            if cursor_id not in self.active_cursors:
                self.logger.warning(f"Attempted to release unknown cursor {cursor_id} ({description})")
                return False
            
            orig_description = self.active_cursors.pop(cursor_id)
            active_count = len(self.active_cursors)
            self.logger.info(f"Released cursor {cursor_id} ({orig_description}). Active: {active_count}/{self.max_cursors}")
        
        self.semaphore.release()
        return True
    
    def get_active_cursors(self):
        """Get current active cursor info for debugging"""
        with self.lock:
            return self.active_cursors.copy()
              
class CursorPool:
    def __init__(self, max_cursors: int = 4):
        self.max_cursors = max_cursors
        self.semaphore = threading.Semaphore(max_cursors)
        self.active_count = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger("CursorPool")
    
    def acquire(self, description: str = "") -> bool:
        acquired = self.semaphore.acquire(blocking=True, timeout=60)
        if acquired:
            with self.lock:
                self.active_count += 1
                self.logger.info(f"Acquired cursor ({description}). Active: {self.active_count}/{self.max_cursors}")
        return acquired
    
    def release(self, description: str = "") -> None:
        with self.lock:
            self.active_count -= 1
            self.logger.info(f"Released cursor ({description}). Active: {self.active_count}/{self.max_cursors}")
        self.semaphore.release()

class LoggingManager:
    def __init__(self, log_file: str, log_level=logging.INFO):
        self.log_file = log_file
        logging.basicConfig(
            filename=log_file, 
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.print_lock = threading.Lock()
    
    def log(self, message: str, level=logging.INFO, console=True, end="\n"):
        with self.print_lock:
            if console:
                print(message, end=end)
                sys.stdout.flush()
            logging.log(level, message)
    
    def progress(self, message: str):
        with self.print_lock:
            print(f"\r{message}", end="")
            sys.stdout.flush()
            logging.info(message)
    
    def format_time(self, seconds: float) -> str:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

class IgnoreListManager:
    def __init__(self, ignore_file: str):
        self.ignore_file = ignore_file
        self.ignore_set = set()
        self.load_ignore_list()
        self.lock = threading.Lock()
        
    def load_ignore_list(self) -> None:
        if os.path.exists(self.ignore_file):
            with open(self.ignore_file, 'r') as f:
                self.ignore_set = {line.strip() for line in f if line.strip()}
    
    def add(self, table_name: str) -> None:
        with self.lock:
            self.ignore_set.add(table_name)
            with open(self.ignore_file, 'a') as f:
                f.write(f"{table_name}\n")
    
    def is_ignored(self, table_name: str) -> bool:
        return table_name in self.ignore_set
    
    def get_all(self) -> Set[str]:
        return self.ignore_set.copy()

class ProgressConnector:
    def __init__(self, jar_path: str, driver_class: str, host: str, port: int, 
                db_name: str, username: str, password: str, schema: str, 
                cursor_manager: CursorManager, logger: LoggingManager, ignore_manager: IgnoreListManager):
        self.jar_path = jar_path
        self.driver_class = driver_class
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.schema = schema
        self.connection = None
        self.cursor_manager = cursor_manager
        self.logger = logger
        self.ignore_manager = ignore_manager
    
    def connect(self) -> bool:
        try:
            self.logger.log(f"Connecting to Progress DB ({self.host}:{self.port})...", end="")
            jdbc_url = f"jdbc:datadirect:openedge://{self.host}:{self.port};databaseName={self.db_name}"
            
            self.connection = jaydebeapi.connect(
                self.driver_class,
                jdbc_url,
                [self.username, self.password],
                self.jar_path
            )
            
            # Disable autocommit for better control
            try:
                if hasattr(self.connection, 'jconn'):
                    self.connection.jconn.setAutoCommit(False)
            except Exception as e:
                self.logger.log(f"Warning: Could not set autocommit: {e}", level=logging.WARNING)
            
            self.logger.log(" Connected!")
            return True
        except Exception as e:
            self.logger.log(f" Failed!\nConnection error: {str(e)}", level=logging.ERROR)
            return False
    
    def execute_with_cursor(self, operation_func, description=""):
        """
        Execute a function that requires a cursor with proper resource management
        
        Args:
            operation_func: Function that takes a cursor as its only argument
            description: Description of the operation for logging
            
        Returns:
            The result of operation_func or None if an error occurred
        """
        if not self.connection:
            raise ConnectionError("Not connected to database")
        
        cursor = None
        cursor_id = None
        
        try:
            # Acquire cursor slot
            cursor_id = self.cursor_manager.acquire(description)
            
            # Create actual cursor
            cursor = self.connection.cursor()
            
            # Execute the operation
            result = operation_func(cursor)
            
            # Commit the transaction
            self.connection.commit()
            
            return result
        except Exception as e:
            self.logger.log(f"Error in {description}: {str(e)}", level=logging.ERROR)
            
            # Attempt rollback
            try:
                if self.connection:
                    self.connection.rollback()
            except Exception as rollback_error:
                self.logger.log(f"Rollback error: {str(rollback_error)}", level=logging.ERROR)
            
            # Add problem table to ignore list if appropriate
            if description.startswith("get_") or description.startswith("count_") or description.startswith("fetch_"):
                table_name = "_".join(description.split("_")[1:])
                if table_name:
                    self.ignore_manager.add(table_name)
            
            return None
        finally:
            # Always close the cursor
            if cursor:
                try:
                    cursor.close()
                except Exception as close_error:
                    self.logger.log(f"Error closing cursor: {str(close_error)}", level=logging.ERROR)
            
            # Always release the cursor slot
            if cursor_id:
                self.cursor_manager.release(cursor_id, description)
    
    def get_tables(self) -> List[Dict[str, str]]:
        """Get list of tables using metadata API with proper cursor management"""
        def get_tables_operation(cursor):
            tables = []
            self.logger.log(f"Fetching table list from schema '{self.schema}'...", end="")
            
            metadata = self.connection.jconn.getMetaData()
            result_set = metadata.getTables(None, self.schema, "%", ["TABLE"])
            
            while result_set.next():
                table_info = {
                    "schema": result_set.getString("TABLE_SCHEM"),
                    "name": result_set.getString("TABLE_NAME")
                }
                tables.append(table_info)
            
            self.logger.log(f" Found {len(tables)} tables")
            return tables
        
        return self.execute_with_cursor(get_tables_operation, "get_tables") or []
    
    def get_columns(self, schema: str, table_name: str) -> List[Dict[str, Any]]:
        """Get column metadata with proper cursor management"""
        description = f"get_columns_{schema}_{table_name}"
        
        def get_columns_operation(cursor):
            columns = []
            self.logger.log(f"Fetching columns for table '{schema}.{table_name}'...", end="")
            
            metadata = self.connection.jconn.getMetaData()
            result_set = metadata.getColumns(None, schema, table_name, "%")
            
            while result_set.next():
                column = {
                    "name": result_set.getString("COLUMN_NAME"),
                    "type": result_set.getString("TYPE_NAME"),
                    "size": result_set.getInt("COLUMN_SIZE"),
                    "nullable": bool(result_set.getInt("NULLABLE")),
                    "position": result_set.getInt("ORDINAL_POSITION")
                }
                columns.append(column)
            
            # Sort columns by position
            columns.sort(key=lambda x: x["position"])
            
            self.logger.log(f" Found {len(columns)} columns")
            return columns
        
        return self.execute_with_cursor(get_columns_operation, description) or []
    
    def get_row_count(self, schema: str, table_name: str) -> int:
        """Get row count with proper cursor management"""
        description = f"count_{schema}_{table_name}"
        
        def count_operation(cursor):
            self.logger.log(f"Counting rows in '{schema}.{table_name}'...", end="")
            
            qualified_table = f"\"{schema}\".\"{table_name}\""
            query = f"SELECT COUNT(*) FROM {qualified_table}"
            cursor.execute(query)
            
            row = cursor.fetchone()
            count = row[0] if row else 0
            
            self.logger.log(f" Found {count:,} rows")
            return count
        
        result = self.execute_with_cursor(count_operation, description)
        return result if result is not None else 0
    
    def fetch_data(self, schema: str, table_name: str, column_names: List[str], 
                  batch_size: int, callback) -> int:
        """Fetch and process data with proper cursor management"""
        description = f"fetch_{schema}_{table_name}"
        
        # Get row count first
        total_row_count = self.get_row_count(schema, table_name)
        if total_row_count == 0:
            self.logger.log(f"Table '{schema}.{table_name}' has 0 rows. Skipping data transfer.")
            return 0
        
        estimated_batches = (total_row_count + batch_size - 1) // batch_size
        
        def fetch_operation(cursor):
            start_time = time.time()
            total_rows = 0
            
            self.logger.log(f"Mirroring '{schema}.{table_name}' ({total_row_count:,} rows, ~{estimated_batches} batches)")
            
            # Try to set fetch size for better performance
            try:
                if hasattr(cursor, '_cursor'):
                    cursor._cursor.setFetchSize(batch_size)
            except:
                pass
            
            columns_str = ", ".join([f"\"{col}\"" for col in column_names])
            qualified_table = f"\"{schema}\".\"{table_name}\""
            query = f"SELECT {columns_str} FROM {qualified_table}"
            
            cursor.execute(query)
            
            current_batch = []
            batch_count = 0
            
            # Process rows in batches
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                
                current_batch.append(row)
                if len(current_batch) >= batch_size:
                    batch_count += 1
                    total_rows += len(current_batch)
                    
                    # Calculate time estimate
                    elapsed_time = time.time() - start_time
                    rows_per_second = total_rows / max(0.1, elapsed_time)
                    remaining_rows = total_row_count - total_rows
                    estimated_seconds = remaining_rows / max(0.1, rows_per_second)
                    estimated_time = self.logger.format_time(estimated_seconds)
                    
                    # Process the batch
                    callback(
                        current_batch, 
                        batch_count, 
                        estimated_batches, 
                        total_rows, 
                        total_row_count, 
                        estimated_time
                    )
                    
                    current_batch = []
            
            # Process any remaining rows
            if current_batch:
                batch_count += 1
                total_rows += len(current_batch)
                
                elapsed_time = time.time() - start_time
                rows_per_second = total_rows / max(0.1, elapsed_time)
                remaining_rows = total_row_count - total_rows
                estimated_seconds = remaining_rows / max(0.1, rows_per_second)
                estimated_time = self.logger.format_time(estimated_seconds)
                
                callback(
                    current_batch, 
                    batch_count, 
                    estimated_batches, 
                    total_rows, 
                    total_row_count, 
                    estimated_time
                )
            
            self.logger.log("")  # Add newline after progress updates
            return total_rows
        
        result = self.execute_with_cursor(fetch_operation, description)
        return result if result is not None else 0
    
class PostgresConnector:
    def __init__(self, conn_string: str, logger: LoggingManager):
        self.conn_string = conn_string
        self.logger = logger
        self.connection = None
        self.connections = {}
        self.conn_lock = threading.Lock()
    
    def connect(self) -> bool:
        try:
            self.logger.log("Connecting to PostgreSQL...", end="")
            self.connection = psycopg2.connect(self.conn_string)
            self.logger.log(" Connected!")
            return True
        except Exception as e:
            self.logger.log(f" Failed!\nPostgreSQL connection error: {str(e)}", level=logging.ERROR)
            return False
    
    def disconnect(self) -> None:
        if self.connection:
            try:
                self.logger.log("Disconnecting from PostgreSQL...", end="")
                self.connection.close()
                self.logger.log(" Done")
            except Exception as e:
                self.logger.log(f" Failed!\nPostgreSQL disconnect error: {str(e)}", level=logging.ERROR)
            finally:
                self.connection = None
        
        with self.conn_lock:
            for tid, conn in list(self.connections.items()):
                try:
                    conn.close()
                except:
                    pass
            self.connections.clear()
    
    def get_connection(self):
        """Get a thread-specific connection from the pool"""
        tid = threading.get_ident()
        with self.conn_lock:
            if tid not in self.connections:
                self.connections[tid] = psycopg2.connect(self.conn_string)
                # Set ideal isolation level for maximum performance for concurrent writes
                self.connections[tid].set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            return self.connections[tid]
    
    def map_to_postgres_type(self, oe_type: str, size: int) -> str:
        if hasattr(oe_type, 'toString'):
            oe_type = str(oe_type.toString())
        else:
            oe_type = str(oe_type)
        
        oe_type = oe_type.upper()
        
        type_mapping = {
            "INTEGER": "INTEGER",
            "INT": "INTEGER",
            "SMALLINT": "SMALLINT",
            "DECIMAL": "NUMERIC",
            "NUMERIC": "NUMERIC",
            "DOUBLE PRECISION": "DOUBLE PRECISION",
            "FLOAT": "DOUBLE PRECISION",
            "DATE": "DATE",
            "DATETIME": "TIMESTAMP",
            "TIMESTAMP": "TIMESTAMP",
            "TIME": "TIME",
            "CHAR": f"CHAR({min(size, 10485760)})",
            "CHARACTER": f"CHAR({min(size, 10485760)})",
            "VARCHAR": f"VARCHAR({min(size, 10485760)})",
            "CHARACTER VARYING": f"VARCHAR({min(size, 10485760)})",
            "LONG VARCHAR": "TEXT",
            "LONG VARBINARY": "BYTEA",
            "CLOB": "TEXT",
            "BLOB": "BYTEA",
            "BINARY": "BYTEA",
            "VARBINARY": "BYTEA",
            "LOGICAL": "BOOLEAN"
        }
        
        return type_mapping.get(oe_type, "TEXT")
    
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
        
        cursor = None
        
        try:
            self.logger.log(f"Creating table '{table_name}' in PostgreSQL...", end="")
            cursor = self.connection.cursor()
            
            column_defs = []
            for col in columns:
                pg_type = self.map_to_postgres_type(col["type"], col["size"])
                nullable = "" if col["nullable"] else " NOT NULL"
                column_defs.append(f"\"{col['name']}\" {pg_type}{nullable}")
            
            # Add unique index creation if possible
            create_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(column_defs)})"
            cursor.execute(create_query)
            
            # Consider adding optimized indexes
            self.connection.commit()
            self.logger.log(" Done")
            return True
        except Exception as e:
            self.logger.log(f" Failed!\nError creating table: {str(e)}", level=logging.ERROR)
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    def truncate_table(self, table_name: str) -> bool:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
        
        cursor = None
        
        try:
            self.logger.log(f"Truncating table '{table_name}' in PostgreSQL...", end="")
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE \"{table_name}\"")
            self.connection.commit()
            self.logger.log(" Done")
            return True
        except Exception as e:
            self.logger.log(f" Failed!\nError truncating table: {str(e)}", level=logging.ERROR)
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    def table_exists(self, table_name: str) -> bool:
        conn = self.get_connection()
        cursor = None
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            result = cursor.fetchone()[0]
            conn.commit()
            return result
        except Exception as e:
            self.logger.log(f"Error checking if table {table_name} exists: {str(e)}", level=logging.ERROR)
            conn.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    def get_row_count(self, table_name: str) -> int:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
        
        if not self.table_exists(table_name):
            return 0
        
        cursor = None
        
        try:
            self.logger.log(f"Counting rows in PostgreSQL table '{table_name}'...", end="")
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            count = cursor.fetchone()[0]
            self.connection.commit()
            self.logger.log(f" Found {count:,} rows")
            return count
        except Exception as e:
            self.logger.log(f" Failed!\nError counting rows: {str(e)}", level=logging.ERROR)
            self.connection.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
    
    def insert_data(self, table_name: str, column_names: List[str], batch: List[List], 
                   batch_num: int, total_batches: int, current_rows: int, total_rows: int, 
                   estimated_time: str = "") -> int:
        conn = self.get_connection()
        if not batch:
            return 0
        
        cursor = None
        
        try:
            percent = min(100, (current_rows / max(1, total_rows)) * 100)
            if estimated_time:
                progress_msg = f"Processing {table_name}: Batch {batch_num}/{total_batches} ({percent:.1f}%, {current_rows:,}/{total_rows:,} rows) - Est. remaining: {estimated_time}"
            else:
                progress_msg = f"Processing {table_name}: Batch {batch_num}/{total_batches} ({percent:.1f}%, {current_rows:,}/{total_rows:,} rows)"
            
            self.logger.progress(progress_msg)
            
            cursor = conn.cursor()
            
            placeholders = ', '.join(['%s'] * len(column_names))
            quoted_columns = [f"\"{col}\"" for col in column_names]
            insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({placeholders})"
            
            # Use efficient executemany for bulk inserts
            cursor.executemany(insert_query, batch)
            conn.commit()
            
            return len(batch)
        except Exception as e:
            self.logger.log(f"\nError inserting data: {str(e)}", level=logging.ERROR)
            conn.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()


class DBMirror:
    def __init__(
        self, 
        progress_jar: str,
        driver_class: str,
        progress_host: str, 
        progress_port: int, 
        progress_db: str, 
        progress_user: str, 
        progress_pass: str,
        progress_schema: str,
        postgres_conn_string: str,
        batch_size: int = 1000,
        max_workers: int = 4,
        log_file: str = "dbmirror.log",
        ignore_file: str = "ignored_tables.txt"
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # Initialize managers
        self.logger = LoggingManager(log_file)
        self.cursor_manager = CursorManager(max_cursors=4)
        self.ignore_manager = IgnoreListManager(ignore_file)
        
        self.progress = ProgressConnector(
            progress_jar,
            driver_class,
            progress_host,
            progress_port,
            progress_db,
            progress_user,
            progress_pass,
            progress_schema,
            self.cursor_manager,
            self.logger,
            self.ignore_manager
        )
        
        self.postgres = PostgresConnector(postgres_conn_string, self.logger)
        
        # For tracking results
        self.results = {}
        self.results_lock = threading.Lock()

    def get_primary_key(self, schema: str, table_name: str) -> Optional[str]:
        """Retrieve the primary key column for a table, if available."""
        def get_pk_operation(cursor):
            metadata = self.progress.connection.jconn.getMetaData()
            result_set = metadata.getPrimaryKeys(None, schema, table_name)
            pk_column = None
            while result_set.next():
                pk_column = result_set.getString("COLUMN_NAME")
                break  # Assume single-column PK for simplicity
            return pk_column
        
        return self.progress.execute_with_cursor(get_pk_operation, f"get_pk_{schema}_{table_name}")

    def get_row_hashes(self, connector, schema: str, table_name: str, column_names: List[str], 
                      pk_column: Optional[str], limit: int = None) -> Dict[Any, str]:
        """Compute a hash of rows for comparison, using PK if available."""
        hashes = {}
        is_progress = isinstance(connector, ProgressConnector)
        description = f"hash_{schema}_{table_name}"

        def hash_operation(cursor):
            nonlocal hashes
            columns = ", ".join([f"\"{col}\"" for col in column_names])
            table_ref = f"\"{schema}\".\"{table_name}\"" if is_progress else f"\"{table_name}\""
            query = f"SELECT {columns} FROM {table_ref}"
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                row_data = list(row)
                if pk_column:
                    pk_index = column_names.index(pk_column)
                    pk_value = row_data[pk_index]
                    row_hash = hash(tuple(row_data))  # Simple hash of row contents
                    hashes[pk_value] = str(row_hash)
                else:
                    # Fallback: use entire row as key
                    row_hash = hash(tuple(row_data))
                    hashes[row_hash] = str(row_hash)
            
            return hashes
        
        if is_progress:
            return connector.execute_with_cursor(hash_operation, description) or {}
        else:
            try:
                conn = connector.get_connection()
                cursor = conn.cursor()
                result = hash_operation(cursor)
                conn.commit()
                return result
            finally:
                if cursor:
                    cursor.close()

    def mirror_table(self, schema: str, table_name: str, pg_table_name: str = None, truncate: bool = False) -> bool:
        if pg_table_name is None:
            pg_table_name = table_name
        
        self.logger.log(f"\n=== Mirroring table: {schema}.{table_name} to {pg_table_name} ===")
        
        try:
            # Check if table should be ignored
            if self.ignore_manager.is_ignored(f"{schema}.{table_name}"):
                self.logger.log(f"Table {schema}.{table_name} is in the ignore file. Skipping.")
                return False
            
            # Get column info
            columns = self.progress.get_columns(schema, table_name)
            if not columns:
                self.logger.log(f"No columns found for table {schema}.{table_name}")
                self.ignore_manager.add(f"{schema}.{table_name}")
                return False
            
            column_names = [col["name"] for col in columns]
            
            # Create table in Postgres if it doesn't exist
            if not self.postgres.table_exists(pg_table_name):
                if not self.postgres.create_table(pg_table_name, columns):
                    self.ignore_manager.add(f"{schema}.{table_name}")
                    return False
            
            # Get primary key
            pk_column = self.get_primary_key(schema, table_name)
            self.logger.log(f"Primary key for {schema}.{table_name}: {pk_column if pk_column else 'None'}")
            
            # Get row counts
            oe_row_count = self.progress.get_row_count(schema, table_name)
            pg_row_count = self.postgres.get_row_count(pg_table_name)
            
            if self.ignore_manager.is_ignored(f"{schema}.{table_name}"):
                return False
            
            if oe_row_count == 0:
                self.logger.log(f"Table {schema}.{table_name} has 0 rows. No data to transfer.")
                if pg_row_count > 0 and truncate:
                    self.postgres.truncate_table(pg_table_name)
                return True
            
            # Compare data to find differences
            if pg_row_count == oe_row_count and pg_row_count > 0:
                self.logger.log(f"Comparing data in {schema}.{table_name}...")
                # Sample hashes to check for differences
                progress_hashes = self.get_row_hashes(self.progress, schema, table_name, column_names, pk_column)
                postgres_hashes = self.get_row_hashes(self.postgres, schema, pg_table_name, column_names, pk_column)
                
                if pk_column:
                    progress_keys = set(progress_hashes.keys())
                    postgres_keys = set(postgres_hashes.keys())
                    
                    new_keys = progress_keys - postgres_keys
                    common_keys = progress_keys & postgres_keys
                    changed_keys = {k for k in common_keys if progress_hashes[k] != postgres_hashes[k]}
                    
                    if not new_keys and not changed_keys:
                        self.logger.log(f"Table {schema}.{table_name} is up to date. Skipping.")
                        return True
                else:
                    # Without PK, compare entire row hashes
                    if progress_hashes == postgres_hashes:
                        self.logger.log(f"Table {schema}.{table_name} is up to date. Skipping.")
                        return True
            
            # If truncate is explicitly requested, truncate and fetch all
            if truncate:
                self.logger.log(f"Truncate requested for {pg_table_name}. Fetching all data.")
                if not self.postgres.truncate_table(pg_table_name):
                    self.ignore_manager.add(f"{schema}.{table_name}")
                    return False
                adjusted_batch_size = self.batch_size
                if oe_row_count > 1000000:
                    adjusted_batch_size = min(10000, self.batch_size * 5)
                elif oe_row_count < 1000:
                    adjusted_batch_size = max(100, self.batch_size // 2)
                
                def data_callback(batch, batch_num, total_batches, current_rows, total_rows, estimated_time):
                    self.postgres.insert_data(
                        pg_table_name, column_names, batch, 
                        batch_num, total_batches, current_rows, total_rows, 
                        estimated_time
                    )
                
                total_rows = self.progress.fetch_data(
                    schema=schema,
                    table_name=table_name,
                    column_names=column_names,
                    batch_size=adjusted_batch_size,
                    callback=data_callback
                )
                
                self.logger.log(f"Table '{schema}.{table_name}' mirroring complete - {total_rows:,} total rows transferred")
                return True
            
            # Incremental update
            self.logger.log(f"Performing incremental update for {schema}.{table_name}...")
            adjusted_batch_size = self.batch_size
            if oe_row_count > 1000000:
                adjusted_batch_size = min(10000, self.batch_size * 5)
            elif oe_row_count < 1000:
                adjusted_batch_size = max(100, self.batch_size // 2)
            
            def data_callback(batch, batch_num, total_batches, current_rows, total_rows, estimated_time):
                if pk_column:
                    # Split batch into new and updated rows
                    new_rows = []
                    update_rows = []
                    pk_index = column_names.index(pk_column)
                    
                    for row in batch:
                        pk_value = row[pk_index]
                        row_hash = str(hash(tuple(row)))
                        if pk_value not in postgres_hashes:
                            new_rows.append(row)
                        elif postgres_hashes.get(pk_value) != row_hash:
                            update_rows.append(row)
                    
                    # Insert new rows
                    if new_rows:
                        self.postgres.insert_data(
                            pg_table_name, column_names, new_rows,
                            batch_num, total_batches, current_rows, total_rows,
                            estimated_time
                        )
                    
                    # Update changed rows
                    if update_rows:
                        conn = self.postgres.get_connection()
                        cursor = None
                        try:
                            cursor = conn.cursor()
                            update_columns = [col for col in column_names if col != pk_column]
                            set_clause = ", ".join([f"\"{col}\" = %s" for col in update_columns])
                            update_query = f"UPDATE \"{pg_table_name}\" SET {set_clause} WHERE \"{pk_column}\" = %s"
                            
                            for row in update_rows:
                                pk_index = column_names.index(pk_column)
                                update_values = [row[column_names.index(col)] for col in update_columns]
                                update_values.append(row[pk_index])
                                cursor.execute(update_query, update_values)
                            
                            conn.commit()
                        except Exception as e:
                            self.logger.log(f"Error updating rows: {str(e)}", level=logging.ERROR)
                            conn.rollback()
                        finally:
                            if cursor:
                                cursor.close()
                else:
                    # Without PK, insert all rows (fallback to insert-only)
                    self.postgres.insert_data(
                        pg_table_name, column_names, batch,
                        batch_num, total_batches, current_rows, total_rows,
                        estimated_time
                    )
            
            # Modify fetch_data to filter rows if PK exists
            if pk_column:
                def fetch_filtered_data(cursor):
                    start_time = time.time()
                    total_rows = 0
                    
                    columns_str = ", ".join([f"\"{col}\"" for col in column_names])
                    qualified_table = f"\"{schema}\".\"{table_name}\""
                    
                    # Fetch only new or changed rows
                    progress_hashes = self.get_row_hashes(self.progress, schema, table_name, column_names, pk_column)
                    postgres_hashes = self.get_row_hashes(self.postgres, schema, pg_table_name, column_names, pk_column)
                    
                    new_keys = set(progress_hashes.keys()) - set(postgres_hashes.keys())
                    changed_keys = {k for k in progress_hashes.keys() if k in postgres_hashes and progress_hashes[k] != postgres_hashes[k]}
                    keys_to_fetch = new_keys | changed_keys
                    
                    if not keys_to_fetch:
                        self.logger.log("No new or changed rows to fetch.")
                        return 0
                    
                    # Fetch in batches
                    key_list = list(keys_to_fetch)
                    batch_size = adjusted_batch_size
                    total_row_count = len(key_list)
                    estimated_batches = (total_row_count + batch_size - 1) // batch_size
                    
                    self.logger.log(f"Fetching {total_row_count:,} new/changed rows from '{schema}.{table_name}'")
                    
                    current_batch = []
                    batch_count = 0
                    
                    for i in range(0, len(key_list), batch_size):
                        batch_keys = key_list[i:i + batch_size]
                        placeholders = ", ".join(["?" for _ in batch_keys])
                        query = f"SELECT {columns_str} FROM {qualified_table} WHERE \"{pk_column}\" IN ({placeholders})"
                        cursor.execute(query, batch_keys)
                        
                        batch_rows = []
                        while True:
                            row = cursor.fetchone()
                            if row is None:
                                break
                            batch_rows.append(row)
                        
                        if batch_rows:
                            batch_count += 1
                            total_rows += len(batch_rows)
                            
                            elapsed_time = time.time() - start_time
                            rows_per_second = total_rows / max(0.1, elapsed_time)
                            remaining_rows = total_row_count - total_rows
                            estimated_seconds = remaining_rows / max(0.1, rows_per_second)
                            estimated_time = self.logger.format_time(estimated_seconds)
                            
                            data_callback(
                                batch_rows, batch_count, estimated_batches,
                                total_rows, total_row_count, estimated_time
                            )
                    
                    self.logger.log("")  # Newline after progress
                    return total_rows
                
                total_rows = self.progress.execute_with_cursor(fetch_filtered_data, f"fetch_filtered_{schema}_{table_name}") or 0
            else:
                # Fallback: fetch all data
                total_rows = self.progress.fetch_data(
                    schema=schema,
                    table_name=table_name,
                    column_names=column_names,
                    batch_size=adjusted_batch_size,
                    callback=data_callback
                )
            
            self.logger.log(f"Table '{schema}.{table_name}' mirroring complete - {total_rows:,} total rows processed")
            return True
        except Exception as e:
            self.logger.log(f"Error mirroring table {schema}.{table_name}: {str(e)}", level=logging.ERROR)
            self.ignore_manager.add(f"{schema}.{table_name}")
            return False

    # Rest of the DBMirror class remains unchanged
    def process_table(self, table_info, index, total):
        schema = table_info["schema"]
        table_name = table_info["name"]
        
        self.logger.log(f"\nProcessing table {index}/{total}: {schema}.{table_name}")
        
        success = self.mirror_table(schema, table_name)
        
        with self.results_lock:
            self.results[f"{schema}.{table_name}"] = success
        
        if success:
            self.logger.log(f"  ✓ Table '{schema}.{table_name}' mirrored successfully")
        else:
            self.logger.log(f"  ✗ Failed to mirror table '{schema}.{table_name}'")
    
    def mirror_all_tables(self, exclude_tables: List[str] = None) -> Dict[str, bool]:
        if exclude_tables is None:
            exclude_tables = []
        
        # Combine exclude tables with ignored tables
        ignored_tables = self.ignore_manager.get_all()
        exclude_set = set(exclude_tables).union(ignored_tables)
        
        self.logger.log(f"Tables in ignore file: {len(ignored_tables)}")
        self.logger.log("\n=== Starting database mirroring process ===\n")
        
        # Establish connections
        if not self.progress.connect():
            self.logger.log("Failed to connect to Progress database")
            return {}
        
        if not self.postgres.connect():
            self.logger.log("Failed to connect to PostgreSQL database")
            self.progress.disconnect()
            return {}
        
        try:
            # Get table list
            tables = self.progress.get_tables()
            self.results = {}
            
            self.logger.log(f"\nTotal tables to process: {len(tables)}")
            
            if exclude_set:
                self.logger.log(f"Tables to exclude: {len(exclude_set)}")
                
            # Filter tables
            included_tables = []
            for t in tables:
                full_name = f"{t['schema']}.{t['name']}"
                if full_name not in exclude_set and t["name"] not in exclude_set:
                    included_tables.append(t)
                
            self.logger.log(f"Tables to mirror: {len(included_tables)}")
            
            # Get table sizes for sorting
            table_sizes = {}
            for t in included_tables:
                schema = t["schema"]
                table_name = t["name"]
                size = self.progress.get_row_count(schema, table_name)
                table_sizes[f"{schema}.{table_name}"] = size
            
            # Sort tables by size (smallest first for quick wins)
            sorted_tables = sorted(included_tables, 
                                key=lambda t: table_sizes.get(f"{t['schema']}.{t['name']}", 0))
            
            # Calculate total estimated time
            total_rows = sum(table_sizes.values())
            estimated_rows_per_sec = 500  # Conservative estimate
            total_estimated_seconds = total_rows / estimated_rows_per_sec
            self.logger.log(f"Estimated total time: {self.logger.format_time(total_estimated_seconds)}")
            
            # Process tables
            if self.max_workers > 1:
                self._parallel_process_tables(sorted_tables)
            else:
                self._sequential_process_tables(sorted_tables)
                
            return self.results
            
        except Exception as e:
            self.logger.log(f"Error during mirroring: {str(e)}", level=logging.ERROR)
            return {}
            
        finally:
            self.progress.disconnect()
    
    def _parallel_process_tables(self, tables):
        """Process tables in parallel using a thread pool"""
        self.logger.log(f"Processing tables using {self.max_workers} parallel workers")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for i, table_info in enumerate(tables, 1):
                futures.append(
                    executor.submit(self.process_table, table_info, i, len(tables))
                )
                
            # Wait for all to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.log(f"Error in table processing thread: {str(e)}", level=logging.ERROR)
    
    def _sequential_process_tables(self, tables):
        """Process tables sequentially"""
        for i, table_info in enumerate(tables, 1):
            self.process_table(table_info, i, len(tables))
            if i < len(tables):
                time.sleep(1)  # Small pause between tables
    
    def generate_summary_report(self):
        """Generate a summary report of the migration"""
        if not self.results:
            return "No migration results available."
        
        success_count = sum(1 for success in self.results.values() if success)
        total_count = len(self.results)
        
        report = []
        report.append("=== Migration Summary ===")
        report.append(f"Tables processed: {total_count}")
        report.append(f"Successfully mirrored: {success_count}")
        report.append(f"Failed tables: {total_count - success_count}")
        
        if total_count - success_count > 0:
            failed_tables = [table for table, success in self.results.items() if not success]
            report.append(f"Failed tables: {', '.join(failed_tables)}")
        
        return "\n".join(report)

def main():

    
    # Print configuration summary
    print(f"\n=== Progress to PostgreSQL Database Mirror Tool ===\n")
    print(f"Configuration:")
    print(f"  - Progress DB: {PROGRESS_HOST}:{PROGRESS_PORT}/{PROGRESS_DB}")
    print(f"  - Progress Schema: {PROGRESS_SCHEMA}")
    print(f"  - JDBC Driver: {DRIVER_CLASS}")
    print(f"  - JAR File: {JAR_FILE}")
    print(f"  - Batch Size: {BATCH_SIZE}")
    print(f"  - Max Workers: {MAX_WORKERS}")
    print(f"  - Log File: {LOG_FILE}")
    print(f"  - Ignore File: {IGNORE_FILE}")
    
    # Initialize and run the mirror process
    mirror = DBMirror(
        JAR_FILE,
        DRIVER_CLASS,
        PROGRESS_HOST,
        PROGRESS_PORT,
        PROGRESS_DB,
        PROGRESS_USER,
        PROGRESS_PASS,
        PROGRESS_SCHEMA,
        PG_CONN_STRING,
        BATCH_SIZE,
        MAX_WORKERS,
        LOG_FILE,
        IGNORE_FILE
    )
    
    start_time = time.time()
    results = mirror.mirror_all_tables(EXCLUDE_TABLES)
    end_time = time.time()
    
    # Print summary report
    print(mirror.generate_summary_report())
    
    # Print timing info
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print(f"Total time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"Detailed log available in: {LOG_FILE}")
    print(f"Ignored tables list: {IGNORE_FILE}")

if __name__ == "__main__":
    main()

