# Progress to PostgreSQL Database Mirror, this was designed to enable users to retrieve the total data and structure in a Progress OpenEdge database. 
#  The tools works like this:
#   1. Connect to both databases
#   2. Query the SYS tables for the schema
#   3. In alphabetical order, query each table on the Progress side
#   4. Create a corresponding PostgreSQL table
#   5. Replicate the Progress table to the PostgreSQL table
#   6. For the user's information, this script takes a row count and provides a status indicator as it runs
#             Processing table1: Batch 13/13 (100.0%, 12,474/12,474 rows)                        -> Pulling from Progress
#             Table 'SCHEMA0.table1' mirroring complete - 12,474 total rows transferred          -> Tables pulled successfully
#             ✓ Table 'SCHEMA0.table1' mirrored successfully                                     -> Tables created in PostgreSQL
#
#   Make sure you install jpype and psychopg2, if you're using the .JAR files as I have, this was tested to work on OpenJDK 21
#
#   The PostgreSQL setup is pretty basic: Create a database, create a user, assign all rights to the created database to the user, and you're all set!


import jpype
import jpype.imports
import psycopg2
import logging
import os
import time
import sys
from threading import Lock, Semaphore
from typing import List, Dict, Any, Optional, Tuple

JAR_FILE = ""
PROGRESS_HOST = ""
PROGRESS_PORT = ""
PROGRESS_DB = ""
PROGRESS_USER = ""
PROGRESS_SCHEMA = ""
PROGRESS_PASS = ""
PG_CONN_STRING = ""
MAX_CURSORS = ""
LOG_FILE = ""
BATCH_SIZE = ""
EXCLUDE_TABLES = ""
IGNORE_FILE = ""

logging.basicConfig(
    filename=LOG_FILE, 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

cursor_semaphore = Semaphore(MAX_CURSORS)


def print_status(message, end="\n", log_only=False):
    if not log_only:
        print(message, end=end)
        sys.stdout.flush()  
    logging.info(message)


def print_progress(message, log_only=False):
    if not log_only:
        print(f"\r{message}", end="")
        sys.stdout.flush()
    logging.info(message)


def add_to_ignore_file(table_name: str):
    with open(IGNORE_FILE, 'a') as f:
        f.write(f"{table_name}\n")
    print_status(f"Added {table_name} to ignore file: {IGNORE_FILE}")


def read_ignore_file() -> List[str]:
    if not os.path.exists(IGNORE_FILE):
        return []
        
    with open(IGNORE_FILE, 'r') as f:
        return [line.strip() for line in f if line.strip()]


class ProgressConnector:
    def __init__(self, jar_path: str, host: str, port: int, db_name: str, username: str, password: str, schema: str):
        self.jar_path = jar_path
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.schema = schema
        self.connection = None
        
    def start_jvm(self) -> None:
        if not jpype.isJVMStarted():
            print_status("Starting JVM...")
            jpype.startJVM(classpath=[self.jar_path])
            print_status("JVM started successfully")
            
    def connect(self) -> bool:
        self.start_jvm()
        
        try:
            print_status(f"Connecting to Progress DB ({self.host}:{self.port})...", end="")
            from java.sql import DriverManager
            from java.util import Properties
            
            jdbc_url = f"jdbc:datadirect:openedge://{self.host}:{self.port};databaseName={self.db_name}"
            props = Properties()
            props.setProperty("user", self.username)
            props.setProperty("password", self.password)
            
            self.connection = DriverManager.getConnection(jdbc_url, props)
            print_status(" Connected!")
            return True
        except Exception as e:
            print_status(f" Failed!\nConnection error: {str(e)}")
            logging.error(f"Connection error: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        if self.connection:
            try:
                print_status("Disconnecting from Progress DB...", end="")
                self.connection.close()
                print_status(" Done")
            except Exception as e:
                print_status(f" Failed!\nDisconnect error: {str(e)}")
                logging.error(f"Disconnect error: {str(e)}")
            finally:
                self.connection = None
                
    def get_tables(self) -> List[Dict[str, str]]:
        if not self.connection:
            raise ConnectionError("Not connected to database")
            
        tables = []
        stmt = None
        result = None
        
        with cursor_semaphore:
            print_status(f"Fetching table list from schema '{self.schema}'...", end="")
            try:
                metadata = self.connection.getMetaData()
                result = metadata.getTables(None, self.schema, "%", ["TABLE"])
                
                while result.next():
                    table_info = {
                        "schema": result.getString("TABLE_SCHEM"),
                        "name": result.getString("TABLE_NAME")
                    }
                    tables.append(table_info)
                    
                print_status(f" Found {len(tables)} tables")
                
            except Exception as e:
                print_status(f" Failed!\nError fetching tables: {str(e)}")
                logging.error(f"Error fetching tables: {str(e)}")
                
            finally:
                if result:
                    result.close()
                
        return tables
    
    def get_columns(self, schema: str, table_name: str) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ConnectionError("Not connected to database")
            
        columns = []
        result = None
        
        with cursor_semaphore:
            print_status(f"Fetching columns for table '{schema}.{table_name}'...", end="")
            try:
                metadata = self.connection.getMetaData()
                result = metadata.getColumns(None, schema, table_name, "%")
                
                while result.next():
                    column = {
                        "name": result.getString("COLUMN_NAME"),
                        "type": result.getString("TYPE_NAME"),
                        "size": result.getInt("COLUMN_SIZE"),
                        "nullable": bool(result.getInt("NULLABLE")),
                        "position": result.getInt("ORDINAL_POSITION")
                    }
                    columns.append(column)
                    
                print_status(f" Found {len(columns)} columns")
                
            except Exception as e:
                print_status(f" Failed!\nError fetching columns: {str(e)}")
                logging.error(f"Error fetching columns for {schema}.{table_name}: {str(e)}")
                
            finally:
                if result:
                    result.close()
                
        return columns
    
    def get_row_count(self, schema: str, table_name: str) -> int:
        if not self.connection:
            raise ConnectionError("Not connected to database")
            
        count = 0
        stmt = None
        result = None
        
        with cursor_semaphore:
            print_status(f"Counting rows in '{schema}.{table_name}'...", end="")
            try:
                stmt = self.connection.createStatement()
                qualified_table = f"\"{schema}\".\"{table_name}\""
                query = f"SELECT COUNT(*) FROM {qualified_table}"
                result = stmt.executeQuery(query)
                
                if result.next():
                    count = result.getInt(1)
                    
                print_status(f" Found {count:,} rows")
                
            except Exception as e:
                print_status(f" Failed!\nError counting rows: {str(e)}")
                logging.error(f"Error counting rows in {schema}.{table_name}: {str(e)}")
                add_to_ignore_file(f"{schema}.{table_name}")
                
            finally:
                if result:
                    result.close()
                if stmt:
                    stmt.close()
                    
        return count
    
    def fetch_and_process_data(self, schema: str, table_name: str, column_names: List[str], 
                               pg_connector, pg_table_name: str, batch_size: int = 1000) -> int:
        """Fetch data and process it in one go to avoid double-pulling data."""
        if not self.connection:
            raise ConnectionError("Not connected to database")
            
        stmt = None
        result = None
        total_rows = 0
        total_row_count = self.get_row_count(schema, table_name)
        if total_row_count == 0:
            print_status(f"Table '{schema}.{table_name}' has 0 rows. Skipping data transfer.")
            return 0
            
        estimated_batches = (total_row_count + batch_size - 1) // batch_size
        
        with cursor_semaphore:
            print_status(f"Mirroring '{schema}.{table_name}' ({total_row_count:,} rows, ~{estimated_batches} batches)")
            try:
                stmt = self.connection.createStatement()
                stmt.setFetchSize(batch_size)
                columns_str = ", ".join([f"\"{col}\"" for col in column_names])
                qualified_table = f"\"{schema}\".\"{table_name}\""
                query = f"SELECT {columns_str} FROM {qualified_table}"
                print_status(f"Executing query: {query}", log_only=True)
                result = stmt.executeQuery(query)
                
                meta = result.getMetaData()
                column_count = meta.getColumnCount()
                
                current_batch = []
                batch_count = 0
                
                while result.next():
                    row = []
                    for i in range(1, column_count + 1):
                        obj = result.getObject(i)
                        row.append(str(obj) if obj is not None else None)
                    
                    current_batch.append(row)
                    if len(current_batch) >= batch_size:
                        batch_count += 1
                        total_rows += len(current_batch)
                        rows_inserted = pg_connector.insert_data(
                            pg_table_name, column_names, current_batch, 
                            batch_count, estimated_batches, total_rows, total_row_count
                        )
                        current_batch = []
                if current_batch:
                    batch_count += 1
                    total_rows += len(current_batch)
                    rows_inserted = pg_connector.insert_data(
                        pg_table_name, column_names, current_batch, 
                        batch_count, estimated_batches, total_rows, total_row_count
                    )
                print_status("")
                    
            except Exception as e:
                print_status(f"\nError processing data from {schema}.{table_name}: {str(e)}")
                logging.error(f"Error processing data from {schema}.{table_name}: {str(e)}")
                add_to_ignore_file(f"{schema}.{table_name}")
                
            finally:
                if result:
                    result.close()
                if stmt:
                    stmt.close()
                
        return total_rows


class PostgresConnector:
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.connection = None
        
    def connect(self) -> bool:
        try:
            print_status("Connecting to PostgreSQL...", end="")
            self.connection = psycopg2.connect(self.conn_string)
            print_status(" Connected!")
            return True
        except Exception as e:
            print_status(f" Failed!\nPostgreSQL connection error: {str(e)}")
            logging.error(f"PostgreSQL connection error: {str(e)}")
            return False
            
    def disconnect(self) -> None:
        if self.connection:
            try:
                print_status("Disconnecting from PostgreSQL...", end="")
                self.connection.close()
                print_status(" Done")
            except Exception as e:
                print_status(f" Failed!\nPostgreSQL disconnect error: {str(e)}")
                logging.error(f"PostgreSQL disconnect error: {str(e)}")
            finally:
                self.connection = None
                
    def create_table(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
            
        cursor = None
        
        try:
            print_status(f"Creating table '{table_name}' in PostgreSQL...", end="")
            cursor = self.connection.cursor()
            
            column_defs = []
            for col in columns:
                column_defs.append(f"\"{col['name']}\" TEXT")
                
            create_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(column_defs)})"
            cursor.execute(create_query)
            self.connection.commit()
            print_status(" Done")
            return True
            
        except Exception as e:
            print_status(f" Failed!\nError creating table: {str(e)}")
            logging.error(f"Error creating table {table_name} in PostgreSQL: {str(e)}")
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
            print_status(f"Truncating table '{table_name}' in PostgreSQL...", end="")
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE \"{table_name}\"")
            self.connection.commit()
            print_status(" Done")
            return True
            
        except Exception as e:
            print_status(f" Failed!\nError truncating table: {str(e)}")
            logging.error(f"Error truncating table {table_name} in PostgreSQL: {str(e)}")
            self.connection.rollback()
            return False
            
        finally:
            if cursor:
                cursor.close()
    
    def table_exists(self, table_name: str) -> bool:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
            
        cursor = None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            
            result = cursor.fetchone()[0]
            return result
            
        except Exception as e:
            logging.error(f"Error checking if table {table_name} exists: {str(e)}")
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
            print_status(f"Counting rows in PostgreSQL table '{table_name}'...", end="")
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            count = cursor.fetchone()[0]
            print_status(f" Found {count:,} rows")
            return count
            
        except Exception as e:
            print_status(f" Failed!\nError counting rows: {str(e)}")
            logging.error(f"Error counting rows in PostgreSQL table {table_name}: {str(e)}")
            return 0
            
        finally:
            if cursor:
                cursor.close()
                
    def insert_data(self, table_name: str, column_names: List[str], batch: List[List], 
                   batch_num: int, total_batches: int, current_rows: int, total_rows: int) -> int:
        if not self.connection:
            raise ConnectionError("Not connected to PostgreSQL database")
            
        if not batch:
            return 0
            
        cursor = None
        
        try:
            percent = min(100, (current_rows / max(1, total_rows)) * 100)
            progress_msg = f"Processing {table_name}: Batch {batch_num}/{total_batches} ({percent:.1f}%, {current_rows:,}/{total_rows:,} rows)"
            print_progress(progress_msg)
            print_status(f"Inserting batch {batch_num}/{total_batches} ({len(batch)} rows) into '{table_name}'", log_only=True)
            
            cursor = self.connection.cursor()
            
            placeholders = ', '.join(['%s'] * len(column_names))
            quoted_columns = [f"\"{col}\"" for col in column_names]
            insert_query = f"INSERT INTO \"{table_name}\" ({', '.join(quoted_columns)}) VALUES ({placeholders})"
            
            cursor.executemany(insert_query, batch)
            self.connection.commit()
            return len(batch)
            
        except Exception as e:
            print_status(f"\nError inserting data: {str(e)}")
            logging.error(f"Error inserting data into {table_name} in PostgreSQL: {str(e)}")
            self.connection.rollback()
            return 0
            
        finally:
            if cursor:
                cursor.close()


class DBMirror:
    def __init__(
        self, 
        progress_jar: str, 
        progress_host: str, 
        progress_port: int, 
        progress_db: str, 
        progress_user: str, 
        progress_pass: str,
        progress_schema: str,
        postgres_conn_string: str,
        batch_size: int = 1000
    ):
        self.progress = ProgressConnector(
            progress_jar,
            progress_host,
            progress_port,
            progress_db,
            progress_user,
            progress_pass,
            progress_schema
        )
        
        self.postgres = PostgresConnector(postgres_conn_string)
        self.batch_size = batch_size
        
    def mirror_table(self, schema: str, table_name: str, pg_table_name: str = None, truncate: bool = True) -> bool:
        if pg_table_name is None:
            pg_table_name = table_name
            
        print_status(f"\n=== Mirroring table: {schema}.{table_name} to {pg_table_name} ===")
        
        try:
            if f"{schema}.{table_name}" in read_ignore_file():
                print_status(f"Table {schema}.{table_name} is in the ignore file. Skipping.")
                return False
            columns = self.progress.get_columns(schema, table_name)
            
            if not columns:
                print_status(f"No columns found for table {schema}.{table_name}")
                add_to_ignore_file(f"{schema}.{table_name}")
                return False
                
            column_names = [col["name"] for col in columns]
            if not self.postgres.create_table(pg_table_name, columns):
                add_to_ignore_file(f"{schema}.{table_name}")
                return False
            oe_row_count = self.progress.get_row_count(schema, table_name)
            if f"{schema}.{table_name}" in read_ignore_file():
                return False
            if oe_row_count == 0:
                print_status(f"Table {schema}.{table_name} has 0 rows. Table created in PostgreSQL but no data to transfer.")
                return True
            pg_row_count = self.postgres.get_row_count(pg_table_name)
            if oe_row_count == pg_row_count and pg_row_count > 0:
                print_status(f"Table {schema}.{table_name} already has the same number of rows in PostgreSQL ({oe_row_count:,}). Skipping.")
                return True
            if truncate and not self.postgres.truncate_table(pg_table_name):
                add_to_ignore_file(f"{schema}.{table_name}")
                return False
            total_rows = self.progress.fetch_and_process_data(
                schema, table_name, column_names, 
                self.postgres, pg_table_name, self.batch_size
            )
                
            print_status(f"Table '{schema}.{table_name}' mirroring complete - {total_rows:,} total rows transferred")
            return True
            
        except Exception as e:
            print_status(f"Error mirroring table {schema}.{table_name}: {str(e)}")
            logging.error(f"Error mirroring table {schema}.{table_name}: {str(e)}")
            add_to_ignore_file(f"{schema}.{table_name}")
            return False
            
    def mirror_all_tables(self, exclude_tables: List[str] = None) -> Dict[str, bool]:
        if exclude_tables is None:
            exclude_tables = []
        ignored_tables = read_ignore_file()
        for table in ignored_tables:
            if table not in exclude_tables:
                exclude_tables.append(table)
                
        print_status(f"Tables in ignore file: {len(ignored_tables)}")
            
        print_status("\n=== Starting database mirroring process ===\n")
        
        if not self.progress.connect():
            print_status("Failed to connect to Progress database")
            return {}
            
        if not self.postgres.connect():
            print_status("Failed to connect to PostgreSQL database")
            self.progress.disconnect()
            return {}
            
        try:
            tables = self.progress.get_tables()
            results = {}
            
            print_status(f"\nTotal tables to process: {len(tables)}")
            
            if exclude_tables:
                print_status(f"Tables to exclude: {len(exclude_tables)}")
                
            included_tables = []
            for t in tables:
                full_name = f"{t['schema']}.{t['name']}"
                if t["name"] not in exclude_tables and full_name not in exclude_tables:
                    included_tables.append(t)
                
            print_status(f"Tables to mirror: {len(included_tables)}")
            
            for i, table_info in enumerate(included_tables, 1):
                schema = table_info["schema"]
                table_name = table_info["name"]
                
                print_status(f"\nProcessing table {i}/{len(included_tables)}: {schema}.{table_name}")
                
                success = self.mirror_table(schema, table_name)
                results[f"{schema}.{table_name}"] = success
                
                if success:
                    print_status(f"  ✓ Table '{schema}.{table_name}' mirrored successfully")
                else:
                    print_status(f"  ✗ Failed to mirror table '{schema}.{table_name}'")
                if i < len(included_tables):
                    print_status(f"Waiting before processing next table...")
                    time.sleep(1)
                
            return results
            
        except Exception as e:
            print_status(f"Error during mirroring: {str(e)}")
            logging.error(f"Error during mirroring: {str(e)}")
            return {}
            
        finally:
            self.progress.disconnect()
            self.postgres.disconnect()
            
            if jpype.isJVMStarted():
                jpype.shutdownJVM()
                print_status("JVM shut down")


def main():
    print_status("\n=== Progress to PostgreSQL Database Mirror Tool ===\n")
    print_status(f"Configuration:")
    print_status(f"  - Progress DB: {PROGRESS_HOST}:{PROGRESS_PORT}/{PROGRESS_DB}")
    print_status(f"  - Progress Schema: {PROGRESS_SCHEMA}")
    print_status(f"  - OpenEdge JAR: {JAR_FILE}")
    print_status(f"  - Batch Size: {BATCH_SIZE}")
    print_status(f"  - Max Cursors: {MAX_CURSORS}")
    print_status(f"  - Log File: {LOG_FILE}")
    print_status(f"  - Ignore File: {IGNORE_FILE}")
    
    mirror = DBMirror(
        JAR_FILE,
        PROGRESS_HOST,
        PROGRESS_PORT,
        PROGRESS_DB,
        PROGRESS_USER,
        PROGRESS_PASS,
        PROGRESS_SCHEMA,
        PG_CONN_STRING,
        BATCH_SIZE
    )
    
    start_time = time.time()
    results = mirror.mirror_all_tables(EXCLUDE_TABLES)
    end_time = time.time()
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    print_status("\n=== Migration Summary ===")
    print_status(f"Tables processed: {total_count}")
    print_status(f"Successfully mirrored: {success_count}")
    print_status(f"Failed tables: {total_count - success_count}")
    
    if total_count - success_count > 0:
        failed_tables = [table for table, success in results.items() if not success]
        print_status(f"Failed tables: {', '.join(failed_tables)}")
    
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    print_status(f"Total time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print_status(f"Detailed log available in: {LOG_FILE}")
    print_status(f"Ignored tables list: {IGNORE_FILE}")


if __name__ == "__main__":
    main()
