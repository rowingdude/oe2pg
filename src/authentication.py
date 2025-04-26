import os
from typing import Dict, Any

class Config:
    """Class with hardcoded configuration settings."""
    
    def __init__(self):
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.progress_db =   {
            "host": "openedge-server",
            "port": 2030,
            "db_name": "database",
            "user": "user",
            "password": "password",
            "schema": "PUB",
            "jar_file": "/path/to/openedge.jar",
            "driver_class": "com.ddtek.jdbc.openedge.OpenEdgeDriver"
            },
            "postgres_db": {
            "conn_string": "host=postgres-server port=5432 dbname=database user=postgres password=postgres",
            "timeout": 30
            },
            "mirror_settings": {
            "batch_size": 5000,
            "ignore_file": "ignored_tables.txt",
            "log_file": "sync.log"
            }
        }
    
    def get_progress_config(self) -> Dict[str, Any]:
        return self.progress_db
    
    def get_postgres_config(self) -> Dict[str, Any]:
        return self.postgres_db
    
    def get_mirror_config(self) -> Dict[str, Any]:
        return self.mirror_settings
