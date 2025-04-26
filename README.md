## OpenEdge to PostgreSQL Data Synchronization Tool

### Overview
This is a mechanism for mirroring data from Progress OpenEdge databases to PostgreSQL. I aim to supports both full table synchronization and incremental updates.

I've focused on a few features for the initial release, just to get it working:

* Automatically selects the most efficient synchronization approach (full, pk, or timestamp) for each table based on sizing and columns present
* Attempts to resolve  changes in source database structure to the target database
* Batched processing to optimize memory usage and network traffic
* User settings for batch sizes (see `authentication.py`) and table exclusions (via a plain text file called `ignored_tables.txt`)

These are the most pressing features I need to work on:

-  Incremental Updates
-  Whole job progress tracking 
-  Failure recovery - this runs along with "Incremental Updates", but we populate a `tracking` table with the goal of resuming on failure

### System Requirements

* Python 3.8 or higher (with `jaydeebeeapi` and  `psycopg2`)
* Access to OpenEdge database with JDBC connectivity
* PostgreSQL database with write permissions
* Sufficient network bandwidth between systems

#### Python Package Installations:

Most major distributions maintain released versions of these packages. As such, we can install them directly without `pip`:

* Ubuntu: `sudo apt update; sudo apt install -y python3-psycopg2 python3-jaydebeeapi`
* Fedora: `sudo dnf update; sudo dnf install -y python3-psycopg2 python3-jaydebeeapi`
* Arch:   `sudo pacman -Syu; sudo pacman -Sy python3-psycopg2 python3-jaydebeeapi`
* Gentoo: `emerge --sync --quiet; sudo emerge python3-psycopg2 python3-jaydebeeapi`

#### Note about PostgreSQL Installation

I do recommend users on major distributions (Ubuntu, Fedora) add the PostgreSQL repo to their package manager source list, you can find the directions here [PostgreSQL Downloads](https://www.postgresql.org/download/).

### Configuration
The tool expects the authentication configuration file with to contain these following sections:

``` json
  {
    "progress_db": {
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
```
### Usage

* First-time Synchronization
  - For initial data load, use the --full-sync flag: `python oe2pg.py --full-sync`
  - This creates all necessary tables and performs a complete mirror of all accessible tables.
    
* Incremental Updates
  - For routine updates after initial sync: `python oe2pg.py`
  - This (eventually) detects and transfers only new or modified records since the last synchronization.
    
* Selective Synchronization
  - To exclude specific tables: `python oe2pg.py --ignore-table TABLE1 --ignore-table TABLE2`

#### Operation Modes
The tool supports three synchronization methods:

1. Full Sync: Truncates destination tables and reloads all data 
2. PK Sync:  Uses primary keys to identify and transfer only new/changed records
   * Maybe adding `unique` keys too??
3. (WIP) Timestamp-Based 

### Monitoring

The tool logs detailed progress information to both console and the configured log file:

2025-04-26 10:34:17 - INFO - Synced 3000 rows for customer_data (total: 504000 of 1200000 (42.0%))

### Best Practices

* Schedule incremental syncs during periods of low database activity
* Allocate sufficient memory for the batch size configured
* Monitor disk space on PostgreSQL server, especially for large initial syncs
* Review logs periodically to identify slow-syncing tables
* Adjust batch sizes based on available memory and network latency

### Troubleshooting

* If synchronization fails for specific tables, they will be noted in the log file
* Permission errors may indicate insufficient database access rights
* Timeout errors suggest network connectivity issues or database load
* For large tables, consider increasing the batch size for better performance

### Limitations (Ideas for later!)

* Binary/BLOB data may require additional configuration, I do not have a lot of experience with these data as Open Edge stores them
* Table derivatives such as functions, joins, indexes, and views are not automatically synchronized
* Very large tables (>10M rows) may require additional tuning as in testing they just take a long long time
* The tool maintains a best-effort approach to data consistency through direct copy to TEXT, which should transfer raw values, irrespective of formatting or modifiers. Text is the safest import method at the expense of size-on-disk

# Security Considerations

* _Database credentials are stored in plaintext in the configuration file_
* Ensure the configuration file has appropriate permissions, on *Nix operating systems, I recommend `chmod 500 authentication.py` and `chown <db_etl_user>:<db_etl_group> config.json`
* Environment variables should be used over `authentication.py` in production


Thank you for checking out my project, if you have any ideas to improve it, I'm all ears!
