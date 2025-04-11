# Openedge to PostgreSQL Mirroring Tool

## Summary

This script performs table mirroring with incremental update reconciliation. The goal was to mirror a database and then track changes to it. 
Because our old database does not have the best indexing or keys set up, this script attempts three modes to reconcile its progress, in the
first round, it looks for a primary key it can reference, in the second round, it looks for time (timedate) stamps, if neither are found, it 
does a hash of the data and compares rows via hash (THIS IS VERY SLOW). The program informs the user of which mode it's taken.

Once running, users will notice two ouputs, if the data is up to date, the output will be as in #1, whereas if there are updates, #2 is shown.

###  1: No Updates (All data synced)

        === Mirroring table: SCHEMA.original_table to original_table ===
        Fetching columns for table 'SCHEMA.original_table0'... Found 1 columns
        Fetching 235 new/changed rows from 'SCHEMA.original_table1'
        Primary key for SCHEMA.original_table0: orig_pk
        Counting rows in 'SCHEMA.original_table'... Found 7,093 rows
        Counting rows in PostgreSQL table 'original_table0'... Found 7,093 rows
        Comparing data in SCHEMA.original_table0...
        Table SCHEMA.original_table is up to date. Skipping.
        ✓ Table 'SCHEMA.original_table0' mirrored successfully

###  2: New data (incremental update)

        === Mirroring table: SCHEMA.table_a to table_a ===
        Fetching columns for table 'SCHEMA.table_a'... Found 5 columns
        Primary key for SCHEMA.table_a: table_a_pk
        Counting rows in 'SCHEMA.table_a'... Found 8,586 rows
        Counting rows in PostgreSQL table 'table_a'... Found 8,586 rows
        Comparing data in SCHEMA.table_a...
        Performing incremental update for SCHEMA.table_b...
        Table SCHEMA.table_a is up to date. Skipping.
        ✓ Table 'SCHEMA.table_a' mirrored successfully

This program is multithreaded, and its mode of operation is:
#### 1. Identify Schema
#### 2. Count rows
#### 3. Sort tables from fewest to most rows
#### 4. Sync

The workers will go out and sync one table each until the number of tables is less than the number of workers, after that, the table with
the fewest rows remaining is prioritized. The reason for this is to minimize database load. In testing, this has been quite effective. We
also moderate the database load by capping the amount of open cursors, and enforcing strict transaction management. Our old database was 
poorly formed, so it's vulnerable to being overwhelmed even by PowerBI. 

After the entire database is mirrored, this program is designed to be run via cron, but obviously deployment is your sole discretion.

If you have any questions, comments, or critiques, please reach out! Cheers
