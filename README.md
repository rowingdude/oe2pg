After tinkering with this a bit more, I'm going to release it as a mostly finished product stamped pre-release v0.90.

It works as expected, via JVM, it connects to the OE database and queries the stipulated schema's metadata.

While running, it looks like this:

    Processing table 44/999: SCHEMA.table_name
    
		=== Mirroring table: SCHEMA.table_name to table_name ===
		Fetching columns for table 'SCHEMA.table_name'... Found 13 columns
		Creating table 'table_name' in PostgreSQL... Done
		Counting rows in 'SCHEMA.table_name'... Found 132,111 rows
		Truncating table 'table_name' in PostgreSQL... Done
		Counting rows in 'SCHEMA.table_name'... Found 132,111 rows
		Mirroring 'SCHEMA.table_name' (132,111 rows, ~133 batches)
		Processing table_name: Batch 114/133 (86.3%, 114,000/132,111 rows)
		^C
		Error during mirroring: Java Virtual Machine is not running
		Disconnecting from Progress DB... Failed!
		Disconnect error: Java Virtual Machine is not running
		Disconnecting from PostgreSQL... Done


Hopefully someone else is actively working to escape OE hell and come to the land of promise!
