Flow imports data from mysql to HDFS, process it and upload processed data back to mysql.

For run main 'flow.py':
    1. Copies folder 'resources' with all files to some directory.
    2. Copies 'flow.py', 'clean_resources.py' and 'setup.py' to this directory
    3. Runs 'script.sql' (runs command 'SOURCE script.sql' on mysql in the 'resources' folder)
    4. Runs 'setup.py' (runs command 'python setup.py' in it directory)
    5. Runs 'flow.py' (runs command 'python flow.py' in it directory)
    6. Runs 'cleanup.py' (runs command 'python cleanup.py' in it directory)

Flow steps:
    1. Imports data from mysql's table 'test_example.first_table_name'(id,name,count)
     to HDFS's folder '/tmp/data_from_import' in "'id','name','count'" format.
    2. Counts words:
        - Reads files from folder '/tmp/data_from_import';
        - Splits stroke on mapper and gives to reducers in format (new Text('name'), new IntWritable('count'));
        - Summarizes on reducer and return in format (new Text('name'), new IntWritable('count'));
        - Writes files to folder '/tmp/data_to_export'.
    3. Exports data to mysql's table 'test_example.second_table_name'(id,name,count)
     from HDFS's folder '/tmp/data_to_export'.


