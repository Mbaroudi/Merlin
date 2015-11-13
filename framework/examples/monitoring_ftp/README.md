Flow gets metadata of files on FTP server and on HDFS.
Compares them and get only new files on FTP that don't exist on HDFS.
Download new files to HDFS with partition.

For run main 'flow.py':
    1. Copies folder 'resources' with all files to some directory.
    2. Copies 'flow.py', 'clean_resources.py' and 'setup.py' to this directory.
    4. Runs 'setup.py' (runs command 'python setup.py' in it directory).
    5. Runs 'flow.py' (runs command 'python flow.py' in it directory).
    6. Runs 'cleanup.py' (runs command 'python cleanup.py' in it directory).

Flow steps:
    1. Load FileDescriptor for files on FTP.
    2. Load FileDescriptor for files on HDFS.
    3. Compare and get FileDescriptor only for new files on FTP.
    4. Load new files to Local from FTP.
    5. Load new files to HDFS from Local with partition.
    6. Hive's job add new partition.