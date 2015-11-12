Install Docker on your OS.

Create directory 'merlin'

Create directories in 'merlin':
    - hadoop
    - hive
    - kafka
    - sqoop

Download files 'core-site.xml', 'hdfs-site.xml', 'mapred-site.xml' and 'yarn-site.xml'
    from custom cluster at the path '/etc/hadoop/conf' to 'hadoop' directory

Download file 'sqoop-site.xml'
    from custom cluster at the path '/etc/sqoop/conf' to 'sqoop' directory

Download file 'hive-site.xml'
    from custom cluster at the path '/etc/hive/conf' to 'hive' directory

Download all files from custom cluster at the path '/etc/kafka/conf' to 'kafka' directory

Add 'merlin-0.1.tar.gz' to 'merlin'

Add Dockerfile from 'hortonworks' or 'cloudera' directory to 'merlin'

Add mysql-connector-java-5.1.34-bin.jar to 'merlin' for Sqoop

Run next command in the current directory

    docker build --tag merlin --rm=true -f Dockerfile .

    docker run -it --add-host {alias of host in the config files}:{namenode's host} \
    -v /path/to/your/worklow/:/home/merlin merlin

