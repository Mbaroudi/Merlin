<<<<<<< HEAD
# Purpose
Merlin is a Python framework to simplify Hadoop jobs workflow development and management. Framework is integrated with the BigData technology stack and supports Hadoop jobs for Apache MapReduce, Streaming map-reduce, Apache Pig, Apache Hive, Apache Sqoop, Spark and offers commonly used functionality for development of Extract-Transform-Load (ETL) processes.

# Features

* Configure and launch applications that use Java MapReduce, Streaming, Hive, Pig, Spark, Flume, Kafka.
* Configure and run Sqoop jobs to transfer bulk data between Apache Hadoop and structured datastores
* Script HDFS/Local File System/ FTP operations

# Installation instructions

Requires Python 2.7 or later
If you have Python 2.6 or lower you can download Python 2.7 and run all commands using
'python2.7' instead of 'python'
Also you can install Merlin in virtualenv to avoid misconceptions with python's version

        virtualenv ve -p python2.7

        source ve/bin/activate

CentOS 6 still come with Python 2.6 and several critical system utilities, for example yum, will break if the default Python interpreter is upgraded.
The trick is to install new versions of Python in /usr/local.
Below a link to installation Python2.7 on CentOS6

https://git.epam.com/epmc-bdcc/hadoop-framework/wikis/PythonOnCentos6


You can install Merlin by downloading the source and using the setup.py script as follows:


        git clone https://git.epam.com/epmc-bdcc/hadoop-framework.git

        cd hadoop-framework

        python setup.py install

This setup call installs all of the necessary python dependencies.


# Running tests
Merlin comes with a comprehensive suite for unit and integration tests. All check-ins are automatically tested on [CI Server](http://evhubudsd1b86:8111/overview.html)

To run through the whole suite for tests, run

        python setup.py nosetests

Separate test cases can be run with the next command :

        python setup.py nosetests --tests <package>

## Integration tests
For integration testing, the Merlin expects a Hadoop installation available on a localhost.

# Generate Documentation

    python setup.py build_sphinx

# Quickstart
Here's some short descriptions and code examples to give you a sense of what you're getting into.

Most Hadoop tools provide some CLI utilities which can be used to configure and launch specific Hadoop job from command line.

Merlin is nothing more then wrapper for these CLI utilities.

Besides it also includes some nice features though:

* Convenient and simple API for configuring and launching Hadoop jobs from the code.
* Launch pre-configured jobs: Job configurations can be loaded from metastore. Metastore can be config file that contains settings for more than one job. Job name is used as name of the config section containing job-specific settings. Also metasatore can be properties from Hive's table.
* Monitor job progress.
* Support for HDFS / FTP / Local File System operations including creating, copying, and deleting files and directories, examining filesystem information about files and directories, etc.
* Logging : HF relies on standard python logging module to write down log messages. HF provides default logging configuration. In order to override default configurations pass <code>--log-conf=&lt;path_to_logging_config_file&gt;</code> on the command line

## Examples:

A more extensive examples can be found in unit / integration tests, tool module documentation, and [examples sub-module](https://git.epam.com/epmc-bdcc/hadoop-framework/tree/develop/framework/examples).

### MapReduce

#### MapReduce Job configured via HF API
    MapReduce.prepare_mapreduce_job(
         jar="hadoop-mapreduce-examples.jar",
         main_class="wordcount",
         name=_job_name
     ).with_config_option("split.by", "'\\t'") \
         .with_number_of_reducers(3) \
         .with_arguments("/user/vagrant/dmode.txt", "/tmp/test") \
         .run()

Will be transformed to next MapReduce CLI command :

    hadoop jar hadoop-mapreduce-examples.jar wordcount -D mapreduce.job.name=%s -D split.by='\\t' -D mapreduce.job.reduces=3 /user/vagrant/dmode.txt /tmp/test

#### Pre-Configured MapReduce Job

    MapReduce.prepare_mapreduce_job(
                jar='mr.jar',
                main_class="test.mr.Driver",
                config=Configuration.load(metastore=IniFileMetaStore(file=path_to_config_file)),
                name='simple_mr_job'
            ).run()

Job Configurations

    [simple_mr_job]
    job.config = value.delimiter.char=,
        partition.to.process=20142010
    args=/input/dir
        /output/dir

Will be transformed to next MapReduce CLI command :

    hadoop jar mr.jar test.mr.Driver -D mapreduce.job.name=simple_mr_job -D value.delimiter.char=, \
     -D partition.to.process=20142010 /input/dir /output/dir


#### Streaming MapReduce Job

#### Streaming MapReduce Job configured via HF API

    MapReduce.prepare_streaming_job(
        name='job_name'
    ).take(
        'data'
    ).process_with(
        mapper='mapper.py',
        reducer='reducer.py'
    ).save(
        'output.txt'
    ).with_config_option(
        key='value.delimiter.char',
        value=','
    ).with_config_option(
        key='partition.to.process',
        value='20142010'
    ).run()

Will be transformed to next MapReduce CLI command :

    hadoop jar hadoop-streaming.jar -D mapreduce.job.name=job_name \
    -D value.delimiter.char=, -D partition.to.process=20142010 \
    -mapper mapper.py -reducer reducer.py -input data -output output.txt

#### Pre-Configured Streaming MapReduce Job

    MapReduce.prepare_streaming_job(
            config=Configuration.load(metastore=IniFileMetaStore(file=path_to_config_file)),
            name='streaming_test_job_with_multiple_inputs'
        ).run()

Job Configurations

    [streaming_test_job_with_multiple_inputs]
    mapper= smapper.py
    reducer=sreducer.py
    input=/raw/20102014
        /raw/21102014
        /raw/22102014
    output=/core/20102014
    mapreduce.job.reduces=100
    inputformat='org.mr.CustomInputFormat'
    outputformat='org.mr.CustomOutputFormat'
    libjars=mr_001.jar
        mr_002.jar
    files=dim1.txt
    environment.vars=JAVA_HOME=/java
        tmp.dir=/tmp/streaming_test_job_with_multiple_inputs


Will be transformed to next MapReduce CLI command :

    hadoop jar hadoop-streaming.jar -D mapreduce.job.name=streaming_test_job_with_multiple_inputs -files dim1.txt \
    -libjars mr_001.jar,mr_002.jar -mapper smapper.py -reducer sreducer.py -numReduceTasks 100 -input /raw/20102014 \
    -input /raw/21102014 -input /raw/22102014 -output /core/20102014 -inputformat org.mr.CustomInputFormat \
    -outputformat org.mr.CustomOutputFormat -cmdenv JAVA_HOME=/java -cmdenv tmp.dir=/tmp/streaming_test_job_with_multiple_inputs



### Pig

#### run pig commands from file
    Pig.load_commands_from_file("/tmp/file").with_parameter("partition", "'010120014'").run()

Will be transformed to next Pig CLI command :

    pig -param partition='010120014' -f "/tmp/file"

#### run pig commands from string

    Pig.load_commands_from_string("A = LOAD 'data.csv' USING PigStorage()
            AS (name:chararray, age:int, gpa:float);
            B = FOREACH A GENERATE name;
            DUMP B;").run()

Will be transformed to next Pig CLI command :

    pig -e "A = LOAD 'data.csv' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;"

#### run pre-configured pig job

    Pig.load_preconfigured_job(
            job_name='pig test',
            config=Configuration.load(metastore=IniFileMetaStore(file=path_to_config_file))
        ).without_split_filter().run()

In order to run Pig job described bellow configuration file (job.ini) should contain 'pig_test' section

    [pig test]
    brief=enabled
    SplitFilter=disable
    ColumnMapKeyPrune=disable
    file=data_processing.pig

Next Pig CLI command will be generated:

    pig -brief -optimizer_off SplitFilter -optimizer_off ColumnMapKeyPrune -f data_processing.pig

### Hive

#### Run Hive query from file
    Hive.load_queries_from_file("/tmp/file").run()

Will be transformed to next Hive CLI command :

    hive -f /tmp/file

#### Run Hive query from given string :

        Hive.load_queries_from_string('CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);').run()

Will be transformed to next Hive CLI command :

    hive -e "CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);"

#### Run pre-configured Hive job
    hive = Hive.load_preconfigured_job(
        name='hive test',
        config=Configuration.load(metastore=IniFileMetaStore(file=path_to_config_file))
    ).with_hive_conf("hello", "world").run()

 Configuration file (in our case hive.ini) should contain 'hive_test' section with job specific parameters. For example:

    [hive test]
    hive.file=test.hql
    define=A=B
        C=D
    database=hive
    hivevar=A=B
        C=D

 Next Hive CLI command should be generated:

    hive -f test.hql --define A=B --define C=D --hiveconf hello=world --hivevar A=B --hivevar C=D --database hive

### Sqoop

#### Sqoop Import

A basic import of a table named EMPLOYEES in the corp database

        Sqoop.import_data().from_rdbms(
            rdbms="mysql",
            host="db.foo.com",
            database="corp",
            username="mysql_user",
            password_file=".mysql.pwd"
        ).table("EMPLOYEES").to_hdfs(target_dir="/data/EMPLOYEES").run()

Will be transformed to next Sqoop CLI command :

        sqoop-import \
        --connect jdbc:mysql://db.foo.com/corp \
        --username mysql_user \
        --password-file .mysql.pwd \
        --table EMPLOYEES \
        --target-dir /data/EMPLOYEES \
        --as-textfile

#### Sqoop Export
    Sqoop.export_data().to_rdbms(
        rdbms="mysql",
        username="root",
        password_file="/user/cloudera/password",
        host="localhost",
        database="sqoop_tests"
    ).table(table="table_name").from_hdfs(
        export_dir="/core/data"
    ).run()

Will be transformed to next Sqoop CLI command :

    sqoop export --connect jdbc:mysql://localhost/sqoop_tests \
    --username root \
    --password-file /user/cloudera/password \
    --export-dir /core/data \
    --table table_name



### Spark

#### Runs Spark job using yarn client mode

    SparkApplication().master(SparkMaster.yarn_client()).application("application_jar", main_class="Main", app_name="Spark").run()

Will be transformed to next Spark CLI command :

    spark-submit --master yarn-client --class Main --name Spark application_jar


#### Run pre-configured Spark job
    SparkApplication.load_preconfigured_job(
        name="spark_app",
        config=Configuration.load(metastore=IniFileMetaStore(file=path_to_config_file))
    ).run()

In order to run Spar job configuration file 'config_file.ini' should include next section 'spark_app'. For example:

        [spark_app]
        application.jar=test.jar
        master=local[10]
        class=test.SparkApp
        name=test_app
        jars=lib001.jar
            lib002.jar
        files=dim001.cache.txt
            dim002.cache.txt
        properties-file=spark.app.configs
        conf=spark.app.name=test_app
            spark.executor.memory=512m

Next Spark Application submit command will be generated:

    spark-submit --master local[10] --class test.SparkApp --name test_app \
    --jars lib001.jar,lib002.jar --files dim001.cache.txt,dim002.cache.txt \
    --properties-file spark.app.configs --conf "spark.app.name=test_app spark.executor.memory=512m" test.jar



### Flume

#### Runs Flume Agent

    Flume.agent(agent="a1", conf_file="path/to/file")\
            .with_jvm_D_option(name="jvm_option", value="test")\
            .load_configs_from_dir("path/to/dir")\
            .load_plugins_from_dirs(pathes=["path/to/plugins1,path/to/plugins2", "path/to/plugins3"])\
            .run()

Will be transformed to next Flume CLI command :

    flume-ng agent --name a1 --conf-file path/to/file --conf path/to/dir \
    --plugins-path path/to/plugins1,path/to/plugins2,path/to/plugins3 -Xjvm_option=test



### Kafka

#### Starts/Stop Kafka Broker

    Kafka.start_broker(path_to_config="path/to/config")
    Kafka.stop_broker(path_to_config="path/to/config")

Will be transformed to next Kafka CLI command :

    kafka-server-start.sh path/to/config
    kafka-server-stop.sh path/to/config

#### Runs consumer

    Kafka.run_producer(name='namespace.Producer', args=["conf1", "conf2"])

Will be transformed to next Kafka CLI command :

    kafka-run-class.sh namespace.Producer conf1 conf2



###  File System Manipulations

Merlin provides methods to easily interact with the local file system, HDFS and FTP/FTPS/SFTP. These methods are described in the table below.

| `HDFS`               | `LocalFS`         | `FTP/SFTP/FTPS`              | `Description`                                                                                                 |
|----------------------|-------------------|------------------------------|---------------------------------------------------------------------------------------------------------------|
| exists               | exists            | exists                       | Tests whether a file exists.                                                                                  |
| is_directory         | is_directory      | is_directory                 | Tests whether a file is a directory.                                                                          |
| list_files           | list_files        | list_files                   | Returns list of the files for the given path                                                                  |
| recursive_list_files | -                 | -                            | Returns list of the files for the given path                                                                  |
| create               | create            | create                       | Creates new empty file or directory                                                                           |
| copy_to_local        | -                 | download_file / download_dir | Copies file  to the local file system                                                                         |
| copy                 | copy              | -                            | Copies file                                                                                                   |
| move                 | move              | -                            | Moves file                                                                                                    |
| size                 | size              | size                         | Calculates aggregate length of files contained in the directory or the length of a file in case regular file. |
| merge                | -                 | -                            | Merges the files at HDFS path to a single file on the local filesystem.                                       |
| delete               | delete            | delete                       | Deletes a file if it exists.                                                                                  |
| permissions          | permissions       |                              | Gets permission of the file                                                                                   |
| owner                | owner             |                              | Get file's owner                                                                                              |
| modification_time    | modification_time | modification_time            | Get file modification time                                                                                    |
| -                    | copy_to_hdfs      | -                            | Copies file from local filesystem to HDFS                                                                     |
| distcp               | -                 | -                            | Copies files between HDFS clusters                                                                            |
| -                    | -                 | upload                       | Uploads file to FTP server                                                                                    |


=======
# Merlin
Standardized Big Data ETL framework
>>>>>>> origin/master
