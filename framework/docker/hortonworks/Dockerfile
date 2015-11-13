FROM ubuntu:12.04.5
MAINTAINER EPAM Systems

# Install Python and Basic Python Tools
RUN apt-get -qq -yy update
RUN apt-get install -y python-software-properties python-dev python-distribute python-pip
RUN apt-get install -y curl
RUN apt-get install -y wget

RUN wget http://public-repo-1.hortonworks.com/HDP/ubuntu12/2.x/updates/2.2.6.0/hdp.list -O /etc/apt/sources.list.d/hdp.list

RUN apt-get -qq -yy update

# java
RUN mkdir -p /usr/java/default && \
    curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
    tar --strip-components=1 -xz -C /usr/java/default/

ENV JAVA_HOME /usr/java/default/
ENV PATH $PATH:$JAVA_HOME/bin



# Install hadoop client
RUN apt-get install -y --force-yes hadoop-client

# install merlin
# Copy the application folder inside the container
COPY  /merlin-0.1.tar.gz  /merlin-0.1.tar.gz
RUN pip install /merlin-0.1.tar.gz


# Install Hive
RUN apt-get install -y --force-yes hive

# Install Pig
RUN apt-get install -y --force-yes pig

#Install Sqoop
RUN apt-get install -y --force-yes sqoop

# Install HBase
RUN apt-get install -y --force-yes hbase

#Install ZooKeeper
RUN apt-get install -y --force-yes zookeeper

#Install Flume
RUN apt-get install -y --force-yes flume flume-agent

# Install Spark
RUN apt-get install -y --force-yes spark-python

RUN apt-get install -y --force-yes kafka

ADD /mysql-connector-java-5.1.34-bin.jar /var/lib/sqoop/
RUN apt-get install -y --force-yes mysql-client

WORKDIR /home/merlin

RUN unlink /etc/hadoop/conf/yarn-site.xml
RUN unlink /etc/hadoop/conf/core-site.xml
RUN unlink /etc/hadoop/conf/mapred-site.xml
RUN unlink /etc/hadoop/conf/hdfs-site.xml
ADD /hadoop/yarn-site.xml /etc/hadoop/conf/yarn-site.xml
ADD /hadoop/mapred-site.xml /etc/hadoop/conf/mapred-site.xml
ADD /hadoop/core-site.xml /etc/hadoop/conf/core-site.xml
ADD /hadoop/hdfs-site.xml /etc/hadoop/conf/hdfs-site.xml

RUN unlink /etc/hive/conf/hive-site.xml
ADD /hive/hive-site.xml /etc/hive/conf/hive-site.xml

RUN unlink /etc/sqoop/conf/sqoop-site.xml
ADD /sqoop/sqoop-site.xml /etc/sqoop/conf/sqoop-site.xml

ENV PATH $PATH:/usr/hdp/2.2.6.0-2800/kafka/bin/

ADD /mysql-connector-java-5.1.34-bin.jar /usr/hdp/2.2.6.0-2800/sqoop/lib/

RUN apt-get install -y --force-yes tez

ADD /tez/tez-site.xml /etc/tez/conf/tez-site.xml