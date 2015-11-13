FROM ubuntu:12.04.5
MAINTAINER EPAM Systems

# Install Python and Basic Python Tools
RUN apt-get -qq -yy update
RUN apt-get install -y python-software-properties python-dev python-distribute python-pip
RUN apt-get install -y curl

ADD http://archive.cloudera.com/cdh5/one-click-install/precise/amd64/cdh5-repository_1.0_all.deb cdh5-repository_1.0_all.deb
RUN dpkg -i cdh5-repository_1.0_all.deb

RUN apt-get -qq -yy update

# java
RUN mkdir -p /usr/java/default && \
    curl -Ls 'http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.tar.gz' -H 'Cookie: oraclelicense=accept-securebackup-cookie' | \
    tar --strip-components=1 -xz -C /usr/java/default/

ENV JAVA_HOME /usr/java/default/
ENV PATH $PATH:$JAVA_HOME/bin



# Install hadoop client
RUN apt-get install -y hadoop-client

# install merlin
# Copy the application folder inside the container
COPY  /merlin-0.1.tar.gz  /merlin-0.1.tar.gz
RUN pip install /merlin-0.1.tar.gz


# Install Hive
RUN apt-get install -y hive

# Install Pig
RUN apt-get install -y pig

#Install Sqoop
RUN apt-get install -y sqoop

# Install HBase
RUN apt-get install -y hbase

#Install ZooKeeper
RUN apt-get install -y zookeeper

#Install Flume
RUN apt-get install flume-ng

# Install Spark
RUN apt-get install -y spark-core spark-master spark-worker spark-python

RUN mkdir -p /usr/lib/kafka && \
    curl -L --silent http://www.us.apache.org/dist/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz | tar -xz --strip=1 -C /usr/lib/kafka

ENV PATH $PATH:/usr/lib/kafka/bin

ADD /mysql-connector-java-5.1.34-bin.jar /var/lib/sqoop/
RUN apt-get install -y mysql-client

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


