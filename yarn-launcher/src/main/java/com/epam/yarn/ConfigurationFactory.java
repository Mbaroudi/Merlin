package com.epam.yarn;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public final class ConfigurationFactory {
    private ConfigurationFactory() {
    }

    public static YarnConfiguration yarnConfiguration(){
        YarnConfiguration config = new YarnConfiguration();
        String HADOOP_MN = "vm-cluster-node1";
//        String HADOOP_MN = "master1";
        config.set("fs.defaultFS", "hdfs://" + HADOOP_MN + ":8020");
        config.set("yarn.resourcemanager.address", HADOOP_MN + ":8032");
        config.set("yarn.resourcemanager.scheduler.address",  HADOOP_MN + ":8030");
        config.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        config.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        return config;
    }
}
