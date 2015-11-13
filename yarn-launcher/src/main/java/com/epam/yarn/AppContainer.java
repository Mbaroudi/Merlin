package com.epam.yarn;

import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppContainer {
    private static final Logger LOG = LoggerFactory.getLogger(AppContainer.class);
    static{
        LOG.info("{} : Loading container... ", NetUtils.getHostname());
    }

    public AppContainer(String[] args) {

    }

    public static void main(String[] args){
        LOG.info("Container just started on {}", NetUtils.getHostname());
        AppContainer container = new AppContainer(args);
        container.run();
        LOG.info("Container is ending...");
    }


    private void run()  {
        for (int i = 0; i < 10000; i++) {
            LOG.info("Running Container on {}", NetUtils.getHostname());
        }
    }
}
