package com.epam.yarn;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class ApplicationMaster {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);
    private YarnConfiguration conf;
    private final int numOfContainers = 1;

    public ApplicationMaster(String[] args) throws IOException {
        this.conf = ConfigurationFactory.yarnConfiguration();
    }


    public void run() throws YarnException, IOException, InterruptedException {
        LOG.info("Launching Application Master");
        LOG.info(" Initialize clients to ResourceManager and NodeManagers");
        AMRMClient<ContainerRequest> amRMClient = AMRMClient.createAMRMClient();
        amRMClient.init(conf);
        amRMClient.start();

        LOG.info("Register with ResourceManager");
        amRMClient.registerApplicationMaster("", 0, "");

        LOG.info("Set up resource type requirements for Container");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        LOG.info("Make container requests to ResourceManager");
        for (int i = 0; i < numOfContainers; ++i) {
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            amRMClient.addContainerRequest(containerAsk);
        }


        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Setup CLASSPATH for Container
        Map<String, String> containerEnv = new HashMap<String, String>();
        containerEnv.put("CLASSPATH", "./*");

        // Obtain allocated containers and launch
        LOG.info("Allocating containers...");
        int allocatedContainers = 0;
        int completedContainers = 0;
        Map<String, LocalResource> map = new HashMap<String, LocalResource>();
        map.put("script.py", loadScriptToExecute());
        map.putAll(loadResources());
        while (allocatedContainers < numOfContainers) {
            AllocateResponse response = amRMClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                allocatedContainers++;

                ContainerLaunchContext appContainer = createContainerLaunchContext(
                        map,
                        containerEnv
                );
                LOG.info("Launching container " + allocatedContainers);

                nmClient.startContainer(container, appContainer);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
            }
            Thread.sleep(100);
        }

        // Now wait for the remaining containers to complete
        while (completedContainers < numOfContainers) {
            AllocateResponse response = amRMClient.allocate(completedContainers / numOfContainers);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
                LOG.info("ContainerID:" + status.getContainerId() + ", state:" + status.getState().name());
            }
            Thread.sleep(100);
        }

        LOG.info("Completed containers:" + completedContainers);

        // Un-register with ResourceManager
        amRMClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
        LOG.info("Finished Application Master");
    }


    /**
     * Launch container by create ContainerLaunchContext
     *
     * @return
     */
    private ContainerLaunchContext createContainerLaunchContext(Map<String, LocalResource> map,
                                                                Map<String, String> containerEnv) {

        ContainerLaunchContext appContainer = Records.newRecord(ContainerLaunchContext.class);
        appContainer.setLocalResources(map);
        appContainer.setEnvironment(containerEnv);
        appContainer.setCommands(
                Collections.singletonList(
//                        "$JAVA_HOME/bin/java" +
                        "/usr/bin/env python" +
//                                " -Xmx256M" +
//                                " com.epam.yarn.AppContainer" +
                                " " + "script.py" +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/container.stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/container.stderr"
                )
        );

        for (String cmd : appContainer.getCommands()) {
            LOG.info("container command {}", cmd);
        }


        return appContainer;
    }

//    todo          remove : container can fetch script location from env EnvConstants.SCRIPT_FILE ??
    private LocalResource loadScriptToExecute() throws IOException {
        LocalResource scriptResource = Records.newRecord(LocalResource.class);
        Map<String, String> envs = System.getenv();
        String path = envs.get("SCRIPT.FILE");
        if (!path.isEmpty()) {
            scriptResource.setType(LocalResourceType.FILE);
            Path scriptPath = FileSystem.get(conf).makeQualified(new Path(path));
            scriptResource.setResource(ConverterUtils.getYarnUrlFromPath(scriptPath));
            scriptResource.setTimestamp(Long.parseLong(envs.get("SCRIPT.FILE.MODIFICATION.TIME")));
            scriptResource.setSize(Long.parseLong(envs.get("SCRIPT.FILE.LENGTH")));
            scriptResource.setVisibility(LocalResourceVisibility.PUBLIC);
        }
        return scriptResource;

    }

    private Map<String, LocalResource> loadResources() throws IOException {
        LocalResource scriptResource = Records.newRecord(LocalResource.class);
        Map<String, String> envs = System.getenv();
        Map<String, LocalResource> localResource = new HashMap<String, LocalResource>();
        String fileName;
        for (Map.Entry<String, String> entry : envs.entrySet())
        {
            if(entry.getKey().contains("*resource*")) {
                fileName = entry.getKey().substring(10);
                scriptResource.setType(LocalResourceType.FILE);
                Path scriptPath = FileSystem.get(conf).makeQualified(new Path(entry.getValue()));
                scriptResource.setResource(ConverterUtils.getYarnUrlFromPath(scriptPath));
                scriptResource.setVisibility(LocalResourceVisibility.PUBLIC);
                localResource.put(entry.getKey(), scriptResource);
                for (Map.Entry<String, String> e : envs.entrySet()){
                    if(e.getKey().equals(fileName + "_time")) {
                        scriptResource.setTimestamp(Long.parseLong(envs.get(e.getKey())));
                    } else if (e.getKey().equals(fileName + "_len")){
                        scriptResource.setSize(Long.parseLong(envs.get(e.getKey())));
                    }
                }
                localResource.put(fileName, scriptResource);
            }
        }
        return localResource;

    }

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        LOG.info("Starting ApplicationMaster...");
        ApplicationMaster appMaster = new ApplicationMaster(args);
        appMaster.run();
    }
}