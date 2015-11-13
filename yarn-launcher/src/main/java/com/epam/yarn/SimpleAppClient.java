package com.epam.yarn;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 *
 */
public class SimpleAppClient {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleAppClient.class);
    private YarnConfiguration conf;
    private YarnClient yarnClient;
    private FileSystem fs;
    private final long clientStartTime = System.currentTimeMillis();

    @Option(name = "-timeout", required = false,
            usage = "time for executing app")
    private long clientTimeout = 10000000;

    @Option(name = "-resource", required = false,
            usage = "path to local resource")
    private String[] args = new String[0];

    @Option(name = "-execute", required = true,
            usage = "python script for executing")
    private String pyScript;

    public static void main(String[] args) throws IOException, YarnException, URISyntaxException {
        SimpleAppClient client = new SimpleAppClient(args);
        int status = client.run() ? 0 : 1;
        System.exit(status);
    }

    protected String getJarLocation() throws URISyntaxException {
        return SimpleAppClient.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
    }

    public SimpleAppClient(String[] args) throws IOException {
//      initialize yarn client
        conf = ConfigurationFactory.yarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        fs = FileSystem.get(conf);
        // parse args from CLI
        CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            LOG.error(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    public boolean run() throws YarnException, IOException, URISyntaxException {
        // start Yarn client
        yarnClient.start();
        // create app and gey app id
        YarnClientApplication application = yarnClient.createApplication();

        //The response from the YarnClientApplication for a new application also contains information about the cluster
        GetNewApplicationResponse appResponse = application.getNewApplicationResponse();
        logClusterInformation(appResponse);

        LOG.info("Configure the application submission context");
        ApplicationSubmissionContext appContext = application.getApplicationSubmissionContext();
        appContext.setApplicationName("YarnApp");
        ApplicationId appId = appContext.getApplicationId();
        LOG.info("Applicatoin ID = {}", appId);


        //set up max memory and vcores which can be allocated to application master
        configureAppMasterCapabilities(appContext,
                appResponse.getMaximumResourceCapability().getMemory(),
                appResponse.getMaximumResourceCapability().getVirtualCores()
        );

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        appContext.setPriority(pri);


        appContext.setAMContainerSpec(configureApplicationMasterContainer(appId.getId()));

        LOG.info("Submitting application to ASM");
        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    /**
     * Monitor the submitted application for completion.
     * Kill application if time expires.
     *
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     *
     * @throws java.io.IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {
            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. "
                            + " Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                            + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                return false;
            }

            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application"
                        + ". Breaking monitoring loop : ApplicationId:" + appId.getId());
                forceKillApplication(appId);
                return false;
            }
        }
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     *
     * @param appId Application Id to be killed.
     * @throws org.apache.hadoop.yarn.exceptions.YarnException
     *
     * @throws java.io.IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        yarnClient.killApplication(appId);

    }

    private void addToLocalResources(
            FileSystem fs,
            String fileSrcPath,
            String fileDstPath,
            int appId,
            Map<String, LocalResource> localResources
    ) throws IOException {
        String suffix = "YarnApp" + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        LOG.info("Copy {} to {}", fileDstPath, dst);
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);

        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                scFileStatus.getLen(),
                scFileStatus.getModificationTime()
        );
        localResources.put(fileDstPath, scRsrc);
    }


    private ContainerLaunchContext configureApplicationMasterContainer(int appId) throws IOException, YarnException, URISyntaxException {
        LOG.info("Set up the container launch context for the application master");
        ContainerLaunchContext appMasterContainer = Records.newRecord(ContainerLaunchContext.class);

        LOG.info("Set local resources for the application master");
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        // Set application master jar to resources
        addToLocalResources(
                this.fs,
                getJarLocation(),
                "app.jar",
                appId,
                localResources);
        // Set the python script to resources by name 'script.py'
        addToLocalResources(this.fs, pyScript, "script.py", appId, localResources);
        // Set local resources to resources
        for (String arg : args) {
            addToLocalResources(this.fs, arg, new File(arg).getName(), appId, localResources);
        }
        // Copy resources to the filesystem
        appMasterContainer.setLocalResources(localResources);

        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        appMasterContainer.setEnvironment(configureAMEnvironment(localResources, fs));

        LOG.info("Set the necessary command to execute the application master");
        LOG.info("Setting up app master launch command");
        StringBuilder command = new StringBuilder();
        command.append(Environment.JAVA_HOME.$$() + "/bin/java");
        command.append(" ");
        command.append("com.epam.yarn.ApplicationMaster");
        command.append(" ");
        command.append("1><LOG_DIR>/AM.stdout");
        command.append(" ");
        command.append("2><LOG_DIR>/AM.stderr");

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        appMasterContainer.setCommands(commands);

        return appMasterContainer;
    }

    private Map<String, String> configureAMEnvironment(Map<String, LocalResource> localResources,
                                                       FileSystem fs) throws IOException {
        Map<String, String> env = new HashMap<String, String>();
        // Set ApplicationMaster jar file
        LocalResource appJarResource = localResources.get("app.jar");
        Path hdfsAppJarPath = new Path(fs.getHomeDirectory(), appJarResource.getResource().getFile());
        FileStatus hdfsAppJarStatus = fs.getFileStatus(hdfsAppJarPath);
        long hdfsAppJarLength = hdfsAppJarStatus.getLen();
        long hdfsAppJarTimestamp = hdfsAppJarStatus.getModificationTime();

        env.put("AM.JAR", hdfsAppJarPath.toString());
        env.put("AM.JAR.TIMESTAMP", Long.toString(hdfsAppJarTimestamp));
        env.put("AM.JAR.LEN", Long.toString(hdfsAppJarLength));

        //add script matadata to context
        Path hdfsScriptPath = new Path(fs.getHomeDirectory(),
                localResources.get("script.py").getResource().getFile());
        FileStatus hdfsAppScriptStatus = fs.getFileStatus(hdfsScriptPath);
        env.put("SCRIPT.FILE", hdfsScriptPath.toString());
        env.put("SCRIPT.FILE.MODIFICATION.TIME", Long.toString(hdfsAppScriptStatus.getModificationTime()));
        env.put("SCRIPT.FILE.LENGTH", Long.toString(hdfsAppScriptStatus.getLen()));

        //add local resources
        Path hdfsResource;
        FileStatus hdfsResourceStatus;
        String filename;
        for (String arg : args) {
            filename = new File(arg).getName();
            hdfsResource = new Path(fs.getHomeDirectory(), localResources.get(filename).getResource().getFile());
            hdfsResourceStatus = fs.getFileStatus(hdfsResource);
            env.put("*resource*" + filename, hdfsResource.toString());
            env.put(filename + "_time", Long.toString(hdfsResourceStatus.getModificationTime()));
            env.put(filename + "_len", Long.toString(hdfsResourceStatus.getLen()));
        }
        // Add AppMaster jar location to classpath
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
                .append(File.pathSeparatorChar)
                .append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());
        return env;
    }

    private void configureAppMasterCapabilities(ApplicationSubmissionContext appContext, int maxMemory, int maxVCores) {
        Resource capability = Records.newRecord(Resource.class);
        LOG.info("Max virtual cores capability of resources in this cluster {}", maxVCores);
        LOG.info("Max mem capabililty of resources in this cluster {}", maxMemory);
        capability.setMemory(maxMemory);
        capability.setVirtualCores(maxVCores);
        appContext.setResource(capability);
    }

    private void logClusterInformation(GetNewApplicationResponse appResponse) throws YarnException, IOException {
        int maxMemory = appResponse.getMaximumResourceCapability().getMemory();
        int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max memory = {} and max vcores = {}", maxMemory, maxVCores);
        YarnClusterMetrics clusterMetrics =
                yarnClient.getYarnClusterMetrics();
        LOG.info("Number of NodeManagers = {}", clusterMetrics.getNumNodeManagers());

        List<QueueInfo> queueList = yarnClient.getAllQueues();
        for (QueueInfo queue : queueList) {
            LOG.info("Available queue: {} with capacity {} to {}", queue.getQueueName(), queue.getCapacity(),
                    queue.getMaximumCapacity());
        }
    }

}