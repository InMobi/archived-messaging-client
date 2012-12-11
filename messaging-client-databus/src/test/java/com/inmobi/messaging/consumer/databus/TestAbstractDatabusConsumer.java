package com.inmobi.messaging.consumer.databus;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.inmobi.databus.Cluster;
import com.inmobi.messaging.ClientConfig;
import com.inmobi.messaging.consumer.util.ConsumerUtil;
import com.inmobi.messaging.consumer.util.TestUtil;

public abstract class TestAbstractDatabusConsumer {

  int numMessagesPerFile = 100;
  int numDataFiles = 3;
  DatabusConsumer testConsumer;
  static final String testStream = "testclient";
  protected String[] collectors;
  protected String[] dataFiles;
  protected String consumerName;
  Path[] rootDirs;
  Configuration conf = new Configuration();

  public void setup(int numFileToMove) throws Exception {

    ClientConfig config = loadConfig();
    testConsumer = getConsumerInstance();
    //System.out.println(testConsumer.getClass().getCanonicalName());
    testConsumer.initializeConfig(config);

    // setup stream, collector dirs and data files
    Set<String> sourceNames = new HashSet<String>();
    sourceNames.add(testStream);

    rootDirs = testConsumer.getRootDirs();
    for (int i =0; i <rootDirs.length; i++) {
      Map<String, String> clusterConf = new HashMap<String, String>();
      FileSystem fs = rootDirs[i].getFileSystem(conf);
      clusterConf.put("hdfsurl", fs.getUri().toString());
      clusterConf.put("jturl", "local");
      clusterConf.put("name", "databusCluster" + i);
      clusterConf.put("jobqueuename", "default");
      
      String rootDir = rootDirs[i].toUri().toString();
      if (rootDirs[i].toString().startsWith("file:")) {
        String[] rootDirSplit = rootDirs[i].toString().split("file:");
        rootDir = rootDirSplit[1];
      }
      Cluster cluster = new Cluster(clusterConf, 
          rootDir, null, sourceNames);
      fs.delete(new Path(cluster.getRootDir()), true);
      Path streamDir = new Path(cluster.getDataDir(), testStream);
      fs.delete(streamDir, true);
      fs.mkdirs(streamDir);
      for (String collector : collectors) {
        Path collectorDir = new Path(streamDir, collector);
        fs.delete(collectorDir, true);
        fs.mkdirs(collectorDir);
        TestUtil.setUpFiles(cluster, collector, dataFiles, null, null,
            numFileToMove, numFileToMove);
      }
    }
  }

	protected DatabusConsumer getConsumerInstance() {
	  return new DatabusConsumer();
  }

  abstract ClientConfig loadConfig();

  void assertMessages(
      ClientConfig config, int numClusters, int numCollectors) 
      throws IOException, InterruptedException {
    ConsumerUtil.assertMessages(config, testStream, consumerName, numClusters,
        numCollectors,
        numDataFiles, 100, false);
  }

  public void cleanup() throws IOException {
    testConsumer.close();
    for (Path p : rootDirs) {
      FileSystem fs = p.getFileSystem(conf);
      fs.delete(p, true);
    }
  }

}
