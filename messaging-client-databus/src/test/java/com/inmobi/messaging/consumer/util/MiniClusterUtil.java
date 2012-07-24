package com.inmobi.messaging.consumer.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class MiniClusterUtil {

  private static MiniDFSCluster dfsCluster;
  private static FileSystem lfs;
  private static int numAccess = 0;

  public static synchronized MiniDFSCluster getDFSCluster(Configuration conf)
      throws IOException {
    if (dfsCluster == null) {
      lfs = FileSystem.getLocal(conf);
      lfs.delete(new Path(MiniDFSCluster.getBaseDir().toString()), true);
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
    }
    numAccess++;
    return dfsCluster;
  }
  
  public static synchronized void shutdownDFSCluster() throws IOException {
    numAccess--;
    if (numAccess == 0) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
      lfs.delete(new Path(MiniDFSCluster.getBaseDir().toString()), true);
    }
  }
}
