/** 
* Program to recover the lease of a lingering file in HDFS
* javac -cp `hadoop classpath` HDFSClient.java
* jar -cvf recover.jar HDFSClient.class
* hadoop jar recover.jar HDFSClient /path/to/hdfs.file
*
**/

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class HDFSClient {
  Configuration config;
  DistributedFileSystem hdfs;

public HDFSClient() throws IOException{
  config = new Configuration();
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
  config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
  config.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));

  FileSystem fs = FileSystem.get(config);
  hdfs = (DistributedFileSystem) fs;
 
}

public static void printUsage(){
  System.out.println(" Pass in filename to recover lease. i.e. /user/admin/123.json");
}

public boolean ifExists (Path source) throws IOException{
 
  Configuration config = new Configuration();
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
  config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
  config.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
   
  FileSystem hdfs = FileSystem.get(config);
  boolean isExists = hdfs.exists(source);
  return isExists;
}

public boolean recoverLease(Path source) throws IOException{
  return hdfs.recoverLease(source);

}

public boolean isFileClosed(Path source) throws IOException{
  return hdfs.isFileClosed(source);
}

public void close() throws IOException{
  hdfs.close();
}

public static void main(String[] args) throws IOException, InterruptedException {
 
  if (args.length < 1) {
    printUsage();
    System.exit(1);
  }
  Path p = new Path(args[0]);
   
  HDFSClient client = new HDFSClient();
  boolean b = client.recoverLease(p);
  boolean isClosed = client.isFileClosed(p);
  for (int i = 0; i < 600; i++) {
    isClosed = client.isFileClosed(p);
    if (isClosed) {
      System.out.println("Lease recover successfully closed for file: " + args[0]);
      break;
    } else {
      System.out.println("Lease recover processing for file. Sleep for 1 sec: " + args[0]);
      System.out.println("Slept for " + i + " seconds");
      Thread.sleep(1000);
    }
  }
  
  if (isClosed) {
    System.out.println("Lease recovery successful");
  } else {
    System.out.println("Lease recover failed to close file: " + args[0]);
  }
  
  client.close();
}

}
