package org.test;
import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class Home{
		
	public static void main (String [] args) throws Exception{
		String username = "admin";
		String dest = "hdfs://localhost:8020/user/" + username;
		Path path=new Path(dest);

		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(path)) {
      System.out.println("Dir " + dest + " already exists.");
			return;
  	}

		// Create a new file and write data to it.
		Boolean b = fs.mkdirs(path);
		if (b) {
			System.out.println("Home directory created.");
		} else {
			System.out.println("Home directory failed.");
		}
		fs.close();
	}
}
