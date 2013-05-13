package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;

import junit.framework.TestCase;

public class ZooKeeperClientTest extends TestCase{
  private ZooKeeperClient client;
  
  public void setUp(){
    Configuration conf=new Configuration();
    conf.addResource("ha-site.xml");
	try {
	  client=ZooKeeperClient.getInstance(conf);
	} catch (IOException e) {
	  e.printStackTrace();
	}
  }
  public void testGetPrimaryAvatarAddress(String[] args){
    try {
	  String str=client.getPrimaryAvatarAddress(true);
	  assertEquals(true,str!=null);
	} catch (IOException e) {
	  e.printStackTrace();
	} catch (KeeperException e) {
	  e.printStackTrace();
	} catch (InterruptedException e) {
	  e.printStackTrace();
	}
  }
}
