package org.apache.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
/**
 * 
 *
 * Description: <br>

 * Copyright: Copyright (c) 2012 <br>
 * Company: www.renren.com
 * 
 * @author xianquan.zhang{xianquan.zhang@renren.inc.com} 2012-12-7  
 * @version 1.0
 */
public class ZooKeeperClient implements Watcher{
  private String connection;
  private int timeout;
  private int connectTimeout;
  protected Object mutex = null;
  private ZooKeeper zk;
  private String masterNamenodeZKNode=null;
  private static ZooKeeperClient zkClient=null;
  private Configuration conf=null;
  volatile boolean isConnected = false;

  private static final Log LOG = LogFactory.getLog(ZooKeeperClient.class);
  
  public static final int ZK_CONNECTION_RETRIES = 10;
  public static final int ZK_CONNECT_TIMEOUT_DEFAULT = 20; // 20 seconds
  
  private ZooKeeperClient(Configuration conf) throws IOException {
	this.conf=conf;
	this.conf.addResource("ha-site.xml");
	mutex=new Object();
	this.masterNamenodeZKNode=conf.get("hdfs.zookeeper.root.node.name","/hdfs")+"/master";
    this.connection = conf.get("ha.zookeeper.quorum");
    this.timeout = conf.getInt("ha.zookeeper.timeout", 30)*1000;
    this.connectTimeout = conf.getInt("ha.zookeeper.connect.timeout",
        ZK_CONNECT_TIMEOUT_DEFAULT)*1000;
    initZK();
  }

  public static ZooKeeperClient newInstance(Configuration conf) throws IOException{
	return new ZooKeeperClient(conf);
  }
  public static ZooKeeperClient getInstance(Configuration conf) throws IOException{
    if(zkClient==null){
      synchronized(ZooKeeperClient.class){
        if(zkClient==null){
          zkClient=new ZooKeeperClient(conf);
    	}
      }
    }
    return zkClient;
  }
  
  public void process(WatchedEvent event) {
    switch (event.getState()) {
      case SyncConnected:
    	this.isConnected=true;
        synchronized (mutex) {
          mutex.notify();
        }
        LOG.debug("Successfully connected to ZK.");
        break;
      case Expired:
        LOG.error("Detected conn to zk session expired. retry conn");
        try {
		    stopZK();
		} catch (InterruptedException e) {
		}
        try {
	      initZK();
	    } catch (IOException e) {
	      LOG.error("Fail to reinitialize zk!",e);
	    }
        break;
      case Disconnected:
        LOG.error("Lost connection to ZK. Disconnected, will auto reconnected until success.");
        break;
      default:
        break;
      }
  }
  public synchronized void clearPrimary(String zkNode) throws IOException {
    zkCreateRecursively(zkNode, null, true);
  }
  
  private void zkCreateRecursively(String zNode, byte[] data,
      boolean overwrite) throws IOException {
    LOG.info("create " + zNode);
    String[] parts = zNode.split("/");
    String path = "";
    byte[] payLoad = new byte[0];
    List<ACL> acls = new ArrayList<ACL>(1);
    acls.add(new ACL(Perms.ALL, new Id("world", "anyone")));
    
    try {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].isEmpty())
          continue;
        path += "/" + parts[i];
        if (i == parts.length - 1) {
          payLoad = data;
        }
        boolean created = false;
        while (!created) {
          // While loop to keep trying through the ConnectionLoss exceptions
          try {
            if (zk.exists(path, false) != null) {
              // -1 indicates that we should update zNode regardless of its
              // version
              // since we are not utilizing versions in zNode - this is the best
              if (i == parts.length - 1 && !overwrite) {
                throw new IOException("ZNode " + path + " already exists.");
              }
              zk.setData(path, payLoad, -1);
            } else {
              zk.create(path, payLoad, acls, CreateMode.EPHEMERAL);
            }
            created = true;
          } catch (KeeperException ex) {
            ex.printStackTrace();
            if (KeeperException.Code.CONNECTIONLOSS != ex.code()) {
              throw ex;
            }
          }
        }
      }
      LOG.info("Wrote zNode " + zNode);
    } catch (KeeperException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Tries to connect to ZooKeeper. To be used when we need to test if the
   * ZooKeeper cluster is available and the config is correct
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public synchronized void primeConnect() throws IOException,
      InterruptedException {
     stopZK();
  }

  private synchronized void initZK() throws IOException {
    LOG.info("Connecting to  zookeeper server:" + this.connection);
    synchronized (mutex) {
      zk = new ZooKeeper(this.connection, timeout, this);
      while (!isConnected) {
        LOG.info("Waiting for connection to be established ");
        try {
		  mutex.wait(this.connectTimeout);
		} catch (InterruptedException e) {
		  LOG.error("Fail to connect zookeeper.",e);
		}
      }
    }
    LOG.info("Connected to zookeeper with sessionid: " + zk.getSessionId() + " and session timeout(ms): "
      + timeout);
  }

  private void stopZK() throws InterruptedException {
    if (zk != null){
      zk.close();
      zk=null;
    }
  }

  /**
   * Get the information stored in the node of zookeeper. If retry is set
   * to true it will keep retrying until the data in that node is available
   * (failover case). If the retry is set to false it will return the first
   * value that it gets from the zookeeper.
   * 
   * @param node the path of zNode in zookeeper
   * @param stat {@link Stat} object that will contain stats of the node
   * @param retry if true will retry until the data in znode is not null
   * @return byte[] the data in the znode
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private synchronized byte[] getNodeData(String zkNode, Stat stat, boolean retry)
      throws IOException, KeeperException, InterruptedException {
    int failures = 0;
    
    byte[] data = null;
    while (data == null) {
      try {
        data = zk.getData(zkNode, this, stat);
        if (data == null && retry) {
          // Failover is in progress
          // reset the failures
          failures = 0;
          LOG.info("Failover is in progress. Waiting");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
          }
        } else {
          return data;
        }
      } catch (KeeperException kex) {
        if (KeeperException.Code.CONNECTIONLOSS == kex.code()
            && failures < ZK_CONNECTION_RETRIES) {
          failures++;
          // This means there was a failure connecting to zookeeper
          // we should retry since some nodes might be down.
          continue;
        }
        throw kex;
      }
    }
    return data;
  }
  
  public String getPrimaryAvatarAddress(boolean retry)
    throws IOException, KeeperException, InterruptedException {
    byte[] data = getNodeData(masterNamenodeZKNode, new Stat(), retry);
    if (data == null) {
      return null;
    }
    return new String(data, "UTF-8");
  }

  public synchronized void shutdown() throws InterruptedException {
	zkClient=null;
    stopZK();
  }
  /**
   * 判断某个节点是否存在
   * @param path
   * @param watch
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exist(String path,Watcher watch) throws KeeperException, InterruptedException{
	if(watch==null){
	  return zk.exists(path, false);
	}else{
	  return zk.exists(path, watch);	
	}
  }
}
