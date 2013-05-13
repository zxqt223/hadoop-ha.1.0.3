package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.recipes.leader.LeaderElectionAware;
import org.apache.zookeeper.recipes.leader.LeaderElectionSupport;
import org.apache.zookeeper.recipes.leader.LeaderElectionSupport.EventType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZooKeeperClient;
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
public class HAJobTracker{

  private LeaderElectionSupport les = null;
  private String jtAddress = null;
  private State currentJTState = State.NEUTRAL;
  private JobTracker jobtracker = null;
  private JobConf conf = null;

  public static final Log LOG = LogFactory.getLog(HAJobTracker.class);

  static{
    Configuration.addDefaultResource("ha-site.xml");
  }

  private HAJobTracker(){}

  public void initialize(JobConf conf) throws IOException {
	this.conf = conf;
	ZooKeeperClient zkClient=ZooKeeperClient.getInstance(conf);
	try {
	  String namenodeAddress=zkClient.getPrimaryAvatarAddress(true);
	  conf.set("fs.default.name", namenodeAddress);
	} catch (KeeperException e) {
	  throw new IOException("Fail to get namenode address by zookeeper."+e);
	} catch (InterruptedException e) {
	  throw new IOException("Fail to get namenode address by zookeeper."+e);
	}
	try {
	  zkClient.shutdown();
	} catch (InterruptedException e) {
	   LOG.warn("Fail to close zookeeper client.",e);
	}
	les = new LeaderElectionSupport();

	les.setConnectString(conf.get("ha.zookeeper.quorum"));
	les.setSessionTimeout(conf.getInt("ha.zookeeper.timeout", 6) * 1000);
	les.setRootNodeName(conf.get("jobtracker.zookeeper.root.node.name",
			"/jobtracker"));

	this.jtAddress = getLocalJobTrackerAddress();
	if (jtAddress == null) {
		throw new IOException("Fail to find this jobtracker address!");
	}
	conf.set("mapred.job.tracker", this.jtAddress);

	les.addObserver(new DefaultLeaderElectionAware());
  }

  public void start() {
	les.start(this.jtAddress);
  }

  private String getLocalJobTrackerAddress() throws UnknownHostException {
	String addresses = conf.get("jobtracker.servers");
	InetAddress ia = InetAddress.getLocalHost();
	String host = ia.getHostAddress();
	String name = ia.getHostName();
	String[] addressArr = addresses.split(",");
	for (String address : addressArr) {
		if (address.contains(host) || address.contains(name)) {
			return address;
		}
	}
	return null;
  }

  private void execute(State state) {
	LOG.info("jobtracker last state : " + currentJTState
			+ " , jobtracker current state : " + state);
	if (State.ACTIVE == state) {
		startJobTracker();
		currentJTState = state;
	} else if (State.NEUTRAL == state) {
		if (State.ACTIVE == currentJTState) {
			stopJobTracker();
		}
		currentJTState = state;
	}
  }

  private void stopJobTracker() {
	if (jobtracker != null) {
		try {
			jobtracker.stopTracker();
			LOG.info("Success to stop job tracker!");
		} catch (IOException e) {
			LOG.error("Fail to stop job tracker!", e);
		}
	}
  }

  private void startJobTracker() {
	try {
		jobtracker = JobTracker.startTracker(conf);
		jobtracker.offerService();
		LOG.info("Success to start job tracker!");
	} catch (IOException e) {
		LOG.error("Fail to start job tracker,and exit system!", e);
		System.exit(-1);
	} catch (InterruptedException e) {
		LOG.error("Fail to start job tracker,and exit system!", e);
		System.exit(-1);
	}
  }

  private class DefaultLeaderElectionAware implements LeaderElectionAware {
	@Override
    public void onElectionEvent(EventType event) {
      if (event == EventType.ELECTED_COMPLETE) {
	    String data = null;
	    try {
		  data = les.getData();
	    } catch (KeeperException e) {
		  e.printStackTrace();
		  execute(State.NEUTRAL);
		  return;
	    } catch (InterruptedException e) {
		  e.printStackTrace();
		  execute(State.NEUTRAL);
		  return;
	    }
	    if (data.equals(jtAddress)) {
	      execute(State.ACTIVE);
	    } else {
          execute(State.STANDBY);
	    }
      } else if (event == EventType.FAILED) {
	    execute(State.NEUTRAL);
      }
	}
  }

  enum State {
	ACTIVE, STANDBY, NEUTRAL
  }

  public static void main(String argv[]) throws IOException,
		InterruptedException {
    StringUtils.startupShutdownMessage(HAJobTracker.class, argv, LOG);
    try {
      if (argv.length == 0) {
        JobConf conf = new JobConf();
        if (conf.getStrings("jobtracker.servers", "").equals("")) {
          JobTracker.main(argv);
		} else {
		  HAJobTracker haJT = new HAJobTracker();
          haJT.initialize(conf);
          haJT.start();
          Thread.currentThread().join();
        }
      } else {
        System.exit(-1);
	  }
    } catch (Throwable e) {
	  LOG.fatal(StringUtils.stringifyException(e));
	  System.exit(-1);
    }
  }
}
