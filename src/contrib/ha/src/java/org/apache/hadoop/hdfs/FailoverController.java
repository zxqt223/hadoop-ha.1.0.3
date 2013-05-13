package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AvatarProtocol;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.Avatar;
import org.apache.hadoop.hdfs.protocol.AvatarConstants.InstanceId;
import org.apache.hadoop.hdfs.server.namenode.AvatarNode;
import org.apache.hadoop.ipc.RPC;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.recipes.leader.LeaderElectionAware;
import org.apache.zookeeper.recipes.leader.LeaderElectionSupport;
import org.apache.zookeeper.recipes.leader.LeaderElectionSupport.EventType;
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
public class FailoverController {

  private LeaderElectionSupport les = null;
  private String localHostAddress = null;
  private String localHostName = null;
  private Configuration conf = null;
  private State nnState;
  private State currentNNState;
  private AvatarNode avatarNode=null;
  private String masterNNZnodePath=null;
  private InetSocketAddress avatarAddr=null;

  public static final String NN_MASTER="master";
  private static final String HA_CONF_FILE="ha-site.xml";

  static final Log LOG = LogFactory.getLog(FailoverController.class);

  public void initialize() throws IOException {
    nnState=State.NEUTRAL;
    currentNNState=State.NEUTRAL;
    this.conf = new Configuration();
    this.conf.addResource(HA_CONF_FILE);
    this.masterNNZnodePath=conf.get("hdfs.zookeeper.root.node.name","/hdfs")+"/"+NN_MASTER;
    
    les = new LeaderElectionSupport();
    localHostAddress=getLocalHostAddress();
    localHostName=getLocalHostName();

    les.setConnectString(conf.get("ha.zookeeper.quorum"));
    les.setSessionTimeout(conf.getInt("ha.zookeeper.timeout", 6) * 1000);
    les.setRootNodeName(conf.get("hdfs.zookeeper.root.node.name",
		"/hdfs")+"/namenodes");

    les.addObserver(new NamenodeLeaderElectionAware());
	
    les.start(localHostAddress);
  }

  public boolean isActive() throws InterruptedException{
    if(currentNNState==State.NEUTRAL){
      LOG.info("Wait(1s) namenode election...");
      Thread.sleep(1000);
      return isActive();
    }else if(currentNNState==State.STANDBY){
      return false;
    }
    return true;
  }

  public boolean isStandby() throws InterruptedException{
    return !isActive();
  }

  private void processState(){
	//说明当前节点是Active节点
    if(nnState==State.STANDBY){
		//由Standby节点升级到Active节点
      if(this.avatarNode==null){
        LOG.error("The standby namenode can't upgrade to active,because there is no avatarnode.");
        return ;
      }
      try {
		AvatarProtocol avatarNodeProxy=getAvatarNodeProxy();
		if(avatarNodeProxy.isNamenodeAvailable()){
		  LOG.info("Active Namenode is live,it listen zookeeper contine!");
		  return ;
		}
	  } catch (IOException e1) {
		LOG.info("Active Namenode has dead,Standby Namenode will be upgrade to Active Namenode");
	  }
      try {
        this.avatarNode.setAvatar(Avatar.ACTIVE);
      } catch (IOException e) {
        LOG.error("Fail to send avatar node exchange status!",e);
        System.exit(-1);
      }
    }
    if(!createMasterNode()){
      System.exit(-1);
    }
    nnState=State.ACTIVE;
  }
  
  private boolean createMasterNode(){
    try {
      les.createZNode(masterNNZnodePath,this.getNNAddressByCurrentStateIsActive(),CreateMode.EPHEMERAL);
    } catch (IOException e) {
      LOG.error("Fail to register primary namenode to zookeeper!",e);
      return false;
    }
    return true;
  }
  private AvatarProtocol getAvatarNodeProxy() throws IOException{
    return (AvatarProtocol) RPC.getProxy(AvatarProtocol.class,AvatarProtocol.versionID,avatarAddr,this.conf);
  }
  
  public void registerAvatarNode(AvatarNode avatarNode){
    this.avatarNode=avatarNode;
  }

  private class NamenodeLeaderElectionAware implements LeaderElectionAware {
	@Override
    public void onElectionEvent(EventType event) {
	  LOG.info("EventType:"+event.toString());
	  if (event == EventType.ELECTED_COMPLETE) {
	    currentNNState=State.ACTIVE;
		processState();
	  }else if (event == EventType.READY_COMPLETE) {
        currentNNState=State.STANDBY;
        nnState=State.STANDBY;
		try {
		  String primaryAvatarAddress = getNNAddressByCurrentStateIsStandby();
		  String[] arr=primaryAvatarAddress.substring(7).split(":");
  	      avatarAddr=new InetSocketAddress(arr[0],conf.getInt("fs.avatarnode.port",Integer.parseInt(arr[1])+1));
		} catch (IOException e) {
		  LOG.error("Fail to get primary Avatar Address,and System exit!",e);
		  System.exit(-1);
		}
      }else if(event == EventType.EXPIRED_PROCESS_COMPLETE && currentNNState==State.ACTIVE){
    	if(!createMasterNode()){
    	  LOG.error("Fail to create master node when zookeeper expired process successfully!");	
    	}
      }
	}
  }

  enum State {
    ACTIVE, STANDBY, NEUTRAL
  }

  private String getLocalHostAddress() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostAddress();
  }

  private String getLocalHostName() throws UnknownHostException{
    return InetAddress.getLocalHost().getHostName();
  }

  public String getNNAddressByCurrentStateIsActive() throws IOException{
    if(conf.get("fs.default.name0").contains(localHostAddress)||conf.get("fs.default.name0").contains(localHostName)){
      return conf.get("fs.default.name0");
	}
	if(conf.get("fs.default.name1").contains(localHostAddress)||conf.get("fs.default.name1").contains(localHostName)){
	  return conf.get("fs.default.name1");
	}
	throw new IOException("Fail to find namenode config of this node!Please check namenode address configuration.");
  }

  public String getNNAddressByCurrentStateIsStandby() throws IOException{
    if(conf.get("fs.default.name0").contains(localHostAddress)||conf.get("fs.default.name0").contains(localHostName)){
      return conf.get("fs.default.name1");
	}
	if(conf.get("fs.default.name1").contains(localHostAddress)||conf.get("fs.default.name1").contains(localHostName)){
	  return conf.get("fs.default.name0");
	}
	throw new IOException("Fail to find namenode config of this node!Please check namenode address configuration.");
  }
  
  public InstanceId getInstanceId(){
	if(conf.get("fs.default.name0").contains(localHostAddress)||conf.get("fs.default.name0").contains(localHostName)){
	  return InstanceId.NODEZERO;
	}else {
	  return InstanceId.NODEONE;
	}
  }
}
