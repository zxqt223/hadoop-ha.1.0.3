package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.io.DefaultProxy;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
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
public class NoRetryDFSClient extends DFSClient{

  public NoRetryDFSClient(Configuration conf) throws IOException {
	super(conf);
  }
  
  public NoRetryDFSClient(InetSocketAddress nameNodeAddr, Configuration conf
	  ) throws IOException {
	super(nameNodeAddr, conf);
  }
	
	/**
	* Same as this(nameNodeAddr, null, conf, stats);
	* @see #DFSClient(InetSocketAddress, ClientProtocol, Configuration, org.apache.hadoop.fs.FileSystem.Statistics) 
	*/
  public NoRetryDFSClient(InetSocketAddress nameNodeAddr, Configuration conf,
	               FileSystem.Statistics stats)throws IOException {
    super(nameNodeAddr,conf, stats);
  }
  
  public NoRetryDFSClient(InetSocketAddress nameNodeAddr, ClientProtocol rpcNamenode,
	      Configuration conf, FileSystem.Statistics stats) throws IOException {
    super(nameNodeAddr, rpcNamenode, conf, stats);
  }

  @Override
  protected ClientProtocol createClientProtocol(ClientProtocol rpcNamenode) throws IOException {
    RetryPolicy createPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        5, LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);
    Map<Class<? extends Exception>,RetryPolicy> remoteExceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class, createPolicy);

    Map<Class<? extends Exception>,RetryPolicy> exceptionToPolicyMap =
      new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, 
        RetryPolicies.retryByRemoteException(
            RetryPolicies.TRY_ONCE_THEN_FAIL, remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String,RetryPolicy> methodNameToPolicyMap = new HashMap<String,RetryPolicy>();
    
    methodNameToPolicyMap.put("create", methodPolicy);

    return (ClientProtocol) DefaultProxy.create(ClientProtocol.class,
        rpcNamenode, methodNameToPolicyMap);
  }
  @Override
  protected ClientProtocol createRPCProxy(InetSocketAddress nameNodeAddr,
	      Configuration conf, UserGroupInformation ugi) 
    throws IOException {
	  return (ClientProtocol)RPC.getProxy(ClientProtocol.class,
		        ClientProtocol.versionID, nameNodeAddr, ugi, conf,
		        NetUtils.getSocketFactory(conf, ClientProtocol.class));
  }
}