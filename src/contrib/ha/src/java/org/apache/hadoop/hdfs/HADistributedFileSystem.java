/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import java.io.*;
import java.net.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.Progressable;
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
public class HADistributedFileSystem extends DistributedFileSystem {
  private URI uri;

  public static final Log LOG = LogFactory.getLog(HADistributedFileSystem.class);
  
  public HADistributedFileSystem() {
  }

  /** @deprecated */
  public HADistributedFileSystem(InetSocketAddress namenode,
    Configuration conf) throws IOException {
    initialize(NameNode.getUri(namenode), conf);
  }

  /** @deprecated */
  public String getName() { 
    return uri.getAuthority(); 
  }

  private String namenodeAddress=null;
  private Configuration conf=null;
  private DistributedFileSystem haDFS=null;
  private ZooKeeperClient zkClient=null;
  private ReentrantLock lock=new ReentrantLock();
  
  public URI getUri() {
	return uri;
  }

  public void initialize(URI uri, Configuration conf) throws IOException {
	this.uri=uri;
	this.conf=conf;
	this.zkClient=ZooKeeperClient.getInstance(conf);
	this.namenodeAddress=getNamenodeAddress();
	
	initialize();
  }

  private void initialize() throws IOException {
	LOG.info("start initialize ha distribute file system!");
    String strURI=this.uri.toString();
    if(!strURI.startsWith(this.namenodeAddress)){
      LOG.info("Namenode failover maybe happened ,the uri :"+uri+",the namenode address :"+namenodeAddress);
      int pos=indexOf(strURI);
      if(pos==-1){
        this.uri=URI.create(this.namenodeAddress);
      }else{
    	this.uri=URI.create(this.namenodeAddress+strURI.substring(pos));
      }
    }
    conf.set("fs.default.name", this.namenodeAddress);
    haDFS=new DistributedFileSystem();
    haDFS.initialize(this.uri, conf);
    haDFS.dfs.close();
    haDFS.dfs=new NoRetryDFSClient(NameNode.getAddress(uri.getAuthority()), conf, statistics);
  }
  
  private int indexOf(String strURI){
	return strURI.indexOf("/", 7);
  }
  /**@ADDCODE xianquan.zhang*/
  private boolean reinitialize(){
	lock.lock();
	boolean res=false;
	String address=getNamenodeAddress();
	/**judge namenode failover*/
	if(this.namenodeAddress!=null && !address.equals(namenodeAddress)){
	  LOG.info("reinitialize hdfs,the current namenodeAddress is :"+namenodeAddress+",and zookeeper hdfs namenodeAddress is:"+address);
	  this.namenodeAddress=address;
	  try{
        haDFS.close();
      }catch(IOException e){
        LOG.error("Fail to close DistributedFileSystem,the exception is :"+e.getMessage());
      }
      while(true){
    	try{
    	  initialize();
    	  break;
    	}catch(IOException e){
    	  try {
			Thread.sleep(2000);
		  } catch (InterruptedException e1) {
		  }
    	  LOG.info("Fail to initialize HADistributedFileSystem,the exception is "+e.getMessage()+",try to again.");
    	}
      }
	  
	  res=true;
	}
	lock.unlock();
	return res;
  }
  /**@ADDCODE xianquan.zhang*/
  private String getNamenodeAddress(){
    while(true){
	  try {
		return this.zkClient.getPrimaryAvatarAddress(true);
	  } catch (Exception e) {
		LOG.info("Fail to get namenode address by zookeeper,the exception is "+e.getMessage()+".Sleep 3s and retry it again.");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {}
	  } 
	}
  }
  
  public Path getWorkingDirectory() {
    return haDFS.getWorkingDirectory();
  }

  public long getDefaultBlockSize() {
    return haDFS.getDefaultBlockSize();
  }

  public short getDefaultReplication() {
    return haDFS.getDefaultReplication();
  }

  public void setWorkingDirectory(Path dir) {
	dir=validatePath(dir);
    haDFS.setWorkingDirectory(dir);
  }

  /** {@inheritDoc} */
  public Path getHomeDirectory() {
    return haDFS.getHomeDirectory();
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
	try{
	  return haDFS.getFileBlockLocations(file, start, len);
	}catch(IOException e){
      if(this.reinitialize()){
        return haDFS.getFileBlockLocations(file, start, len);
      }else{
        throw e;
      }
	}
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    haDFS.setVerifyChecksum(verifyChecksum);
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.open(f, bufferSize);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.open(f, bufferSize);
      }else{
        throw e;
      }
	}
  }

  /** 
   * Start the lease recovery of a file
   *
   * @param f a file
   * @return true if the file is already closed
   * @throws IOException if an error occurs
   */
  public boolean recoverLease(Path f) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.recoverLease(f);	
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.recoverLease(f);
      }else{
        throw e;
      }
	}
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.append(f, bufferSize, progress);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.append(f, bufferSize, progress);
      }else{
        throw e;
      }
	}
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
    boolean overwrite,
    int bufferSize, short replication, long blockSize,
    Progressable progress) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
      }else{
        throw e;
      }
	}
  }

  /**
   * Same as create(), except fails if parent directory doesn't already exist.
   * @see #create(Path, FsPermission, boolean, int, short, long, Progressable)
   */
  @Override
  public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
      boolean overwrite,
      int bufferSize, short replication, long blockSize, 
      Progressable progress) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.create(f, permission, 
                overwrite, bufferSize, replication, blockSize, progress);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.create(f, permission, 
	              overwrite, bufferSize, replication, blockSize, progress);
      }else{
        throw e;
      }
	}
  }

  public boolean setReplication(Path src, 
                                short replication
                               ) throws IOException {
	src=validatePath(src);
    try{
      return haDFS.setReplication(src, replication);
	}catch(IOException e){
	  if(this.reinitialize()){
		src=validatePath(src);
	    return haDFS.setReplication(src, replication);
      }else{
        throw e;
      }
	}
  }

  /**
   * Rename files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
	src=validatePath(src);
	dst=validatePath(dst);
    try{
      return haDFS.rename(src, dst);
	}catch(IOException e){
	  if(this.reinitialize()){
	    src=validatePath(src);
		dst=validatePath(dst);
		return haDFS.rename(src, dst);
      }else{
        throw e;
      }
	}
  }

  /**
   * Get rid of Path f, whether a true file or dir.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    f=validatePath(f);
    try{
      return haDFS.delete(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.delete(f);
      }else{
        throw e;
      }
	}
  }
  
  /**
   * requires a boolean check to delete a non 
   * empty directory recursively.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.delete(f, recursive);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.delete(f, recursive);
      }else{
        throw e;
      }
	}
  }
  
  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.getContentSummary(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.getContentSummary(f);
      }else{
        throw e;
      }
	}
  }

  /** Set a directory's quotas
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long, long) 
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
	src=validatePath(src);
    try{
      haDFS.setQuota(src, namespaceQuota, diskspaceQuota);
	}catch(IOException e){
	  LOG.info("Fail to call setQuota(),the exception is :"+e.getMessage()+",reinitialize again.");
	  if(this.reinitialize()){
		src=validatePath(src);
	    haDFS.setQuota(src, namespaceQuota, diskspaceQuota);
	  }
	}
  }
  
  /**
   * List all the entries of a directory
   * 
   * Note that this operation is not atomic for a large directory.
   * The entries of a directory may be fetched from NameNode multiple times.
   * It only guarantees that  each name occurs once if a directory 
   * undergoes changes between the calls.
   */
  @Override
  public FileStatus[] listStatus(Path p) throws IOException {
	p=validatePath(p);
    try{
      return haDFS.listStatus(p);
	}catch(IOException e){
	  if(this.reinitialize()){
	    p=validatePath(p);
		return haDFS.listStatus(p);
      }else{
        throw e;
      }
	}
  }

  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.mkdirs(f, permission);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.mkdirs(f, permission);
      }else{
        throw e;
      }
	}
  }

  /** {@inheritDoc} */
  public void close() throws IOException {
//	this.zkClient.removeWatcher(this);
    haDFS.close();
  }

  public String toString() {
    return haDFS.toString();
  }

  public DFSClient getClient() {
    return haDFS.getClient();
  }        
  
  /** Return the disk usage of the filesystem, including total capacity,
   * used space, and remaining space */
  public DiskStatus getDiskStatus() throws IOException {
    try{
      return haDFS.getDiskStatus();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getDiskStatus();
      }else{
        throw e;
      }
	}
  }
  
  /** Return the total raw capacity of the filesystem, disregarding
   * replication .*/
  public long getRawCapacity() throws IOException{
    try{
      return haDFS.getRawCapacity();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getRawCapacity();
      }else{
        throw e;
      }
	}
  }

  /** Return the total raw used space in the filesystem, disregarding
   * replication .*/
  public long getRawUsed() throws IOException{
    try{
      return haDFS.getRawUsed();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getRawUsed();
      }else{
        throw e;
      }
	}
  }
   
  /**
   * Returns count of blocks with no good replicas left. Normally should be
   * zero.
   * 
   * @throws IOException
   */
  public long getMissingBlocksCount() throws IOException {
    try{
      return haDFS.getMissingBlocksCount();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getMissingBlocksCount();
      }else{
        throw e;
      }
	}
  }

  /**
   * Returns count of blocks with one of more replica missing.
   * 
   * @throws IOException
   */
  public long getUnderReplicatedBlocksCount() throws IOException {
    try{
	  return haDFS.getUnderReplicatedBlocksCount();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getUnderReplicatedBlocksCount();
      }else{
        throw e;
      }
	}
  }

  /**
   * Returns count of blocks with at least one replica marked corrupt.
   * 
   * @throws IOException
   */
  public long getCorruptBlocksCount() throws IOException {
    try{
  	  return haDFS.getCorruptBlocksCount();
  	}catch(IOException e){
  	  if(this.reinitialize()){
		return haDFS.getCorruptBlocksCount();
      }else{
        throw e;
      }
  	}
  }

  /** Return statistics for each datanode. */
  public DatanodeInfo[] getDataNodeStats() throws IOException {
    try{
	  return haDFS.getDataNodeStats();
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getDataNodeStats();
      }else{
        throw e;
      }
	}
  }

  /**
   * Enter, leave or get safe mode.
   *  
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setSafeMode(
   *    FSConstants.SafeModeAction)
   */
  public boolean setSafeMode(FSConstants.SafeModeAction action) 
  throws IOException {
    try{
      return haDFS.setSafeMode(action);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		return haDFS.setSafeMode(action);
      }else{
        throw e;
      }
  	}
  }

  /**
   * Save namespace image.
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#saveNamespace()
   */
  public void saveNamespace() throws AccessControlException, IOException {
    try{
      haDFS.saveNamespace();
	}catch(IOException e){
	  if(this.reinitialize()){
		haDFS.saveNamespace();
      }else{
        throw e;
      }
	}
  }

  /**
   * Refreshes the list of hosts and excluded hosts from the configured 
   * files.  
   */
  public void refreshNodes() throws IOException {
    try{
      haDFS.refreshNodes();
  	}catch(IOException e){
  	  if(this.reinitialize()){
		haDFS.refreshNodes();
      }else{
        throw e;
      }
  	}
  }

  /**
   * Finalize previously upgraded files system state.
   * @throws IOException
   */
  public void finalizeUpgrade() throws IOException {
    try{
      haDFS.finalizeUpgrade();
	}catch(IOException e){
	  if(this.reinitialize()){
		haDFS.finalizeUpgrade();
      }else{
        throw e;
      }
	}
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException {
    try{
      return haDFS.distributedUpgradeProgress(action);
  	}catch(IOException e){
  	  if(this.reinitialize()){
		return haDFS.distributedUpgradeProgress(action);
      }else{
        throw e;
      }
  	}
  }

  /*
   * Requests the namenode to dump data strcutures into specified 
   * file.
   */
  public void metaSave(String pathname) throws IOException {
    try{
      haDFS.metaSave(pathname);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		haDFS.metaSave(pathname);
      }else{
        throw e;
      }
  	}
  }

  /**
   * We need to find the blocks that didn't match.  Likely only one 
   * is corrupt but we will report both to the namenode.  In the future,
   * we can consider figuring out exactly which block is corrupt.
   */
  public boolean reportChecksumFailure(Path f, 
    FSDataInputStream in, long inPos, 
    FSDataInputStream sums, long sumsPos) {
	f=validatePath(f);
    return haDFS.reportChecksumFailure(f, in, inPos, sums, sumsPos);
  }

  /**
   * Returns the stat information about the file.
   * @throws FileNotFoundException if the file does not exist.
   */
  public FileStatus getFileStatus(Path f) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.getFileStatus(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.getFileStatus(f);
	  }else{
		  throw e;
	  }
	}
  }

  /** {@inheritDoc} */
  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
	f=validatePath(f);
    try{
      return haDFS.getFileChecksum(f);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		f=validatePath(f);
  		return haDFS.getFileChecksum(f);
      }else{
        throw e;
      }
  	}
  }

  /** {@inheritDoc }*/
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
	p=validatePath(p);
    try{
      haDFS.setPermission(p, permission);
	}catch(IOException e){
	  if(this.reinitialize()){
		p=validatePath(p);
		haDFS.setPermission(p, permission);
      }else{
        throw e;
      }
	}
  }

  /** {@inheritDoc }*/
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
	p=validatePath(p);
    try{
      haDFS.setOwner(p,username, groupname);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		p=validatePath(p);
  		haDFS.setOwner(p,username, groupname);
      }else{
        throw e;
      }
  	}
  }

  /** {@inheritDoc }*/
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
	p=validatePath(p);
    try{
      haDFS.setTimes(p, mtime, atime);
	}catch(IOException e){
	  if(this.reinitialize()){
		p=validatePath(p);
		haDFS.setTimes(p, mtime, atime);
      }else{
        throw e;
      }
	}
  }

  @Override
  protected int getDefaultPort() {
    return haDFS.getDefaultPort();
  }

  @Override
  public 
  Token<DelegationTokenIdentifier> getDelegationToken(String renewer
                                                      ) throws IOException {
    try{
      return haDFS.getDelegationToken(renewer);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		return haDFS.getDelegationToken(renewer);
      }else{
        throw e;
      }
  	}
  }

  /** 
   * Delegation Token Operations
   * These are haDFS only operations.
   */
  
  /**
   * Get a valid Delegation Token.
   * 
   * @param renewer Name of the designated renewer for the token
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   * @Deprecated use {@link #getDelegationToken(String)}
   */
  @Deprecated
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    try{
      return haDFS.getDelegationToken(renewer);
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.getDelegationToken(renewer);
      }else{
        throw e;
      }
	}
  }

  /**
   * Renew an existing delegation token.
   * 
   * @param token delegation token obtained earlier
   * @return the new expiration time
   * @throws IOException
   * @deprecated Use Token.renew instead.
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    try{
      return haDFS.renewDelegationToken(token);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		return haDFS.renewDelegationToken(token);
      }else{
        throw e;
      }
  	}
  }

  /**
   * Cancel an existing delegation token.
   * 
   * @param token delegation token
   * @throws IOException
   * @deprecated Use Token.cancel instead.
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    try{
      haDFS.cancelDelegationToken(token);
	}catch(IOException e){
	  if(this.reinitialize()){
		haDFS.cancelDelegationToken(token);
      }else{
        throw e;
      }
	}
  }

  /**
   * Requests the namenode to tell all datanodes to use a new, non-persistent
   * bandwidth value for haDFS.balance.bandwidthPerSec.
   * The bandwidth parameter is the max number of bytes per second of network
   * bandwidth to be used by a datanode during balancing.
   *
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    try{
      haDFS.setBalancerBandwidth(bandwidth);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		haDFS.setBalancerBandwidth(bandwidth);
      }else{
        throw e;
      }
  	}
  }
  
  public Path validatePath(Path path){
	String strURI=path.toUri().toString();
	if(strURI.startsWith("hdfs:")&&!strURI.startsWith(this.namenodeAddress)){
	  int pos=indexOf(strURI);
	  path.setUri(URI.create(this.namenodeAddress+strURI.substring(pos)));
	}
	return path;
  }
  

  /**
   * Mark a path to be deleted when FileSystem is closed.
   * When the JVM shuts down,
   * all FileSystem objects will be closed automatically.
   * Then,
   * the marked path will be deleted as a result of closing the FileSystem.
   *
   * The path has to exist in the file system.
   * 
   * @param f the path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException
   */
  public boolean deleteOnExit(Path f) throws IOException {
	f=validatePath(f);
	try{
	  return haDFS.deleteOnExit(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.deleteOnExit(f);
      }else{
        throw e;
      }
	}
  }

  /** Check if exists.
   * @param f source file
   */
  public boolean exists(Path f) throws IOException {
	f=validatePath(f);
	try{
	  return haDFS.exists(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		return haDFS.exists(f);
      }else{
        throw e;
      }
	}
  }

  /** True iff the named path is a directory. */
  /** @deprecated Use getFileStatus() instead */ @Deprecated
  public boolean isDirectory(Path f) throws IOException {
	f=validatePath(f);
	try{
	  return haDFS.isDirectory(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.isDirectory(f);
      }else{
        throw e;
      }
	}
  }

  /** True iff the named path is a regular file. */
  public boolean isFile(Path f) throws IOException {
	f=validatePath(f);
	try{
	  return haDFS.isFile(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.isFile(f);
      }else{
        throw e;
      }
	}
  }
    
  /** The number of bytes in a file. */
  /** @deprecated Use getFileStatus() instead */ @Deprecated
  public long getLength(Path f) throws IOException {
    f=validatePath(f);
	try{
	  return haDFS.getLength(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.getLength(f);
      }else{
       throw e;
      }
	}
  }
    
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
	pathPattern=validatePath(pathPattern);
    try{
  	  return haDFS.globStatus(pathPattern);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		pathPattern=validatePath(pathPattern);
  		return haDFS.globStatus(pathPattern);
      }else{
        throw e;
      }
  	}
  }
  
  /**
   * Return an array of FileStatus objects whose path names match pathPattern
   * and is accepted by the user-supplied path filter. Results are sorted by
   * their path names.
   * Return null if pathPattern has no glob and the path does not exist.
   * Return an empty array if pathPattern has a glob and no path matches it. 
   * 
   * @param pathPattern
   *          a regular expression specifying the path pattern
   * @param filter
   *          a user-supplied path filter
   * @return an array of FileStatus objects
   * @throws IOException if any I/O error occurs when fetching file status
   */
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    pathPattern=validatePath(pathPattern);
    try{
  	  return haDFS.globStatus(pathPattern,filter);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		pathPattern=validatePath(pathPattern);
  		return haDFS.globStatus(pathPattern,filter);
      }else{
        throw e;
      }
  	}
  }
  public boolean mkdirs(Path f) throws IOException {
	f=validatePath(f);
    try{
	  return haDFS.mkdirs(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
	    return haDFS.mkdirs(f);
      }else{
        throw e;
      }
	}
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name and the source is kept intact afterwards
   */
  public void copyFromLocalFile(Path src, Path dst)
    throws IOException {
	dst=validatePath(dst);
	try{
	  haDFS.copyFromLocalFile(src, dst);
	}catch(IOException e){
	  if(this.reinitialize()){
		dst=validatePath(dst);
	    haDFS.copyFromLocalFile(src, dst);
      }else{
        throw e;
      }
	}
  }

  /**
   * The src files is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   */
  public void moveFromLocalFile(Path[] srcs, Path dst)
    throws IOException {
    dst=validatePath(dst);
    try{
      haDFS.moveFromLocalFile(srcs, dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		dst=validatePath(dst);
  		haDFS.moveFromLocalFile(srcs, dst);
	  }else{
	    throw e;
	  }
  	}
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   */
  public void moveFromLocalFile(Path src, Path dst)
    throws IOException {
	dst=validatePath(dst);
	try{
      haDFS.copyFromLocalFile(src, dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		dst=validatePath(dst);
  		haDFS.copyFromLocalFile(src, dst);
	  }else{
	    throw e;
	  }
  	}
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
	dst=validatePath(dst);
	try{
      haDFS.copyFromLocalFile(delSrc,src, dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		dst=validatePath(dst);
  		haDFS.copyFromLocalFile(delSrc,src, dst);
	  }else{
	    throw e;
	  }
  	}
  }
  
  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path[] srcs, Path dst)
    throws IOException {
	dst=validatePath(dst);
	try{
      haDFS.copyFromLocalFile(delSrc, overwrite,srcs,dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		dst=validatePath(dst);
  		haDFS.copyFromLocalFile(delSrc, overwrite,srcs,dst);
	  }else{
	    throw e;
	  }
  	}
  }
  
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, 
                                Path src, Path dst)
    throws IOException {
	dst=validatePath(dst);
	try{
      haDFS.copyFromLocalFile(delSrc,overwrite,src, dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		dst=validatePath(dst);
  		haDFS.copyFromLocalFile(delSrc,overwrite,src, dst);
	  }else{
	    throw e;
	  }
  	}
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  public void copyToLocalFile(Path src, Path dst) throws IOException {
	src=validatePath(src);
	try{
      haDFS.copyToLocalFile(src,dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		src=validatePath(src);
  		haDFS.copyToLocalFile(src,dst);
	  }else{
	    throw e;
	  }
  	}
  }
    
  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * Remove the source afterwards
   */
  public void moveToLocalFile(Path src, Path dst) throws IOException {
	src=validatePath(src);
	try{
      haDFS.moveToLocalFile(src, dst);
  	}catch(IOException e){
  	  if(this.reinitialize()){
  		src=validatePath(src);
  		haDFS.moveToLocalFile(src, dst);
	  }else{
	    throw e;
	  }
  	}
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   */   
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
	src=validatePath(src);
    try{
      haDFS.copyToLocalFile(delSrc,src,dst);
	}catch(IOException e){
	  if(this.reinitialize()){
		src=validatePath(src);
		haDFS.copyToLocalFile(delSrc,src,dst);
  	  }else{
  	    throw e;
  	  }
    }
  }

  /**
   * Returns a local File that the user can write output to.  The caller
   * provides both the eventual FS target name and the local working
   * file.  If the FS is local, we write directly into the target.  If
   * the FS is remote, we write into the tmp local area.
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return validatePath(tmpLocalFile);
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
	tmpLocalFile=validatePath(tmpLocalFile);
	fsOutputFile=validatePath(fsOutputFile);
	try{
      haDFS.completeLocalOutput(tmpLocalFile, fsOutputFile);
	}catch(IOException e){
	  if(this.reinitialize()){
	    tmpLocalFile=validatePath(tmpLocalFile);
		fsOutputFile=validatePath(fsOutputFile);
		haDFS.completeLocalOutput(tmpLocalFile, fsOutputFile);
  	  }else{
  	    throw e;
  	  }
    }
  }

  /**
   * Get the block size for a particular file.
   * @param f the filename
   * @return the number of bytes in a block
   */
  /** @deprecated Use getFileStatus() instead */ @Deprecated
  public long getBlockSize(Path f) throws IOException {
	f=validatePath(f);
	try{
      return haDFS.getBlockSize(f);
	}catch(IOException e){
	  if(this.reinitialize()){
		f=validatePath(f);
		return haDFS.getBlockSize(f);
  	  }else{
  	    throw e;
  	  }
    }
  }
  public Path makeQualified(Path path) {
	path=validatePath(path);
    return haDFS.makeQualified(path);
  }
}
