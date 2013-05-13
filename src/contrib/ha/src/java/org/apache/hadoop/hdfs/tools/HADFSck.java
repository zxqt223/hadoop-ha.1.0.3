package org.apache.hadoop.hdfs.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
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
public class HADFSck extends DFSck{
  static{
    Configuration.addDefaultResource("ha-site.xml");
  }
  public HADFSck(Configuration conf) throws IOException {
	super(conf);
  }

  public static void main(String[] args) throws Exception {
    int res = -1;
    if ((args.length == 0 ) || ("-files".equals(args[0]))) 
      printUsage();
    else{
      Configuration conf=new Configuration();
      ZooKeeperClient zkClient=ZooKeeperClient.getInstance(conf);
      String avatarnodeAdd=zkClient.getPrimaryAvatarAddress(true);
      if(conf.get("fs.default.name0").equals(avatarnodeAdd)){
    	conf.set("dfs.http.address", conf.get("dfs.http.address0"));
      }else if(conf.get("fs.default.name1").equals(avatarnodeAdd)){
    	conf.set("dfs.http.address", conf.get("dfs.http.address1"));
      }else{
    	 System.err.println("Avatarnode Address is not equal fs.default.name0 or fs.default.name1,please check config file!");
      }
      res = ToolRunner.run(new HADFSck(conf), args);
      zkClient.shutdown();
    }
    System.exit(res);
  }
}
