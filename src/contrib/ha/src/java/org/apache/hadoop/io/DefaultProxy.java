package org.apache.hadoop.io;

import java.lang.reflect.Proxy;
import java.util.Map;

import org.apache.hadoop.io.retry.RetryPolicy;
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
public class DefaultProxy {
  public static Object create(Class<?> iface, Object implementation,
                              RetryPolicy retryPolicy) {
    return Proxy.newProxyInstance(
                                  implementation.getClass().getClassLoader(),
                                  new Class<?>[] { iface },
                                  new DefaultInvocationHandler(implementation, retryPolicy)
                                  );
  }  
  
  public static Object create(Class<?> iface, Object implementation,
                              Map<String,RetryPolicy> methodNameToPolicyMap) {
    return Proxy.newProxyInstance(
                                  implementation.getClass().getClassLoader(),
                                  new Class<?>[] { iface },
                                  new DefaultInvocationHandler(implementation, methodNameToPolicyMap)
                                  );
  }
}

