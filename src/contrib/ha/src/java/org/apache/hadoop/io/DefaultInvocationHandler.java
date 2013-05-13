package org.apache.hadoop.io;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.util.StringUtils;

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
class DefaultInvocationHandler implements InvocationHandler {
  public static final Log LOG = LogFactory.getLog(DefaultInvocationHandler.class);
  private Object implementation;
  
  private RetryPolicy defaultPolicy;
  private Map<String,RetryPolicy> methodNameToPolicyMap;
  
  private static final int RETRIES_COUNT=10;
  
  public DefaultInvocationHandler(Object implementation, RetryPolicy retryPolicy) {
    this.implementation = implementation;
    this.defaultPolicy = retryPolicy;
    this.methodNameToPolicyMap = Collections.emptyMap();
  }
  
  public DefaultInvocationHandler(Object implementation, Map<String, RetryPolicy> methodNameToPolicyMap) {
    this.implementation = implementation;
    this.defaultPolicy = RetryPolicies.TRY_ONCE_THEN_FAIL;
    this.methodNameToPolicyMap = methodNameToPolicyMap;
  }

  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    RetryPolicy policy = methodNameToPolicyMap.get(method.getName());
    if (policy == null) {
      policy = defaultPolicy;
    }
    
    int retries = 0;
    while (true) {
      try {
        return invokeMethod(method, args);
      } catch (Exception e) {
        if (!policy.shouldRetry(e, retries++)) {
          LOG.info("Exception while invoking " + method.getName()
                   + " of " + implementation.getClass() + ". Not retrying."
                   + StringUtils.stringifyException(e));
          if (!method.getReturnType().equals(Void.TYPE)) {
            throw e; // non-void methods can't fail without an exception
          }
          return null;
        }
        LOG.debug("Exception while invoking " + method.getName()
                 + " of " + implementation.getClass() + ". Retrying."
                 + StringUtils.stringifyException(e));
        if(retries==RETRIES_COUNT){
        	throw e;
        }
      }
    }
  }

  private Object invokeMethod(Method method, Object[] args) throws Throwable {
    try {
      if (!method.isAccessible()) {
        method.setAccessible(true);
      }
      return method.invoke(implementation, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

}
