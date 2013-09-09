
package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;


public final class HaUtils {
    private HaUtils() {}

    public static boolean shouldReconnect(Throwable e) {
    	if(e == null)
    		return false;
    	
    	if(e instanceof ConnectException) {
    		//rabbit server is not up
    		return true;
    	}
    	
    	if(e instanceof ShutdownSignalException) {
    		/*
    		 * If we didn't initiate the shutdown, or we encounter a connection error... we should reconnect
    		 */
    		return !((ShutdownSignalException) e).isInitiatedByApplication()
    			|| ((ShutdownSignalException)e).isHardError();
    	}
    	
    	if(e instanceof ConsumerCancelledException) {
    		//if the server cancelled our consumer, we should reconnect
    		return true;
    	}
    	
    	if(e instanceof IOException) {
    		return shouldReconnect(e.getCause());    		
    	}
    	
    	return false;
    }
    
    public static Object invokeAnUnwrapException(final Method method, final Object[] args, final Object target) throws Throwable {
        try {
        	return method.invoke(target, args);
        } catch (InvocationTargetException ite) {
        	throw ite.getTargetException();
        }
    }
    
}
