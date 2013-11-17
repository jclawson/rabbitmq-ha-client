
package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQCommand;
import com.rabbitmq.client.impl.AMQImpl.Channel;


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
    		Object reason = ((ShutdownSignalException) e).getReason();
    		if(reason instanceof AMQCommand) {
    			com.rabbitmq.client.Method method = ((AMQCommand) reason).getMethod();
    			if(method instanceof Channel.Close) {
    				if(((Channel.Close) method).getReplyCode() == AMQP.PRECONDITION_FAILED) {
    					//this is the case when we declare a queue with different params than the one that exists
    					//TODO: have a configuration option to ignore the erroring method call and continue on
    					//      if the queue exists... who cares?
    					return false;
    				}
    			}
    		}
    		
    		/*
    		 * If we didn't initiate the shutdown, or we encounter a connection error... we should reconnect
    		 * isHardError returns true if it was a connection error... false if a channel error
    		 */
    		return !((ShutdownSignalException) e).isInitiatedByApplication();
    			/*((ShutdownSignalException)e).isHardError()*/
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
