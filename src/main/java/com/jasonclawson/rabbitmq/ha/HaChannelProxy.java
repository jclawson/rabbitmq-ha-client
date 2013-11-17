package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * This is used to proxy an instance of HaChannel in order to 
 * replay method calls when a connection is reconnected
 * 
 * @author jclawson
 */
@RequiredArgsConstructor
@Slf4j
public class HaChannelProxy implements InvocationHandler {

	
	
	private static final String BASIC_CONSUME_METHOD_NAME = "basicConsume";
    private static final String CLOSE_METHOD_NAME = "close";
	
	/**
     * When these methods are called, it will be recorded. If we have to reconnect, the calls will be replayed on the new channel
     */
    private static final Set<String> REPLAYABLE_METHOD_NAMES = new HashSet<String>(Arrays.asList("flow", "basicQos", "confirmSelect", "flow", "txSelect"));
    
    /**
     * These methods are expected to fail sometimes, can be called again with the same parameters on a new channel delegate
     * Methods like: basickAck cannot do this because it depends on channel state (delieverTag) 
     * 
     * Note: basicConsume is handled as part of the reconnect process
     */
    private static final Set<String> RETRYABLE_METHOD_NAMES = new HashSet<String>(Arrays.asList(
    		"basicPublish", "exchangeDeclare", "exchangeDelete", "exchangeBind", "exchangeUnbind",
    		"queueDeclare", "queueDeclarePassive", "queueDelete", "queueBind", "queueUnbind",
    		"queuePurge", "basicGet", "basicRecover", "getNextPublishSeqNo", "asyncRpc", "rpc"));
    
    private final HaConnection haConnection;
    private final HaChannelImpl channel;
    
    private ConcurrentHashMap<HaConsumer, HaConsumerProxy> consumerProxies = new ConcurrentHashMap<HaConsumer, HaConsumerProxy>();
    private Map<Method, Object[]> callsToReplay = new LinkedHashMap<Method, Object[]>();

    public long getInternalChannelId() {
    	return channel.getInternalChannelId();
    }
    
    protected synchronized void reconnect(HaConnection connection) throws IOException {
    	log.info("Reconnecting channel {}", this.channel.getInternalChannelId());
    	
    	Channel newDelegateChannel = connection.createDelegateChannel(channel.getChannelNumber());
    	this.channel.refreshChannelDelegate(newDelegateChannel);
    	reconsume();
    }
    
    private void reconsume() throws IOException {
    	log.debug("Reconsuming {} calls on new channel {}", callsToReplay.size(), this.channel.getInternalChannelId());
		for (Entry<Method, Object[]> callToReplay : callsToReplay.entrySet()) {
			try {
				if(log.isDebugEnabled()) {
					log.debug("Replaying call to {} on new channel", callToReplay.getKey().getName());
				}
				HaUtils.invokeAnUnwrapException(callToReplay.getKey(), callToReplay.getValue(), channel);
			} catch (Error e) {
				//bubble errors, like OOM up
				throw e;
			} catch (ShutdownSignalException e) {
				log.error("I got a ShutdownSignalException while attempting to replay the call to {}", callToReplay.getKey().getName(), e);
				throw e;
			} catch (IOException e) {
				log.error("I got an IOException while attempting to replay the call to {}", callToReplay.getKey().getName(), e);
				throw e;
			} catch (Throwable e) {
				throw new IOException("Error replaying call", e);
			}
		}

		log.debug("Re-binding {} consumers on channel", this.consumerProxies.size());
		for(HaConsumerProxy consumer : this.consumerProxies.values()) {
			consumer.reconsume();
		}
    }
    
	@Override
	public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {		
		if (method.getName().equals(CLOSE_METHOD_NAME)) {
			try {
				channel.close();
			} finally {
				haConnection.removeClosedChannel(this);
			}
		}
		
		//if consume method is being called, wrap the incoming consumer with a proxy
		adjustArgsIfBasicConsume(method, arguments);
		
		int tries = 0;
		while(true) {		
			tries++;
			try {
				Object result = null;
				
				if(1 == tries || RETRYABLE_METHOD_NAMES.contains(method.getName())) {
					if(!this.channel.isOpen()) {
						log.error("Channel {} is closed! channel: {} connection: {} ", channel.getChannelNumber(), System.identityHashCode(channel), System.identityHashCode(channel.getConnection()));
					}
					log.debug("invoking {} on internal channel {}", method.getName(), this.getInternalChannelId());
					result = HaUtils.invokeAnUnwrapException(method, arguments, channel);
				}
				
				//this comes after because if we are waiting on the latch
				//another thread will call reconsume() which will execute the REPLAYABLE_METHOD_NAMES
				//and then this loop will run again, calling <code>method</code>
				if (REPLAYABLE_METHOD_NAMES.contains(method.getName())) {
		            callsToReplay.put(method, arguments);
		        }
				
				/*
				 * This will happen if we call waitForConfirms and we reconnect
				 */
				if(result == null 
						&& !RETRYABLE_METHOD_NAMES.contains(method.getName())
						&& method.getReturnType() != null
						&& method.getReturnType().isPrimitive()
						&& method.getReturnType() != void.class) {
					throw new IOException("Tried to execute non-retryable method "+method.getName()+", but we reconnected.");
				}
				
				return result;
			} catch (Exception e) {
				log.warn("invoke hit an exception {}", e.toString());
				if(HaUtils.shouldReconnect(e)) {
					log.info("I will try to reconnect");
					haConnection.reconnect();
				} else {
					log.info("I will throw the exception {}", e.getMessage());
					throw e;
				}
			}
		}
	}
	
	/*
	 * This method was taken from Josh Devins rabbitmq-ha-client
	 * -----------------------------------------------------------
	 * Copyright 2010 Josh Devins
	 * 
	 * Licensed under the Apache License, Version 2.0
	 * 
	 * http://www.apache.org/licenses/LICENSE-2.0
	 * 
	 */
	private void adjustArgsIfBasicConsume(Method method, Object[] args) {
		if (method.getName().equals(BASIC_CONSUME_METHOD_NAME)) {
			// Consumer is always the last argument, let it fail if not
			HaConsumer targetConsumer = (HaConsumer) args[args.length - 1];
			// already wrapped?
			if (!(targetConsumer instanceof HaConsumerProxy)) {
				// check to see if we already have a proxied Consumer
				HaConsumerProxy consumerProxy = consumerProxies.get(targetConsumer);
				if (consumerProxy == null) {
					log.debug("wrapping targetConsumer in HaConsumerProxy");
					consumerProxy = new HaConsumerProxy(targetConsumer, this, method, args);
				} else {
					log.debug("targetConsumer already wrapped in HaConsumerProxy");
				}
	
				// currently we think there is not a proxy
				// try to do this atomically and worse case someone else already created one
				HaConsumerProxy existingConsumerProxy = consumerProxies.putIfAbsent(targetConsumer,
						consumerProxy);
	
				// replace with the proxy for the real invocation
				args[args.length - 1] = existingConsumerProxy == null 
											? consumerProxy
											: existingConsumerProxy;
				
				
			} else {
				log.info("targetConsumer was an instance of HaConsumerProxy");
			}
		}
	}

	public void askConnectionToReconnect() throws InterruptedException {
		haConnection.reconnect();
	}
	
}
