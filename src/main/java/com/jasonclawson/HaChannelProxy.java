package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownSignalException;

@Slf4j
@EqualsAndHashCode(of="haChannelId")
public class HaChannelProxy implements InvocationHandler {
	
	private final UUID haChannelId = UUID.randomUUID();
	
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
    
    private final ConcurrentHashMap<Consumer, HaConsumerProxy> consumerProxies = new ConcurrentHashMap<Consumer, HaConsumerProxy>();
    private final Map<Method, Object[]> callsToReplay = new LinkedHashMap<Method, Object[]>();
    
	private final HaConnectionProxy haConnection;
	private volatile Channel channelDelegate;
	private volatile long connectionId;
	
	private int channelNumber;
	
	public HaChannelProxy(HaConnectionProxy connection, Channel channelDelegate) {
		this.haConnection = connection;
		this.channelDelegate = channelDelegate;
		this.connectionId = connection.getConnectionId();
		this.channelNumber = channelDelegate.getChannelNumber();
	}
	
	
	protected synchronized void reconnect(long connectionId, Connection connection) throws IOException {
		if (log.isDebugEnabled() && this.channelDelegate != null) {
			log.debug("Replacing channel: channel=" + this.channelDelegate.toString());
		}
		
//     closing the channel that has been disconnected can take a while to timeout
//		we probably don't need to do this because the connection is closed.
//		if(channelDelegate.isOpen()) {
//			try {
//				channelDelegate.close();
//			} catch (Exception e) {
//				log.warn("Unable to close delegate channel", e);
//			}
//		}
		
		Channel channel = connection.createChannel(channelNumber);	
		this.channelDelegate = channel;
		reconsume(connectionId);
		
		if (log.isDebugEnabled() && this.channelDelegate != null) {
			log.debug("Replaced channel: channel=" + this.channelDelegate.toString());
		}
	}
	
	/**
	 * When the connection is refreshed, we have to reconsume all our queues
	 * 
	 * @param connectionId
	 * @throws IOException 
	 */
	private void reconsume(long connectionId) throws IOException {
		this.connectionId = connectionId;
		for (Entry<Method, Object[]> callToReplay : callsToReplay.entrySet()) {
			try {
				if(log.isDebugEnabled()) {
					log.debug("Replaying call to {} on new channel", callToReplay.getKey().getName());
				}
				HaUtils.invokeAnUnwrapException(callToReplay.getKey(), callToReplay.getValue(), channelDelegate);
			} catch (Error e) {
				//bubble errors, like OOM up
				throw e;
			} catch (ShutdownSignalException e) {
				throw e;
			} catch (IOException e) {
				throw e;
			} catch (Throwable e) {
				throw new IOException("Error replaying call", e);
			}
		}

		for(HaConsumerProxy consumer : this.consumerProxies.values()) {
			consumer.reconsume();
		}

	}


	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (log.isDebugEnabled()) {
            log.debug("Invoke: " + method.getName());
        }
		
		//close() call
		if (method.getName().equals(CLOSE_METHOD_NAME)) {
            try {
            	channelDelegate.close();
            } catch (Exception e) {
            	//this is probably ok
            	log.debug("Error while trying to close underlying channel" + e.getMessage());
            }
            haConnection.removeClosedChannel(this);
            return null;
        }
		

		//if consume method is being called, wrap the incoming consumer with a proxy
		adjustArgsIfBasicConsume(proxy, method, args);		
		
		//FIXME: add max tries
		int tries = 0;
		while(true) {		
			tries++;
			try {
				Object result = null;
				
				if(1 == tries || RETRYABLE_METHOD_NAMES.contains(method.getName())) {
					if(!this.channelDelegate.isOpen()) {
						log.error("Channel {} is closed! channel: {} connection: {} ", channelDelegate.getChannelNumber(), System.identityHashCode(channelDelegate), System.identityHashCode(channelDelegate.getConnection()));
					}
					log.debug("invoking {}", method.getName());
					result = HaUtils.invokeAnUnwrapException(method, args, channelDelegate);
				}
				
				//this comes after because if we are waiting on the latch
				//another thread will call reconsume() which will execute the REPLAYABLE_METHOD_NAMES
				//and then this loop will run again, calling <code>method</code>
				if (REPLAYABLE_METHOD_NAMES.contains(method.getName())) {
		            callsToReplay.put(method, args);
		        }
				
				/*
				 * This will happen if we call waitForConfirms and we reconnect
				 */
				if(result == null 
						&& !RETRYABLE_METHOD_NAMES.contains(method.getName())
						&& method.getReturnType() != null
						&& method.getReturnType().isPrimitive()) {
					throw new IOException("Tried to execute non-retryable method "+method.getName()+", but we reconnected.");
				}
				
				return result;
			} catch (Exception e) {
				log.debug("invoke hit an exception {}", e.toString());
				if(HaUtils.shouldReconnect(e)) {
					askConnectionToReconnect();
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
	private void adjustArgsIfBasicConsume(Object proxy, Method method, Object[] args) {
		if (method.getName().equals(BASIC_CONSUME_METHOD_NAME)) {
			// Consumer is always the last argument, let it fail if not
			Consumer targetConsumer = (Consumer) args[args.length - 1];
			// already wrapped?
			if (!(targetConsumer instanceof HaConsumerProxy)) {
				// check to see if we already have a proxied Consumer
				HaConsumerProxy consumerProxy = consumerProxies.get(targetConsumer);
				if (consumerProxy == null) {
					consumerProxy = new HaConsumerProxy(targetConsumer, this, method, args);
				}
	
				// currently we think there is not a proxy
				// try to do this atomically and worse case someone else already created one
				HaConsumerProxy existingConsumerProxy = consumerProxies.putIfAbsent(targetConsumer,
						consumerProxy);
	
				// replace with the proxy for the real invocation
				args[args.length - 1] = existingConsumerProxy == null 
											? consumerProxy
											: existingConsumerProxy;
				
				
			}
		}
	}


	public void askConnectionToReconnect() throws InterruptedException {
		long currentConnectionId = this.connectionId;
		haConnection.reconnect(currentConnectionId); //then try and reconnect
	}
}
