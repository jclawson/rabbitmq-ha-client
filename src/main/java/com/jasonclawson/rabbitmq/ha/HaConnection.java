package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Delegate;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

@Slf4j
public class HaConnection {
	public static final int DISCONNECTED = 0;
	public static final int CONNECTING   = 1;
	public static final int CONNECTED    = 2;
	
	private AtomicInteger reconnectionState = new AtomicInteger(CONNECTED);
	
	@Delegate(excludes=PrunedConnection.class)
	private Connection delegateConnection;
	
	private final Set<HaChannelProxy> haChannels;
	private final ReentrantLock reconnectLock;
	private final ReconnectionFactory reconnectionFactory;
	private final long reconnectDelay;
	private final long maxReconnectTries;
	private final ShutdownListener shutdownListener = new ShutdownListener() {
		@Override
		public void shutdownCompleted(ShutdownSignalException cause) {
			log.warn("Shutdown listener called with ", cause);
			if(HaUtils.shouldReconnect(cause)) {
				try {
					log.warn("Shutdown listener is reconnecting connection {} due to ShutdownSignalException", cause);
					reconnect();
				} catch (InterruptedException e) {
					log.warn("Shutdown listener reconnect interrupted");
					Thread.currentThread().interrupt();
				}
			}
		}
	};
	
	public HaConnection(ReconnectionFactory reconnectionFactory, final Connection target, long reconnectDelay, long maxReconnectTries) {
		this.delegateConnection = target;
        this.reconnectionFactory = reconnectionFactory;
        this.maxReconnectTries = maxReconnectTries;
        this.reconnectDelay = reconnectDelay;
        reconnectLock = new ReentrantLock();

        //this must be a concurrent collection because other threads may remove from it
        //when a channel is closed
        haChannels = Collections.newSetFromMap(new ConcurrentHashMap<HaChannelProxy, Boolean>());
	}
	
    public HaChannel createChannel() throws IOException {
		Channel delegate = delegateConnection.createChannel();
		return proxyChannel(delegate);
	}

    public HaChannel createChannel(int channelNumber) throws IOException {
    	Channel delegate = delegateConnection.createChannel(channelNumber);
    	return proxyChannel(delegate);
    }
    
    protected Channel createDelegateChannel(int channelNumber) throws IOException {
    	return delegateConnection.createChannel(channelNumber);
    }
    
    private HaChannel proxyChannel(Channel delegate) {
    	HaChannelImpl channel = new HaChannelImpl(delegate);
    	
    	ClassLoader classLoader = HaChannel.class.getClassLoader();
        Class<?>[] interfaces = { HaChannel.class };
        
        HaChannelProxy proxy = new HaChannelProxy(this, channel);
        haChannels.add(proxy);
        
        return (HaChannel) Proxy.newProxyInstance(classLoader, interfaces, proxy);
    }
    
    public void removeClosedChannel(HaChannelProxy haChannelProxy) {
		haChannels.remove(haChannelProxy);
	}

	public void reconnect() throws InterruptedException {
		reconnectionState.compareAndSet(CONNECTED, DISCONNECTED);
		
		log.info("Thread '{}' is asking to reconnect", Thread.currentThread().getName());
		reconnectLock.lockInterruptibly();
		try {
			if(!reconnectionState.compareAndSet(DISCONNECTED, CONNECTING)) {
				log.info("Thread {} will not reconnect because we are in connection state {} (Not the DISCONNECTED state)", reconnectionState.get());
			} else /*if(!this.delegateConnection.isOpen())*/ {
				log.info("Thread '{}' is reconnecting", Thread.currentThread().getName());
				
				try {
					if(this.delegateConnection.isOpen()) {
						log.warn("I am reconnecting, but my connection appears to be open! I will close it.");
						this.delegateConnection.removeShutdownListener(shutdownListener);
						this.delegateConnection.close(5000);
					}
				} catch (Exception e) {
					log.warn("An error ocurred while trying to close the existing connection delegate", e);
				}
				
				Exception reconnectException = null;
				int tryNumber = 0;
				do {
	
					if(Thread.interrupted()) {
						throw new InterruptedException("Connection reconnect process interrupted after "+tryNumber+" tries");
					}
					
					Thread.sleep(reconnectDelay); //wait a little before attempting the reconnection
	
					try {
						log.debug("Attempting to connect");
						Connection connection = reconnectionFactory.newConnection();
						log.debug(connection.isOpen()?"I am connected":"I am not connected!!!");
						try {
							//we have this loop so we don't call reconnectChannels() without ensuring
							//our connection is valid
							this.delegateConnection = connection;
							applyConnectionShutdownListener();
							reconnectChannels();
							reconnectException = null;
						} catch (Exception e) {
							try {
								connection.close(2000);
							} catch (Exception e2) {
								log.error("Got an error reconnecting channels and I am unable to close the connection", e2);
							}
							
							log.error("Unable to reconnect... I will try {} more times", maxReconnectTries-tryNumber, e);
							reconnectException = e;
						}
						
					} catch (Exception e) {
						log.debug("Unable to connect {}", e.getMessage());
						reconnectException = e;
					}
	
				} while (++tryNumber < maxReconnectTries && (!this.delegateConnection.isOpen() || (reconnectException != null && HaUtils.shouldReconnect(reconnectException))));
	
				if(tryNumber >= maxReconnectTries) {
					log.error("Max reconnect tries exceeded!");
					throw new RuntimeException("Max reconnect tries, "+maxReconnectTries+", exceeded");
				}
			}/* else {
				log.warn("Connection is still open! I will not reconnect!!!");
			}*/
		} catch (InterruptedException e) {
			reconnectionState.compareAndSet(CONNECTING, DISCONNECTED);
			throw e;
		} catch (RuntimeException e) {
			reconnectionState.compareAndSet(CONNECTING, DISCONNECTED);
			throw e;
		} finally {
			reconnectionState.compareAndSet(CONNECTING, CONNECTED);
			reconnectLock.unlock();
		} 
	}
	
	private void reconnectChannels() throws IOException {
		log.debug("Reconnecting {} channels", haChannels.size());
		for(HaChannelProxy channel : haChannels) {
			channel.reconnect(this);
		}
	}
	
	private void applyConnectionShutdownListener() {
		this.delegateConnection.addShutdownListener(shutdownListener);
	}
}
