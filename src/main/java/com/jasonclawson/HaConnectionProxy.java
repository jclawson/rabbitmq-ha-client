package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

@Slf4j
public class HaConnectionProxy implements InvocationHandler {
	private final Address[] addresses;
	private Connection delegateConnection;
	private final Set<HaChannelProxy> haChannels;
	private final ReentrantReadWriteLock reconnectLock;
	private final HaConnectionFactory factory;
	private final ExecutorService executor;
	
	@Getter
	private volatile long connectionId = 1;
	
	public static final int MAX_RECONNECT_TRIES = 100;
	
	private static Method CREATE_CHANNEL_METHOD;
    private static Method CREATE_CHANNEL_INT_METHOD;
	
	public HaConnectionProxy(HaConnectionFactory factory, ExecutorService executor, final Address[] addrs, final Connection target) {

        assert addrs != null;
        assert addrs.length > 0;
        //assert retryStrategy != null;

        this.delegateConnection = target;
        this.addresses = addrs;
        this.executor = executor;
        this.factory = factory;
        //this.retryStrategy = retryStrategy;
        reconnectLock = new ReentrantReadWriteLock();

        //this must be a concurrent collection because other threads may remove from it
        //when a channel is closed
        haChannels = Collections.newSetFromMap(new ConcurrentHashMap<HaChannelProxy, Boolean>());
        
        applyConnectionShutdownListener();
    }
	
	/**
	 * Refreshes the underlying connection
	 */
	public void reconnect(long currentConnectionId) {
		if(true) {
			reconnectLock.writeLock().lock();			
			try {
				/*
				 * This will stop late comers from reconnecting
				 */
				if(currentConnectionId < connectionId) {
					log.warn("Hit race condition on reconnect. I will not attempt the reconnect.");
					return;
				}
				closeChannelLatches();
				try {
					if(this.delegateConnection.isOpen())
						this.delegateConnection.close(1000);
				} catch (Exception e) {
					log.warn("An error ocurred while trying to close the existing connection delegate", e);
				}
				
				Exception reconnectException = null;
				int tryNumber = 0;
				do {
					
					try {
						Thread.sleep(1500); //wait a little before attempting the reconnection
					} catch (InterruptedException e) {
						log.warn("Reconnect process was interrupted");
						Thread.currentThread().interrupt();
						return;
					}
					
					try {
						//we have this loop so we don't call reconnectChannels() without ensuring
						//our connection is valid
						Connection connection = this.factory.newDelegateConnection(executor, addresses);
						this.delegateConnection = connection;
						this.connectionId++;
						applyConnectionShutdownListener();
						reconnectChannels();
					} catch (Exception e) {
						log.error("Unable to reconnect... I will try again", e);
						reconnectException = e;
					}
				
				} while (++tryNumber < MAX_RECONNECT_TRIES && (!this.delegateConnection.isOpen() || (reconnectException != null && HaUtils.shouldReconnect(reconnectException))));
				
				if(tryNumber == MAX_RECONNECT_TRIES) {
					log.error("Max reconnect tries exceeded!");
				}
				
			} finally {
				try {
					//ensure we don't leave the channel latches closed even if something
					//goes wrong.. opening the latches will allow us to try again
					openChannelLatches();
				} finally {
					reconnectLock.writeLock().unlock();
				}
				
			}
		}
	}
	
	private void applyConnectionShutdownListener() {
		this.delegateConnection.addShutdownListener(new ShutdownListener() {
			@Override
			public void shutdownCompleted(ShutdownSignalException cause) {
				if(HaUtils.shouldReconnect(cause)) {
					reconnect(connectionId);
				}
			}
		});
	}
	
	private void closeChannelLatches() {
		for(HaChannelProxy channel : haChannels) {
			channel.closeLatch();
		}
	}
	
	private void openChannelLatches() {
		for(HaChannelProxy channel : haChannels) {
			channel.openLatch();
		}
	}
	
	private void reconnectChannels() throws IOException {
		for(HaChannelProxy channel : haChannels) {
			channel.reconnect(connectionId, delegateConnection);
		}
	}
	
	public Connection getConnectionDelegate() {
		return delegateConnection;
	}

	public void removeClosedChannel(HaChannelProxy haChannel) {
		haChannels.remove(haChannel);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		// intercept calls to create a channel
        if (method.equals(CREATE_CHANNEL_METHOD) || method.equals(CREATE_CHANNEL_INT_METHOD)) {

            return createChannelAndWrapWithProxy(method, args);
        }

        // delegate all other method invocations
        return HaUtils.invokeAnUnwrapException(method, args, delegateConnection);
	}
	
	protected Channel createChannelAndWrapWithProxy(final Method method, final Object[] args)
            throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {

        Channel targetChannel = (Channel) method.invoke(delegateConnection, args);

        ClassLoader classLoader = Connection.class.getClassLoader();
        Class<?>[] interfaces = { Channel.class };

        // create the proxy and add to the set of channels we have created
        HaChannelProxy proxy = new HaChannelProxy(this, targetChannel);

        if (log.isDebugEnabled()) {
            log.debug("Creating channel proxy: " + targetChannel.toString());
        }

        haChannels.add(proxy);

        return (Channel) Proxy.newProxyInstance(classLoader, interfaces, proxy);
    }
	
	static {
        // initialize cache for these methods
        try {
            CREATE_CHANNEL_METHOD = Connection.class.getMethod("createChannel");
            CREATE_CHANNEL_INT_METHOD = Connection.class.getMethod("createChannel", int.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
