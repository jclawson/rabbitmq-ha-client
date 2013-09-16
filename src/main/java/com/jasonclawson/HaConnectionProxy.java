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
import java.util.concurrent.locks.ReentrantLock;

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
	private final ReentrantLock reconnectLock;
	private final HaConnectionFactory factory;
	private final ExecutorService executor;
	private final long reconnectDelay;
	private final long maxReconnectTries;
	
	@Getter
	private volatile long connectionId = 1;
	
	private static Method CREATE_CHANNEL_METHOD;
    private static Method CREATE_CHANNEL_INT_METHOD;
	
	public HaConnectionProxy(HaConnectionFactory factory, ExecutorService executor, final Address[] addrs, final Connection target, long reconnectDelay, long maxReconnectTries) {

        assert addrs != null;
        assert addrs.length > 0;

        this.delegateConnection = target;
        this.addresses = addrs;
        this.executor = executor;
        this.factory = factory;
        this.maxReconnectTries = maxReconnectTries;
        this.reconnectDelay = reconnectDelay;
        reconnectLock = new ReentrantLock();

        //this must be a concurrent collection because other threads may remove from it
        //when a channel is closed
        haChannels = Collections.newSetFromMap(new ConcurrentHashMap<HaChannelProxy, Boolean>());
        
        applyConnectionShutdownListener();
    }
	
	/**
	 * Refreshes the underlying connection. All channel threads will come here until we reconnect. After that, we 
	 * let the threads go but they will all bail out on the connectionId comparison check.
	 * 
	 * @param currentConnectionId the connectionId as the channel saw it when it decided it needed to reconnect
	 * @throws InterruptedException 
	 */
	public void reconnect(long currentConnectionId) throws InterruptedException {
		reconnectLock.lock();			
		try {
			/*
			 * This will stop late comers from reconnecting
			 */
			if(currentConnectionId < connectionId) {
				log.warn("Hit race condition on reconnect. I will not attempt the reconnect.");
				return;
			}

			try {
				if(this.delegateConnection.isOpen())
					this.delegateConnection.close(1000);
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

			} while (++tryNumber < maxReconnectTries && (!this.delegateConnection.isOpen() || (reconnectException != null && HaUtils.shouldReconnect(reconnectException))));

			if(tryNumber == maxReconnectTries) {
				log.error("Max reconnect tries exceeded!");
			}

		} finally {
			reconnectLock.unlock();
		}
	}
	
	private void applyConnectionShutdownListener() {
		this.delegateConnection.addShutdownListener(new ShutdownListener() {
			@Override
			public void shutdownCompleted(ShutdownSignalException cause) {
				if(HaUtils.shouldReconnect(cause)) {
					try {
						reconnect(connectionId);
					} catch (InterruptedException e) {
						log.warn("Shutdown listener reconnect interrupted");
						Thread.currentThread().interrupt();
					}
				}
			}
		});
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
        // initialize cache for these methods so we don't have to do a lot of reflection
        try {
            CREATE_CHANNEL_METHOD = Connection.class.getMethod("createChannel");
            CREATE_CHANNEL_INT_METHOD = Connection.class.getMethod("createChannel", int.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
