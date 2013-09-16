package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@Slf4j
public class HaConnectionFactory extends ConnectionFactory {
	
	@Getter
	@Setter
	private long reconnectDelay = 1500;
	
	@Getter
	@Setter
	private long maxReconnectTries = 5000;
	
	public HaConnectionFactory() {
		
	}
	
	protected Connection createConnectionProxyInstance(ExecutorService executor, final Address[] addrs, final Connection targetConnection) {

        ClassLoader classLoader = Connection.class.getClassLoader();
        Class<?>[] interfaces = { Connection.class };

        HaConnectionProxy proxy = new HaConnectionProxy(this, executor, addrs, targetConnection, reconnectDelay, maxReconnectTries);

        if (log.isDebugEnabled()) {
            log.debug("Creating connection proxy: "
                            + (targetConnection == null ? "none" : targetConnection.toString()));
        }

        return (Connection) Proxy.newProxyInstance(classLoader, interfaces, proxy);
    }
	
	protected Connection newDelegateConnection(ExecutorService executor, Address[] addrs) throws IOException {
		return super.newConnection(executor, addrs);
	}
	
	@Override
    public Connection newConnection(ExecutorService executor, Address[] addrs) throws IOException {
		Connection target = null;
		int tries = 0;
		while(target == null && tries++ < maxReconnectTries) {
			try {
				if(Thread.interrupted()) {
					Thread.currentThread().interrupt();
					throw new InterruptedException("Connection process interrupted after "+tries+" tries");
				}
	        	target = newDelegateConnection(executor, addrs);
	        } catch (Exception e) {
	        	if(e instanceof IOException && !HaUtils.shouldReconnect(e)) {
	        		throw (IOException)e;
	        	} else if (! (e instanceof IOException)) {
	        		throw new IOException("Unable to connect to RabbitMQ", e);
	        	}
	        	
	        	try {
					Thread.sleep(reconnectDelay);
				} catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					throw new RuntimeException("Connect process was interrupted");
				}
	        	log.warn("Unable to connect to RabbitMQ... trying again...");
	        }
		}
		
		if(target == null) {
			throw new RuntimeException("Unable to connect to RabbitMQ. Gave up after "+tries+" tries.");
		}
		
        return createConnectionProxyInstance(executor, addrs, target);
    }
	

	@Override
	public Connection newConnection(Address[] addrs) throws IOException {
		return this.newConnection(null, addrs);
	}

	@Override
	public Connection newConnection() throws IOException {
		return this.newConnection(null, null);
	}

	@Override
	public Connection newConnection(ExecutorService executor) throws IOException {
		return this.newConnection(executor, null);
	}
	
	
	
}
