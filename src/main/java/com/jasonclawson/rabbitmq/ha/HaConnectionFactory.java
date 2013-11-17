package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import lombok.Delegate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@Slf4j
public class HaConnectionFactory {
	
	@Getter
	@Setter
	private long reconnectDelay = 1500;
	
	@Getter
	@Setter
	private long maxReconnectTries = 5000;
	
	@Delegate(excludes=HaConnectionFactoryPruned.class)
	private final ConnectionFactory delegate;
	
	public HaConnectionFactory() {
		delegate = new ConnectionFactory();
	}
	
	protected HaConnection createConnectionProxyInstance(ExecutorService executor, final Address[] addrs, final Connection targetConnection) {      
        ReconnectionFactory factory = new ReconnectionFactory(this, executor, addrs);
        return new HaConnection(factory, targetConnection, reconnectDelay, maxReconnectTries);
    }
	
	protected Connection newDelegateConnection(ExecutorService executor, Address[] addrs) throws IOException {
		return delegate.newConnection(executor, addrs);
	}
	
	public HaConnection newConnection(ExecutorService executor, Address[] addrs) throws IOException {
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
	
	public HaConnection newConnection(Address[] addrs) throws IOException {
		return this.newConnection(null, addrs);
	}

	public HaConnection newConnection() throws IOException {
		return this.newConnection(null, null);
	}

	public HaConnection newConnection(ExecutorService executor) throws IOException {
		return this.newConnection(executor, null);
	}	
}
