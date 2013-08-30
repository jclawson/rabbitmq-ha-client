package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;

import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@Slf4j
public class HaConnectionFactory extends ConnectionFactory {
	
	public HaConnectionFactory() {
		
	}
	
	protected Connection createConnectionProxyInstance(ExecutorService executor, final Address[] addrs, final Connection targetConnection) {

        ClassLoader classLoader = Connection.class.getClassLoader();
        Class<?>[] interfaces = { Connection.class };

        HaConnectionProxy proxy = new HaConnectionProxy(this, executor, addrs, targetConnection);

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
		try {
        	target = newDelegateConnection(executor, addrs);
        } catch (IOException e) {
        	if(!HaUtils.shouldReconnect(e)) {
        		throw e;
        	}
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
