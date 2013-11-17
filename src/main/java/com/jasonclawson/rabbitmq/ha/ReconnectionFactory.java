package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import lombok.RequiredArgsConstructor;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;

@RequiredArgsConstructor
public class ReconnectionFactory {
	private final HaConnectionFactory factory; 
	private final ExecutorService executor;
	private final Address[] addresses;
	
	public Connection newConnection() throws IOException {
		return factory.newDelegateConnection(executor, addresses);
	}
}
