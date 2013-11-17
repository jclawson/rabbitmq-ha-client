package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.rabbitmq.client.Address;

public interface HaConnectionFactoryPruned {

	public abstract HaConnection newConnection(ExecutorService executor,
			Address[] addrs) throws IOException;

	public abstract HaConnection newConnection(Address[] addrs)
			throws IOException;

	public abstract HaConnection newConnection() throws IOException;

	public abstract HaConnection newConnection(ExecutorService executor)
			throws IOException;

}