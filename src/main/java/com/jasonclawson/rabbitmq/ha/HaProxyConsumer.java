package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;

import lombok.Delegate;
import lombok.RequiredArgsConstructor;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

@RequiredArgsConstructor
public class HaProxyConsumer implements Consumer {

	private final long internalChannelId;
	@Delegate
	private final HaConsumer delegate;	

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
		HaEnvelope haEnvelope = new HaEnvelope(internalChannelId, envelope);
		delegate.handleDelivery(consumerTag, haEnvelope, properties, body);
	}
}
