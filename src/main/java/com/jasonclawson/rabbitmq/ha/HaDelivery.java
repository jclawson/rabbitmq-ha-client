package com.jasonclawson.rabbitmq.ha;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

public class HaDelivery {
	private final HaEnvelope _envelope;
    private final AMQP.BasicProperties _properties;
    private final byte[] _body;

    public HaDelivery(HaEnvelope envelope, AMQP.BasicProperties properties, byte[] body) {
        _envelope = envelope;
        _properties = properties;
        _body = body;
    }

    /**
     * Retrieve the message envelope.
     * @return the message envelope
     */
    public HaEnvelope getEnvelope() {
        return _envelope;
    }

    /**
     * Retrieve the message properties.
     * @return the message properties
     */
    public BasicProperties getProperties() {
        return _properties;
    }

    /**
     * Retrieve the message body.
     * @return the message body
     */
    public byte[] getBody() {
        return _body;
    }
}
