package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.Consumer;

/**
 * Just a little helper to make Lombok not delegate these methods
 * @author jclawson
 */
public interface ChannelPruned {
	public String basicConsume(String queue, Consumer callback) throws IOException;
	public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException;
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) throws IOException;
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException;
    public void basicAck(long deliveryTag, boolean multiple) throws IOException;
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException;
    public void basicReject(long deliveryTag, boolean requeue) throws IOException;
}
