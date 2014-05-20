package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Delegate;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.Channel;

@Slf4j
public class HaChannelImpl implements HaChannel {
	private static AtomicLong HA_CHANNEL_ID_GENERATOR = new AtomicLong(0);
	
	//private final HaConnectionProxy haConnection;
	
	@Delegate(excludes=ChannelPruned.class)
	private volatile Channel channelDelegate;
	
	@Getter
	private volatile long internalChannelId;
	
	public HaChannelImpl(Channel channelDelegate) {
		internalChannelId = HA_CHANNEL_ID_GENERATOR.incrementAndGet();
		this.channelDelegate = channelDelegate;
	}
	
	protected void refreshChannelDelegate(Channel channelDelegate) {
		if(this.channelDelegate.isOpen()) {
			try {
				log.info("Reconnecting. Current channel appears to be open... but I will not close it because of a possible deadlock bug in the RabbitClient");
				//This can cause a deadlock in the RabbitMQ client when the client does a blocking call
				//to a server that is down. It waits forever and is uninterruptable
				//this.channelDelegate.close();
			} catch (Exception e) {
				log.warn("Unable to close delegate channel", e);
			}
		}
		
		//allows us to easily tell if the underlying channel is different than 
		//what we expect. We can fail fast on ack/nack operations
    	long oldChannelId = this.internalChannelId;
		internalChannelId = HA_CHANNEL_ID_GENERATOR.incrementAndGet();
		log.info("New internal channel id {} -> {}", oldChannelId, internalChannelId);
		this.channelDelegate = channelDelegate;
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicAck(com.jasonclawson.new2.DeliveryTag, boolean)
	 */
	@Override
	public void basicAck(DeliveryTag deliveryTag, boolean multiple) throws IOException {
		log.debug("Calling basicAck for {}", deliveryTag);
		this.assertValidOperationForChannel(deliveryTag, "basicAck");
		channelDelegate.basicAck(deliveryTag.getDeliveryTag(), multiple);
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicNack(com.jasonclawson.new2.DeliveryTag, boolean, boolean)
	 */
	@Override
	public void basicNack(DeliveryTag deliveryTag, boolean multiple, boolean requeue) throws IOException {
		log.debug("Calling basicNack for {}", deliveryTag);
		this.assertValidOperationForChannel(deliveryTag, "basicNack");
		channelDelegate.basicNack(deliveryTag.getDeliveryTag(), multiple, requeue);
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicReject(com.jasonclawson.new2.DeliveryTag, boolean)
	 */
	@Override
	public void basicReject(DeliveryTag deliveryTag, boolean requeue)
			throws IOException {
		log.debug("Calling basicReject for {}", deliveryTag);
		this.assertValidOperationForChannel(deliveryTag, "basicReject");
		channelDelegate.basicReject(deliveryTag.getDeliveryTag(), requeue);
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicConsume(java.lang.String, com.jasonclawson.HaConsumer)
	 */
	@Override
	public String basicConsume(String queue, HaConsumer callback) throws IOException {
		return channelDelegate.basicConsume(queue, new HaProxyConsumer(this.internalChannelId, callback));
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicConsume(java.lang.String, boolean, com.jasonclawson.HaConsumer)
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck, HaConsumer callback) throws IOException {
		return channelDelegate.basicConsume(queue, autoAck, new HaProxyConsumer(this.internalChannelId, callback));
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicConsume(java.lang.String, boolean, java.lang.String, com.jasonclawson.HaConsumer)
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, HaConsumer callback) throws IOException {
		
		return channelDelegate.basicConsume(queue, autoAck, consumerTag, 
				new HaProxyConsumer(this.internalChannelId, callback)
				);
	}

	/* (non-Javadoc)
	 * @see com.jasonclawson.new2.HaChannel#basicConsume(java.lang.String, boolean, java.lang.String, boolean, boolean, java.util.Map, com.jasonclawson.HaConsumer)
	 */
	@Override
	public String basicConsume(String queue, boolean autoAck,
			String consumerTag, boolean noLocal, boolean exclusive,
			Map<String, Object> arguments, HaConsumer callback)
			throws IOException {
		
		return channelDelegate.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, 
				new HaProxyConsumer(this.internalChannelId, callback)
				);
		
	}
	
	private void assertValidOperationForChannel(DeliveryTag deliveryTag, String operation) throws ChannelMismatchException {
		if(deliveryTag.getInternalChannelId() != internalChannelId) {
			throw new ChannelMismatchException(deliveryTag.getInternalChannelId(), internalChannelId, operation);
		}
	}
}
