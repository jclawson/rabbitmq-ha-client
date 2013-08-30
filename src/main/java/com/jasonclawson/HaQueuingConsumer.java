package com.jasonclawson;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;

/**
 * You should use this QueueingConsumer instead of the one that comes with Rabbit. This consumer 
 * ensures proper handling of the locally cached messages on reconnect. (They get cleared out in 
 * preparation for Rabbit to redeliver them)
 * @author jclawson
 *
 */
public class HaQueuingConsumer extends DefaultConsumer {

	private final LinkedBlockingQueue<Delivery> queue = new LinkedBlockingQueue<Delivery>();
	private volatile ShutdownSignalException shutdown;
	private volatile ConsumerCancelledException cancelled;

	public HaQueuingConsumer(Channel channel) {
		super(channel);
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
		cancelled = new ConsumerCancelledException();
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope,
			AMQP.BasicProperties properties, byte[] body) throws IOException {
		checkShutdown();
		this.queue.add(new Delivery(envelope, properties, body));
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		shutdown = sig;
	}
	
	private void checkShutdown() {
        if (shutdown != null)
            throw Utility.fixStackTrace(shutdown);
    }
	
	private Delivery handle(Delivery delivery) {
        if(shutdown != null || cancelled != null) {
	        if (null != shutdown)
	            throw Utility.fixStackTrace(shutdown);
	        if (null != cancelled)
	            throw Utility.fixStackTrace(cancelled);
        }
        return delivery;
    }

    /**
     * Main application-side API: wait for the next message delivery and return it.
     * @return the next message
     * @throws InterruptedException if an interrupt is received while waiting
     * @throws ShutdownSignalException if the connection is shut down while waiting
     * @throws ConsumerCancelledException if this consumer is cancelled while waiting
     */
    public Delivery nextDelivery()
        throws InterruptedException, ShutdownSignalException, ConsumerCancelledException
    {
        return handle(queue.take());
    }

    /**
     * Main application-side API: wait for the next message delivery and return it.
     * @param timeout timeout in millisecond
     * @return the next message or null if timed out
     * @throws InterruptedException if an interrupt is received while waiting
     * @throws ShutdownSignalException if the connection is shut down while waiting
     * @throws ConsumerCancelledException if this consumer is cancelled while waiting
     */
    public Delivery nextDelivery(long timeout)
        throws InterruptedException, ShutdownSignalException, ConsumerCancelledException
    {
        return handle(queue.poll(timeout, TimeUnit.MILLISECONDS));
    }
    
    public void reset() {
    	queue.clear();
    	this.shutdown = null;
    	this.cancelled = null;
    }

}
