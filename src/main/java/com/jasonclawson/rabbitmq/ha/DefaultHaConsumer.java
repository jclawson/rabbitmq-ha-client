package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownSignalException;

public class DefaultHaConsumer implements HaConsumer {
	/** Channel that this consumer is associated with. */
    private final HaChannel _channel;
    /** Consumer tag for this consumer. */
    private volatile String _consumerTag;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     */
    public DefaultHaConsumer(HaChannel channel) {
        _channel = channel;
    }

    /**
     * Stores the most recently passed-in consumerTag - semantically, there should be only one.
     * @see Consumer#handleConsumeOk
     */
    public void handleConsumeOk(String consumerTag) {
        this._consumerTag = consumerTag;
    }

    /**
     * No-op implementation of {@link Consumer#handleCancelOk}.
     * @param consumerTag the defined consumer tag (client- or server-generated)
     */
    public void handleCancelOk(String consumerTag) {
        // no work to do
    }

    /**
     * No-op implementation of {@link Consumer#handleCancel(String)}
     * @param consumerTag the defined consumer tag (client- or server-generated)
     */
    public void handleCancel(String consumerTag) throws IOException {
        // no work to do
    }

    /**
     * No-op implementation of {@link Consumer#handleShutdownSignal}.
     */
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        // no work to do
    }

     /**
     * No-op implementation of {@link Consumer#handleRecoverOk}.
     */
    public void handleRecoverOk(String consumerTag) {
        // no work to do
    }

    /**
     * No-op implementation of {@link Consumer#handleDelivery}.
     */
    public void handleDelivery(String consumerTag,
                               HaEnvelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body)
        throws IOException
    {
            // no work to do
    }

    /**
    *  Retrieve the channel.
     * @return the channel this consumer is attached to.
     */
    public HaChannel getChannel() {
        return _channel;
    }

    /**
    *  Retrieve the consumer tag.
     * @return the most recently notified consumer tag.
     */
    public String getConsumerTag() {
        return _consumerTag;
    }
}
