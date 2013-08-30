package com.jasonclawson;

import java.io.IOException;
import java.lang.reflect.Method;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

@RequiredArgsConstructor
@Slf4j
public class HaConsumerProxy implements Consumer {

	private final Consumer delegateConsumer;
	private final HaChannelProxy haChannel;
	private final Method basicConsumeMethod;
	private final Object[] basicConsumeArgs;

	protected Object reconsume() throws IOException {
		try {
            //if I reconsume... I need to clear my consumer
			if(delegateConsumer instanceof HaQueuingConsumer) {
				((HaQueuingConsumer) delegateConsumer).reset();
			}
			return haChannel.invoke(haChannel, basicConsumeMethod, basicConsumeArgs);
        } catch (Error e) {
			//bubble errors, like OOM up
			throw e;
		} catch (ShutdownSignalException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		} catch (Throwable e) {
			throw new IOException("Error reconsuming ", e);
		}
	}

	/**
	 * I am wondering if I need to do anything here. The connection shutdown listener SHOULD get this signal 
	 * and handle it there. I am thinking I don't have to do anything...
	 */
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		/*
		 * This will add POISON to a Queueing consumer local queue buffer causing a ShutdownSignalException
		 * to be thrown when the application code asks for the queue entry. This is probably ok and desierable.
		 * We don't want to comment this out, because a ShutdownSignal could have been initiated by the application
		 * via a close() call.
		 */
		log.debug("Consumer {} proxy recieved a Shutdown Signal {}. {}", consumerTag, sig.isInitiatedByApplication() ? "initiated by the app" : "not initiated by the app", sig.toString());
		log.debug(""+HaUtils.shouldReconnect(sig));
		if(HaUtils.shouldReconnect(sig)) {
			try {
				haChannel.askConnectionToReconnect();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log.warn("Thread was interrupted");
				throw new RuntimeException(e);
			}
		} else {
			//delegateConsumer.handleShutdownSignal(consumerTag, sig);
		}
	}
	
	
	//all delegate methods ----------------------
	
	public void handleConsumeOk(String consumerTag) {
		delegateConsumer.handleConsumeOk(consumerTag);
	}

	public void handleCancelOk(String consumerTag) {
		delegateConsumer.handleCancelOk(consumerTag);
	}

	public void handleCancel(String consumerTag) throws IOException {		
		log.debug("Consumer proxy recieved a cancel for consumer tag {}. I will try to reconnect", consumerTag);
		try {
			haChannel.askConnectionToReconnect();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.warn("Thread was interrupted");
			throw new RuntimeException(e);
		}
	}

	public void handleDelivery(String consumerTag, Envelope envelope,
			BasicProperties properties, byte[] body) throws IOException {
		delegateConsumer
				.handleDelivery(consumerTag, envelope, properties, body);
	}

	

	public void handleRecoverOk(String consumerTag) {
		delegateConsumer.handleRecoverOk(consumerTag);
	}

	
	
	
	
}
