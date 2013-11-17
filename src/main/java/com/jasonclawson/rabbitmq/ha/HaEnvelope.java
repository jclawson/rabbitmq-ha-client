package com.jasonclawson.rabbitmq.ha;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import com.rabbitmq.client.Envelope;

@Data
public class HaEnvelope {
	@Getter
	private DeliveryTag deliveryTag;
	
	@Getter(AccessLevel.PROTECTED)
	private Envelope internalEnvelope;
	
	public HaEnvelope(long internalChannelId, Envelope internalEnvelope) {
		this.deliveryTag = new DeliveryTag(internalChannelId, internalEnvelope.getDeliveryTag());
		this.internalEnvelope = internalEnvelope;
	}

	public boolean isRedeliver() {
		return internalEnvelope.isRedeliver();
	}

	public String getExchange() {
		return internalEnvelope.getExchange();
	}

	public String getRoutingKey() {
		return internalEnvelope.getRoutingKey();
	}
}
