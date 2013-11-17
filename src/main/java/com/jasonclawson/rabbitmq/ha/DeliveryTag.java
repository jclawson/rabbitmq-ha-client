package com.jasonclawson.rabbitmq.ha;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class DeliveryTag {
	private final long internalChannelId;
	private final long deliveryTag;
}
