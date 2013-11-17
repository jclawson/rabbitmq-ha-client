package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;

public class ChannelMismatchException extends IOException {
	private static final long serialVersionUID = 1L;

	public ChannelMismatchException(long channelId, long expectedChannelId, String operation) {
		super("Given internal channel id "+channelId+" instead of the expected "+expectedChannelId+". Cannot execute "+operation);
	}
}
