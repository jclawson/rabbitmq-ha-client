package com.jasonclawson.rabbitmq.ha;

import java.io.IOException;

import com.rabbitmq.client.Channel;

public interface PrunedConnection {
    public Channel createChannel() throws IOException;
    public Channel createChannel(int channelNumber) throws IOException;
}
