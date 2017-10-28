/*
 * Copyright (C) Fredrik Larsson <nossralf@gmail.com>
 * All rights reserved.
 *
 * This software may be modified and distributed under the terms
 * of the BSD license.  See the LICENSE file for details.
 */
package com.analogmountains.flume;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMQSource extends AbstractSource implements Configurable,
    Consumer, EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(RabbitMQSource.class);
  private Connection connection;
  private ConnectionFactory factory;
  private Channel channel;
  private String exchangeName;
  private String queueName;
  private String exchangeType;
  private String bindingKey;

  @Override
  public synchronized void start() {
    logger.info("Starting RabbitMQ source");
    try {
      connection = factory.newConnection();
      channel = connection.createChannel();

      channel.exchangeDeclare(exchangeName, exchangeType, true);
      channel.queueDeclare(queueName, true, false, false, null);
      channel.queueBind(queueName, exchangeName, bindingKey);

      channel.basicConsume(queueName, false, this);
    } catch (Exception e) {
      logger.error("Exception while connecting to RabbitMQ", e);
    }
    super.start();
    logger.info("RabbitMQ source started");
  }

  @Override
  public synchronized void stop() {
    logger.info("Stopping RabbitMQ source");

    try {
      channel.close();
      connection.close();
    } catch (Exception e) {
      logger.error("Exception caught during connection closing", e);
    }
    super.stop();
    logger.info("RabbitMQ source stopped");
  }

  @Override
  public void configure(Context context) {
    exchangeName = context.getString("exchangeName");
    exchangeType = context.getString("exchangeType");
    queueName = context.getString("queueName");
    bindingKey = context.getString("bindingKey");

    factory = new ConnectionFactory();
    factory.setUsername(context.getString("userName"));
    factory.setPassword(context.getString("password"));
    factory.setVirtualHost(context.getString("virtualHost"));
    factory.setHost(context.getString("hostName"));
    factory.setPort(context.getInteger("port", 5672));
  }

  @Override
  public void handleDelivery(String consumerTag, Envelope envelope,
      BasicProperties properties, byte[] body) throws IOException {
    Map<String, String> headers = eventHeadersFromBasicProperties(properties);
    headers.put("routingKey", envelope.getRoutingKey());
    headers.put("exchange", envelope.getExchange());

    Event event = EventBuilder.withBody(body);
    event.setHeaders(headers);
    getChannelProcessor().processEvent(event);

    long deliveryTag = envelope.getDeliveryTag();
    channel.basicAck(deliveryTag, false);
  }

  private Map<String, String> eventHeadersFromBasicProperties(
      BasicProperties properties) {
    Map<String, String> headers = new HashMap<String, String>();
    if (properties.getAppId() != null) {
      headers.put("appId", properties.getAppId());
    }
    if (properties.getContentEncoding() != null) {
      headers.put("contentEncoding", properties.getContentEncoding());
    }
    if (properties.getContentType() != null) {
      headers.put("contentType", properties.getContentType());
    }
    if (properties.getCorrelationId() != null) {
      headers.put("correlationId", properties.getCorrelationId());
    }
    if (properties.getDeliveryMode() != null) {
      headers.put("deliveryMode",
          Integer.toString(properties.getDeliveryMode()));
    }
    if (properties.getExpiration() != null) {
      headers.put("expiration", properties.getExpiration());
    }
    if (properties.getMessageId() != null) {
      headers.put("messageId", properties.getMessageId());
    }
    if (properties.getPriority() != null) {
      headers.put("priority", Integer.toString(properties.getPriority()));
    }
    if (properties.getReplyTo() != null) {
      headers.put("replyTo", properties.getReplyTo());
    }
    if (properties.getType() != null) {
      headers.put("type", properties.getType());
    }
    if (properties.getUserId() != null) {
      headers.put("userId", properties.getUserId());
    }
    return headers;
  }

  @Override
  public void handleCancelOk(String consumerTag) {
  }

  @Override
  public void handleConsumeOk(String consumerTag) {
  }

  @Override
  public void handleShutdownSignal(String consumerTag,
      ShutdownSignalException sig) {
  }

  @Override
  public void handleCancel(String consumerTag) throws IOException {
  }

  @Override
  public void handleRecoverOk(String consumerTag) {
  }
}
