/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pinmicro.messagebrokerclient.service;

import javax.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import io.netty.util.internal.StringUtil;

/**
 *
 * @author deepak
 */
@Service
public class MQTTService {

    @Value("${broker.mqtt.channel}")
    private String channel;

    @Value("${broker.mqtt.url}")
    private String mqttUrl;

    @Value("${broker.mqtt.username}")
    private String mqttUsername;

    @Value("${broker.mqtt.password}")
    private String mqttPassword;

    private static final String CLIENT_ID = "client-";
    MemoryPersistence persistence = new MemoryPersistence();

    private MqttClient mqttClient;

    private final Logger LOGGER = LoggerFactory.getLogger(MQTTService.class);

    @PostConstruct
    public void createMQTTClient() {
        if (StringUtil.isNullOrEmpty(mqttUrl)) {
            LOGGER.error("mqtt.url is invalid,skipping MQTT connection");
        } else if (StringUtil.isNullOrEmpty(mqttPassword) || StringUtil.isNullOrEmpty(mqttUsername)) {
            LOGGER.error("mqtt.username and mqtt.password required");
        } else {
            try {
                mqttClient = new MqttClient(mqttUrl, CLIENT_ID + MqttClient.generateClientId(), persistence);
                MqttConnectOptions connectOptions = new MqttConnectOptions();
                connectOptions.setUserName(mqttUsername);
                connectOptions.setPassword(mqttPassword.toCharArray());
                mqttClient.connect(connectOptions);

                LOGGER.info("Reciever Connected");

                try {
                    subscribe(channel);
                    LOGGER.info("topic: {} suscribed", channel);
                } catch (MqttException ex) {
                    LOGGER.error("excep: ", ex);
                }

            } catch (MqttException ex) {
                LOGGER.error("MQTT connection exception:", ex);
            }
        }
    }

    private void subscribe(String channel) throws MqttException {
        mqttClient.subscribe(channel, (String topic, MqttMessage mm) -> {
            LOGGER.info("Message received : " + topic + " " + new String(mm.getPayload()));
        });
    }

    private void disconnect() throws MqttException {
        LOGGER.info("Reciever Disconnected");
        mqttClient.disconnect();
    }

}
