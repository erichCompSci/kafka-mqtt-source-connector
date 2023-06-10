package com.conyers.dam;

import com.conyers.dam.util.SSLUtils;
import com.conyers.dam.util.Version;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.*;
import org.eclipse.paho.mqttv5.common.packet.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;

import javax.net.ssl.SSLSocketFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;



public class MqttSourceConnectorTask extends SourceTask implements MqttCallback {

    private MqttClient mqttClient;
    private List<String> kafkaTypes;
    private List<String> mqttTopics;
    private String mqttClientId;
    private String connectorName;
    private MqttSourceConnectorConfig connectorConfiguration;
    private SSLSocketFactory sslSocketFactory;
    BlockingQueue<SourceRecord> mqttRecordQueue = new LinkedBlockingQueue<SourceRecord>();
    private static final Logger logger = LoggerFactory.getLogger(MqttSourceConnectorTask.class);

    private void initMqttClient() {

        MqttConnectionOptions mqttConnectOptions = new MqttConnectionOptions();
        mqttConnectOptions.setServerURIs(new String[] {connectorConfiguration.getString("mqtt.connector.broker.uri")});
        mqttConnectOptions.setConnectionTimeout(connectorConfiguration.getInt("mqtt.connector.connection_timeout"));
        mqttConnectOptions.setKeepAliveInterval(connectorConfiguration.getInt("mqtt.connector.keep_alive"));
        mqttConnectOptions.setCleanStart(connectorConfiguration.getBoolean("mqtt.connector.clean_session"));
        mqttConnectOptions.setKeepAliveInterval(connectorConfiguration.getInt("mqtt.connector.connection_timeout"));
        if (connectorConfiguration.getBoolean("mqtt.connector.ssl")) {
            logger.info("SSL TRUE for MqttSourceConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
            try {
                String caCrtFilePath = connectorConfiguration.getString("mqtt.connector.ssl.ca");
                String crtFilePath = connectorConfiguration.getString("mqtt.connector.ssl.crt");
                String keyFilePath = connectorConfiguration.getString("mqtt.connector.ssl.key");
                SSLUtils sslUtils = new SSLUtils(caCrtFilePath, crtFilePath, keyFilePath);
                sslSocketFactory = sslUtils.getMqttSocketFactory();
                mqttConnectOptions.setSocketFactory(sslSocketFactory);
            } catch (Exception e) {
                logger.error("Not able to create SSLSocketfactory: '{}', for mqtt client: '{}', and connector: '{}'", sslSocketFactory, mqttClientId, connectorName);
                //logger.error(e.to_string());
            }
        } else {
            logger.info("SSL FALSE for MqttSourceConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
        }

        try {
            mqttClient = new MqttClient(connectorConfiguration.getString("mqtt.connector.broker.uri"), mqttClientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            mqttClient.connect(mqttConnectOptions);
            logger.info("SUCCESSFULL MQTT CONNECTION for MqttSourceConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
        } catch (MqttException e) {
            logger.error("FAILED MQTT CONNECTION for MqttSourceConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
            //logger.error(e);
        }

        try {
            for (String topic : mqttTopics) {
                mqttClient.subscribe(topic, connectorConfiguration.getInt("mqtt.connector.qos"));
            }
            logger.info("SUCCESSFULL MQTT CONNECTION for MqttSinkConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
        } catch (MqttException e) {
            logger.error("FAILED MQTT CONNECTION for MqttSinkConnectorTask: '{}, and mqtt client: '{}'.", connectorName, mqttClientId);
            e.printStackTrace();
        }

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        connectorConfiguration = new MqttSourceConnectorConfig(map);
        connectorName = connectorConfiguration.getString("mqtt.connector.kafka.name");
        
        //kafkaTopics     = connectorConfiguration.getList("mqtt.connector.kafka.topics");
        kafkaTypes      = connectorConfiguration.getList("mqtt.connector.kafka.topic_types");
        mqttTopics      = connectorConfiguration.getList("mqtt.connector.broker.topics");
        if(kafkaTypes.size() != mqttTopics.size())
        {
            logger.error("Topics size must match mqtt Topics size must match kafka types size, '{}', '{}'", 
                         kafkaTypes.size(), 
                         mqttTopics.size());
            throw new RuntimeException("Topics size and types must match!");
        }
        mqttClientId    = connectorConfiguration.getString("mqtt.connector.client.id");
        logger.info("Starting MqttSourceConnectorTask with connector name: '{}'", connectorName);
        initMqttClient();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        records.add(mqttRecordQueue.take());
        return records;
    }

    @Override
    public void stop() {
        logger.info("Stop has been called...");
    }

    private <T> void submitToQueue(Schema type, T val, String kafkaTopic) {
        //Map<String, String> source_partition = new HashMap<String, String>();
        //source_partition.put("source", "mqttClient");
        //Map<String, Long> source_offset = new HashMap<String, Long>();
        ////source_offset.put("timestamp", System.currentTimeMillis());

        Schema transferBuilder = SchemaBuilder.struct()
         .name("com.conyers.dam.MqttTransferStruct").version(1).doc("A struct that can change the internal data type when transfering from mqtt to kafka")
         .field("timestamp_ms", Schema.INT64_SCHEMA)
         .field("value", type)
         .build();

        Struct val_to_send = new Struct(transferBuilder);
        val_to_send.put("timestamp_ms", System.currentTimeMillis());
        val_to_send.put("value", val);

        try {
            mqttRecordQueue.put(new SourceRecord(null, null, kafkaTopic, null,
                    transferBuilder, val_to_send)
            );
        } catch (Exception e) {
            logger.error("ERROR: Not able to create source record from kafkaTopic '{}'.", kafkaTopic);
            //logger.error(e);
        }
    }


    @Override
    public void messageArrived(String tempMqttTopic, MqttMessage mqttMessage) {
        logger.debug("Mqtt message arrived to connector: '{}', running client: '{}', on topic: '{}'.", connectorName, mqttClientId, tempMqttTopic);
            
        String mqtt_payload = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);

        int index_of_topic = mqttTopics.indexOf(tempMqttTopic);
        if(index_of_topic == -1)
        {
            logger.error("Topic could not be found in config file: '{}'", tempMqttTopic);
            return;
        }

        String k_type = kafkaTypes.get(index_of_topic);
        if(k_type.equals("int")) {
            int value = Integer.parseInt(mqtt_payload);
            submitToQueue(Schema.INT32_SCHEMA, value, tempMqttTopic);
        } else if(k_type.equals("long")) {
            long value = Long.parseLong(mqtt_payload);
            submitToQueue(Schema.INT64_SCHEMA, value, tempMqttTopic);
        } else if(k_type.equals("float")) {
            float value = Float.parseFloat(mqtt_payload);
            submitToQueue(Schema.FLOAT32_SCHEMA, value, tempMqttTopic);
        } else if(k_type.equals("double")) {
            double value = Double.parseDouble(mqtt_payload);
            submitToQueue(Schema.FLOAT64_SCHEMA, value, tempMqttTopic);
        } else if(k_type.equals("string")) {
            submitToQueue(Schema.STRING_SCHEMA, mqtt_payload, tempMqttTopic);
        } else{
            logger.error("Kafka type did not match any predefined types: '{}'", k_type);
            throw new RuntimeException("Kafka type did not match any predefined types!");
        }
    }

    @Override
    public void disconnected(MqttDisconnectResponse mqttDisconnectResponse) {
        logger.error("Received a graceful disconnect, should we do something graceful here?");
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
        logger.error("Mqtt error occurred: '{}'", exception.getMessage());
    }

    @Override
    public void deliveryComplete(IMqttToken iMqttToken) {
        logger.info("Delivery complete has occurred...");

    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        logger.info("Connection complete reconnect: '{}' and server URI: '{}'", reconnect, serverURI);
    }

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {
        logger.error("This has not been implemented yet, do we need it?");
    }
}

