package com.conyers.dam;

import com.conyers.dam.util.SSLUtils;
import com.conyers.dam.util.Version;
import org.apache.kafka.connect.data.Schema;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class MqttSourceConnectorTask extends SourceTask implements MqttCallback {

    private MqttClient mqttClient;
    private String kafkaTopic;
    private String mqttTopic;
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
            mqttClient.subscribe(mqttTopic, connectorConfiguration.getInt("mqtt.connector.qos"));
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
        kafkaTopic = connectorConfiguration.getString("mqtt.connector.kafka.topic");
        mqttClientId = connectorConfiguration.getString("mqtt.connector.client.id");
        mqttTopic = connectorConfiguration.getString("mqtt.connector.broker.topic");
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

    /*@Override
    public void connectionLost(Throwable throwable) {
        logger.error("Connection for connector: '{}', running client: '{}', lost to topic: '{}'.", connectorName, mqttClientId, mqttTopic);
    }*/

    @Override
    public void messageArrived(String tempMqttTopic, MqttMessage mqttMessage) {
        logger.debug("Mqtt message arrived to connector: '{}', running client: '{}', on topic: '{}'.", connectorName, mqttClientId, tempMqttTopic);
        try {
            String new_string = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
            logger.debug("Mqtt message payload in byte array: '{}'", new_string);
            mqttRecordQueue.put(new SourceRecord(null, null, kafkaTopic, null,
                    Schema.STRING_SCHEMA, new_string)
            );
            /*mqttRecordQueue.put(new SourceRecord(null, null, kafkaTopic, null,
                    Schema.STRING_SCHEMA, makeDBDoc(mqttMessage.getPayload(), tempMqttTopic))
            );*/
        } catch (Exception e) {
            logger.error("ERROR: Not able to create source record from mqtt message '{}' arrived on topic '{}' for client '{}'.", mqttMessage.toString(), tempMqttTopic, mqttClientId);
            //logger.error(e);
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

    private byte[] addTopicToJSONByteArray(byte[] bytes, String topic) {
        String topicAsJSON = ",\"topic\":\""+topic+"\"}";
        int byteslen = bytes.length-1;
        int topiclen = topicAsJSON.length();
        logger.debug("New topic: '{}', for publishing by connector: '{}'", topicAsJSON, connectorName);
        byte[] byteArrayWithTopic = new byte[byteslen+topiclen];
        for (int i = 0; i < byteslen; i++) {
            byteArrayWithTopic[i] = bytes[i];
        }
        for (int i = 0; i < topiclen; i++) {
            byteArrayWithTopic[byteslen+i] = (byte) topicAsJSON.charAt(i);
        }
        logger.debug("New payload with topic key/value, as ascii array: '{}'", byteArrayWithTopic);
        return byteArrayWithTopic;
    }

    //This is old nonsense code, but I'm keeping it around just in case it has 
    //valuable information
    /*private String makeDBDoc(byte[] payload, String topic) {
      String msg = new String(payload);
      Document message = Document.parse(msg);
      Document doc = new Document();
      List<String> topicArr = Arrays.asList(topic.split("/"));
      Long unique_id = Long.parseLong(topicArr.get(21));
      Long quadkey = Long.parseLong(String.join("",topicArr.subList(2,20)));
      String now = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
      Document dt = new Document();
      dt.put("$date",now);
      doc.put("message",message);
      doc.put("unique_id",unique_id);
      doc.put("quadkey",quadkey);
      doc.put("updateDate",dt);
      doc.put("pushed",false);
      return doc.toJson();
    }*/
}
