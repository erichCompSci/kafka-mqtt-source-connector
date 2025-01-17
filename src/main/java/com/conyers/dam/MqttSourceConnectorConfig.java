package com.conyers.dam;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class MqttSourceConnectorConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnectorConfig.class);

    static ConfigDef configuration = baseConfigDef();
    public MqttSourceConnectorConfig(Map<String, String> properties) {
        super(configuration, properties);
    }

    public static ConfigDef baseConfigDef() {
        ConfigDef configDef = new ConfigDef();
        configDef.define("mqtt.connector.broker.uri", Type.STRING,
                "tcp://localhost:1883", Importance.HIGH,
                "Full uri to mqtt broker")
                .define("mqtt.connector.broker.topics", Type.LIST, null, Importance.HIGH,
                        "mqtt server to connect to")
                .define("mqtt.connector.client.id", Type.STRING, "kafka_source_connector", Importance.MEDIUM,
        "mqtt client id to use don't set to use random")
                .define("mqtt.connector.clean_session", Type.BOOLEAN, true, Importance.MEDIUM,
        "If connection should begin with clean session")
                .define("mqtt.connector.connection_timeout", Type.INT, 30, Importance.LOW,
        "Connection timeout limit")
                .define("mqtt.connector.keep_alive", Type.INT, 60, Importance.LOW,
        "The interval to keep alive")
                .define("mqtt.connector.qos", Type.INT, 1, Importance.LOW,
                        "which qos to use for paho client connection")
                .define("mqtt.connector.ssl", Type.BOOLEAN, false, Importance.LOW,
                        "which qos to use for paho client connection")
                .define("mqtt.connector.ssl.ca", Type.STRING, "./ca.crt", Importance.LOW,
                        "If secure (SSL) then path to CA is needed.")
                .define("mqtt.connector.ssl.crt", Type.STRING, "./client.crt", Importance.LOW,
                        "If secure (SSL) then path to client crt is needed.")
                .define("mqtt.connector.ssl.key", Type.STRING, "./client.key", Importance.LOW,
                        "If secure (SSL) then path to client key is needed.")
                .define("mqtt.connector.kafka.topics", Type.LIST, null, Importance.MEDIUM,
                        "Kafka topic to publish on. This depends on processing unit.")
                .define("mqtt.connector.kafka.topic_types", Type.LIST, null, Importance.MEDIUM,
                        "Type of kafka topics.  Can be (int, long, float, double, or string).")
                .define("mqtt.connector.kafka.name", Type.STRING, "source_kafka", Importance.MEDIUM,
                        "Kafka topic to publish on. This depends on processing unit.");
        return configDef;
    }

    public static void main(String[] args) {
        log.info(configuration.toEnrichedRst());
    }


}
