package com.conyers.dam;

import com.conyers.dam.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MqttSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceConnector.class);
    private Map<String, String> connectorProperties;

    @Override
    public void start(Map<String, String> map) {
        connectorProperties = map;
        logger.info("STARTING mqtt source connector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        logger.info("Returning the task class...");
        return MqttSourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        logger.info("Calling the task configs...");
        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        taskConfigs.add(new HashMap<>(connectorProperties));
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("STOPPING mqtt source connector");
    }

    @Override
    public ConfigDef config() {
        return MqttSourceConnectorConfig.configuration;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

}
