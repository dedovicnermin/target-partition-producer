package io.nermdev.kafka.targetpartitionproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public final class ConfigUtils {
    static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    private ConfigUtils() {}

    public static void loadConfig(final String file, final Properties properties) {
        try (
                final FileInputStream inputStream = new FileInputStream(file)
        ) {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Exception when attempting to load configuration file : {}", e.getMessage());
            System.exit(1);
        }
    }

    

    public static Properties getProperties(String[] args) {
        if (args.length < 1) throw new IllegalArgumentException("Pass path to application.properties");
        final Properties properties = new Properties();
        ConfigUtils.loadConfig(args[0], properties);
        return properties;
    }
}
