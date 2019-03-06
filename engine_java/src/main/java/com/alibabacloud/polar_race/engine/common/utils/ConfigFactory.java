package com.alibabacloud.polar_race.engine.common.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigFactory {
    private static ConfigFactory instance = null;

    public static ConfigFactory Instance() {
        if (instance == null) {
            instance = new ConfigFactory();
        }
        return instance;
    }

    // Properties is thread safe, so we do not put synchronization on it.
    private Properties prop = null;

    private ConfigFactory() {
        prop = new Properties();
        String engineHome = System.getenv("ENGINE_HOME");
        InputStream in = null;
        if (engineHome == null) {
            in = this.getClass().getResourceAsStream("/engine.properties");
        } else {
            if (!(engineHome.endsWith("/") || engineHome.endsWith("\\"))) {
                engineHome += "/";
            }
            prop.setProperty("engine.home", engineHome);
            try {
                in = new FileInputStream(engineHome + "engine.properties");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        try {
            if (in != null) {
                prop.load(in);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadProperties(String propFilePath) throws IOException {
        FileInputStream in = null;
        try {
            in = new FileInputStream(propFilePath);
            this.prop.load(in);
        } catch (IOException e) {
            throw e;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw e;
                }
            }
        }
    }

    public void addProperty(String key, String value) {
        this.prop.setProperty(key, value);
    }

    public String getProperty(String key) {
        return this.prop.getProperty(key);
    }
}
