/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.config;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.loader.EnhancedServiceLoader;

import java.util.Objects;

/**
 * The type Configuration factory.
 * 配置工厂
 *
 * @author jimin.jm @alibaba-inc.com
 * @author Geng Zhang
 */
public final class ConfigurationFactory {
    private static final String REGISTRY_CONF_PREFIX = "registry";
    private static final String REGISTRY_CONF_SUFFIX = ".conf";
    private static final String ENV_SYSTEM_KEY = "SEATA_ENV";
    private static final String ENV_PROPERTY_KEY = "seataEnv";
    /**
     * the name of env
     */
    private static String envValue;

    static {
        envValue = System.getProperty(ENV_PROPERTY_KEY);
        if (null == envValue) {
            envValue = System.getenv(ENV_SYSTEM_KEY);
        }
    }

    // 默认配置文件registry.conf
    private static final Configuration DEFAULT_FILE_INSTANCE = new FileConfiguration(
        REGISTRY_CONF_PREFIX + REGISTRY_CONF_SUFFIX);
    // 当前文件配置实例
    public static final Configuration CURRENT_FILE_INSTANCE = null == envValue ? DEFAULT_FILE_INSTANCE : new FileConfiguration(REGISTRY_CONF_PREFIX + "-" + envValue
        + REGISTRY_CONF_SUFFIX);
    private static final String NAME_KEY = "name";
    private static final String FILE_TYPE = "file";

    private static volatile Configuration instance = null;

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static Configuration getInstance() {
        if (instance == null) {
            synchronized (Configuration.class) {
                if (instance == null) {
                    instance = buildConfiguration();
                }
            }
        }
        return instance;
    }

    private static Configuration buildConfiguration() {
        ConfigType configType = null;
        String configTypeName = null;
        try {
            // get config.type
            configTypeName = CURRENT_FILE_INSTANCE.getConfig(
                ConfigurationKeys.FILE_ROOT_CONFIG + ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR
                    + ConfigurationKeys.FILE_ROOT_TYPE);
            configType = ConfigType.getType(configTypeName);
        } catch (Exception e) {
            throw new NotSupportYetException("not support register type: " + configTypeName, e);
        }
        if (ConfigType.File == configType) {
            // config.file.name
            String pathDataId = ConfigurationKeys.FILE_ROOT_CONFIG + ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR
                + FILE_TYPE + ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR
                + NAME_KEY;
            // 对于默认配置，得到的是file.conf
            String name = CURRENT_FILE_INSTANCE.getConfig(pathDataId);
            return new FileConfiguration(name);
        } else {
            return EnhancedServiceLoader.load(ConfigurationProvider.class, Objects.requireNonNull(configType).name())
                .provide();
        }
    }
}
