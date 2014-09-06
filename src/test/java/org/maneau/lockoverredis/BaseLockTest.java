package org.maneau.lockoverredis;

import junit.framework.TestCase;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by maneau on 06/09/2014.
 */
public class BaseLockTest extends TestCase {

    private static Logger LOGGER = LoggerFactory.getLogger(BaseLockTest.class);
    protected static String redisHost;
    protected static int redisPort;

    private static JedisPool jedisPool;

    @Before
    public void setUp() throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("testConfig.properties");

        Config.loadProperties(inputStream);
        redisHost = Config.getProperty("redis.host");
        redisPort = Integer.valueOf(Config.getProperty("redis.port"));

        jedisPool = new JedisPool(new GenericObjectPoolConfig(),
                redisHost, redisPort, 20000);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public static class Config {
        private static Properties properties = new Properties();

        public static void loadProperties(InputStream inputStream) throws IOException {

            if (inputStream == null) {
                throw new FileNotFoundException("property file not found in the classpath");
            }
            properties.load(inputStream);
        }

        public static String getProperty(String name) {
            return properties.getProperty(name);
        }
    }

    @Test
    public void testBasic() {
        assertNotNull(jedisPool);
    }

    @After
    public void tearDown() throws Exception {
        jedisPool.destroy();
    }
}
