package org.maneau.lockoverredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;

/**
 * Created by maneau on 07/10/2014.
 */
public class RedisDao {

    private static Logger LOGGER = LoggerFactory.getLogger(RedisDao.class);

    private Jedis jedis = null;
    private JedisPool jedisPool = null;
    private int indexId = 1;

    protected RedisDao(JedisPool jedisPool, int indexId) {
        this.jedisPool = jedisPool;
        this.indexId = indexId;
    }

    protected RedisDao(Jedis jedis, int indexId) {
        this.jedis = jedis;
        this.indexId = indexId;
    }

    protected Jedis reserveJedis() {
        Jedis j = null;
        try {
            if (jedisPool == null) {
                j = jedis;
            } else {
                j = jedisPool.getResource();
            }
            j.select(indexId);
        } catch (JedisException e) {
            LOGGER.error("Error while getting jedis from pool", e);
        }
        return j;
    }

    protected void releaseJedis(Jedis jedis) {
        try {
            if (jedisPool != null && jedis != null) {
                jedisPool.returnResource(jedis);
            }
        } catch (JedisException e) {
            LOGGER.error("Error while releasing a jedis to the pool");
        }
    }
}
