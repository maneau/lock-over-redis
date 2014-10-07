package org.maneau.lockoverredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by maneau on 07/10/2014.
 */
public class FifoDao extends RedisDao {

    private static Logger LOGGER = LoggerFactory.getLogger(FifoDao.class);

    private String fifoName;

    public FifoDao(JedisPool jedisPool, int indexId, String fifoName) {
        super(jedisPool, indexId);
        this.fifoName = fifoName;
    }

    public FifoDao(Jedis jedis, int indexId, String fifoName) {
        super(jedis, indexId);
        this.fifoName = fifoName;
    }

    public void addAtLast(String value) throws IOException {
        if(value == null || value.length() == 0) {
            return;
        }
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            jedis.lpush(fifoName, value);
        } catch (JedisException e) {
            throw new IOException("Error while pushing " + value, e);
        } finally {
            releaseJedis(jedis);
        }
    }

    public void addAtLast(List<String> values) throws IOException {
        if(values == null || values.isEmpty()) {
            return;
        }
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            String[] listValues = values.toArray(new String[values.size()]);
            jedis.lpush(fifoName, listValues);
        } catch (JedisException e) {
            throw new IOException("Error while pushing values", e);
        } finally {
            releaseJedis(jedis);
        }
    }

    public void addAtFirst(List<String> values) throws IOException {
        if(values == null || values.isEmpty()) {
            return;
        }
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            String[] listValues = values.toArray(new String[values.size()]);
            jedis.rpush(fifoName, listValues);
        } catch (JedisException e) {
            throw new IOException("Error while pushing values", e);
        } finally {
            releaseJedis(jedis);
        }
    }

    public void addAtFirst(String value) throws IOException {
        if(value == null || value.length() == 0) {
            return;
        }
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            jedis.rpush(fifoName, value);
        } catch (JedisException e) {
            throw new IOException("Error while pushing " + value, e);
        } finally {
            releaseJedis(jedis);
        }
    }

    public void flush() throws IOException {
        Jedis jedis = reserveJedis();
        jedis.flushDB();
        releaseJedis(jedis);
    }

    public String getFirst() throws IOException {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            value = jedis.rpop(fifoName);
        } catch (JedisException e) {
            throw new IOException("Error while pushing " + value, e);
        } finally {
            releaseJedis(jedis);
        }
        return value;
    }

    public String getLast() throws IOException {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            value = jedis.lpop(fifoName);
        } catch (JedisException e) {
            throw new IOException("Error while pushing " + value, e);
        } finally {
            releaseJedis(jedis);
        }
        return value;
    }

    public List<String> getNFirst(long number) throws IOException {
        List<String> values = new ArrayList<String>();
        Jedis jedis = null;
        try {
            jedis = reserveJedis();

            for (long i = 0; i < number; i++) {
                String value = jedis.rpop(fifoName);
                if (value == null || value.length() == 0) {
                    //la file est vide on arrête
                    break;
                } else {
                    values.add(value);
                }
            }

        } catch (JedisException e) {
            throw new IOException("Error while getting getNFirst", e);
        } finally {
            releaseJedis(jedis);
        }
        return values;
    }

    public List<String> getNLast(long number) throws IOException {
        List<String> values = new ArrayList<String>();
        Jedis jedis = null;
        try {
            jedis = reserveJedis();

            for (long i = 0; i < number; i++) {
                String value = jedis.lpop(fifoName);
                if (value == null || value.length() == 0) {
                    //la file est vide on arrête
                    break;
                } else {
                    values.add(value);
                }
            }

        } catch (JedisException e) {
            throw new IOException("Error while getting getNLast", e);
        } finally {
            releaseJedis(jedis);
        }
        return values;
    }

    public long count() throws IOException {
        long count = 0;
        Jedis jedis = null;
        try {
            jedis = reserveJedis();
            count = jedis.llen(fifoName);
        } catch (JedisException e) {
            throw new IOException("Error while getting getNLast", e);
        } finally {
            releaseJedis(jedis);
        }
        return count;
    }
}
