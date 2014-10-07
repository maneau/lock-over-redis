package org.maneau.lockoverredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Created by maneau on 05/09/2014.
 */
public class LockFactory extends RedisDao {

    private static Logger LOGGER = LoggerFactory.getLogger(LockFactory.class);

    private Jedis jedis = null;
    private JedisPool jedisPool = null;
    private final AtomicLong statsLockStolen = new AtomicLong(0);
    private final AtomicLong statsLockSuccess = new AtomicLong(0);
    private final AtomicLong statsUnlockSuccess = new AtomicLong(0);
    private final AtomicLong statsLockFailed = new AtomicLong(0);

    private final static long DEFAULT_LOCK_TIME = 10;
    private final static int DEFAULT_INDEX_ID = 5;
    private TimeUnit defaultUnitTime = TimeUnit.MINUTES;
    private long defaultLockTime = DEFAULT_LOCK_TIME;

    public LockFactory(Jedis jedis) {
        super(jedis, DEFAULT_INDEX_ID);
        this.jedis = jedis;
    }

    public LockFactory(JedisPool jedisPool) {
        super(jedisPool, DEFAULT_INDEX_ID);
        this.jedisPool = jedisPool;
    }

    public long getStatsLockStolen() {
        return statsLockStolen.get();
    }

    public long getStatsLockSuccess() {
        return statsLockSuccess.get();
    }

    public long getStatsUnlockSuccess() {
        return statsUnlockSuccess.get();
    }

    public long getStatsLockFailed() {
        return statsLockFailed.get();
    }

    /**
     * Methode to change the default expire time
     *
     * @param time
     * @param timeUnit
     */
    public void setDefaultExpire(long time, TimeUnit timeUnit) {
        this.defaultUnitTime = timeUnit;
        this.defaultLockTime = time;
    }

    public MyLock getLock(String collectionName, UUID uuid) {
        return new MyLock(collectionName, uuid.toString(), Thread.currentThread().getId());
    }

    public MyLock getLock(String collectionName, String id) {
        return new MyLock(collectionName, id, Thread.currentThread().getId());
    }

    /**
     * Lock class is the element used to manage locks
     */
    public class MyLock implements Lock {
        private final String collectionName;
        private final String id;
        private final long threadId;

        protected MyLock(String collectionName, String id, long threadId) {
            this.collectionName = collectionName;
            this.id = id;
            this.threadId = threadId;
        }

        @Override
        public void lock() {
            tryLockInner(this, defaultLockTime, defaultUnitTime);
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throw new NotImplementedException();
        }

        @Override
        public boolean tryLock() {
            return tryLockInner(this, defaultLockTime, defaultUnitTime);
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            return tryLockInner(this, l, timeUnit);
        }

        @Override
        public void unlock() {
            tryUnlockInner(this);
        }

        public boolean tryUnlock() {
            return tryUnlockInner(this);
        }

        @Override
        public Condition newCondition() {
            throw new NotImplementedException();
        }

        public long getTtl() {
            return getInnerTtl(this);
        }

        public boolean isLocked() {
            return isInnerLocked(this);
        }

        @Override
        public String toString() {
            return id + ":" + collectionName + ":" + threadId;
        }

        public boolean equals(String s) {
            return this.toString().equals(s);
        }

        private String getLockKey() {
            return id + ":" + collectionName;
        }

        public boolean expire() {
            return expireInner(this, defaultLockTime, defaultUnitTime);
        }

        public boolean expire(final long leaseTime, final TimeUnit unit) {
            return expireInner(this, leaseTime, unit);
        }

    }

    private long getInnerTtl(final MyLock myLock) {
        Jedis jedis = reserveJedis();

        long ttl = jedis.ttl(myLock.getLockKey());
        releaseJedis(jedis);
        return ttl;
    }

    private boolean isInnerLocked(final MyLock myLock) {
        Jedis jedis = reserveJedis();
        boolean isLocked = jedis.exists(myLock.getLockKey());
        releaseJedis(jedis);
        return isLocked;
    }

    private boolean tryLockInner(final MyLock myLock, final long leaseTime, final TimeUnit unit) {
        Jedis jedis = reserveJedis();
        boolean success = (jedis.setnx(myLock.getLockKey(), myLock.toString()) == 1);

        if (success) {
            LOGGER.debug("The lock " + myLock.getLockKey() + " is placed");
            jedis.expire(myLock.getLockKey(), (int) unit.toSeconds(leaseTime));
            statsLockSuccess.incrementAndGet();
        } else {
            //A lock is already placed
            LOGGER.debug("The lock " + myLock.getLockKey() + " already taken by someone else");
            statsLockFailed.incrementAndGet();
        }
        releaseJedis(jedis);

        return success;
    }

    private boolean expireInner(final MyLock myLock, final long leaseTime, final TimeUnit unit) {
        Jedis jedis = reserveJedis();
        boolean success = (jedis.expire(myLock.getLockKey(), (int) unit.toSeconds(leaseTime)) == 1);
        releaseJedis(jedis);

        if (success) {
            LOGGER.debug("The expire of lock " + myLock.getLockKey() + " is reset");
        } else {
            //A lock is already placed
            LOGGER.debug("The lock " + myLock.getLockKey() + " already taken by someone else");
        }

        return success;
    }

    private boolean tryUnlockInner(final MyLock myLock) {
        Jedis jedis = reserveJedis();
        Transaction t = jedis.multi();
        Response<String> respValue = t.get(myLock.getLockKey());
        Response<Long> respSuccess = t.del(myLock.getLockKey());
        t.exec();
        releaseJedis(jedis);

        try {
            String value = respValue.get();
            if (myLock.equals(value)) {
                LOGGER.debug("Lock successfully unlocked");
                statsUnlockSuccess.incrementAndGet();
            } else {
                LOGGER.debug("Unlock process failed");
                statsLockStolen.incrementAndGet();
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error while unlock : " + e.getMessage());
            statsLockStolen.incrementAndGet();
            return false;
        }

        if (respSuccess.get() == 0) {
            LOGGER.error("Error while unlocking process");
            statsLockStolen.incrementAndGet();
            return false;
        }
        return true;
    }

    public void printStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("Lock statistics [");
        sb.append("Errors :(").append(statsLockStolen.get()).append("), ");
        sb.append("Locks :(").append(statsLockSuccess.get()).append("), ");
        sb.append("Unlocks :(").append(statsLockSuccess.get()).append("), ");
        sb.append("Conflics :(").append(statsLockFailed.get()).append(")");
        sb.append("]");

        LOGGER.info(sb.toString());
    }

}
