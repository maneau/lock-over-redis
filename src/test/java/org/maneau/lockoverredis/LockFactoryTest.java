package org.maneau.lockoverredis;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LockFactoryTest extends BaseLockTest {

    @Test
    public void testBasicLock() throws Exception {
        LockFactory lockFactory = new LockFactory(getJedisPool());
        LockFactory.MyLock myLock = lockFactory.getLock("default", UUID.randomUUID());
        assertTrue(myLock.tryLock());

        assertFalse(myLock.tryLock());

        assertTrue(myLock.tryUnlock());
        assertTrue(myLock.tryLock());

        myLock.unlock();
    }

    @Test
     public void testTTLLock() throws Exception {
        LockFactory lockFactory = new LockFactory(getJedisPool());
        LockFactory.MyLock myLock = lockFactory.getLock("default", UUID.randomUUID());
        assertTrue(myLock.tryLock());
        long ttl1 = myLock.getTtl();
        Thread.sleep(2000);
        long ttl2 = myLock.getTtl();
        assertTrue(ttl2 < ttl1);
        Thread.sleep(2000);
        assertTrue(myLock.expire());
        long ttl3 = myLock.getTtl();
        assertTrue(ttl3 > ttl2);
    }

    @Test
    public void testExpiredLock() throws Exception {
        LockFactory lockFactory = new LockFactory(getJedisPool().getResource());
        lockFactory.setDefaultExpire(1, TimeUnit.SECONDS);
        LockFactory.MyLock myLock = lockFactory.getLock("default", UUID.randomUUID().toString());
        assertTrue(myLock.tryLock());
        long ttl1 = myLock.getTtl();
        assertTrue(ttl1 < 2);
        Thread.sleep(2000);
        long ttl2 = myLock.getTtl();
        assertTrue(ttl2 == -2);
    }

    @Test
    public void testIsLocked() throws Exception {
        LockFactory lockFactory = new LockFactory(getJedisPool().getResource());
        lockFactory.setDefaultExpire(1, TimeUnit.SECONDS);
        LockFactory.MyLock myLock = lockFactory.getLock("default", UUID.randomUUID().toString());
        assertTrue(myLock.tryLock());
        assertTrue(myLock.isLocked());
        Thread.sleep(2000);
        assertFalse(myLock.isLocked());
    }

}