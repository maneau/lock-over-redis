package org.maneau.lockoverredis;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class LockBench extends BaseLockTest {

    private static Logger LOGGER = LoggerFactory.getLogger(LockBench.class);

    /**
     * The number of keys, lower bigger the conflicts probabilities are
     */
    private static final int NB_DIFFERENT_KEYS = 10;
    /**
     * Number of threads
     */
    private static final int NB_THREADS = 100;
    /**
     * Time between the lock and the unlock
     */
    private static final long SLEEP_TIME = 1000;
    /**
     * total Number of operations for all threads together
     */
    private static final int TOTAL_OPERATIONS = 100;

    private static Random rand = new Random();
    private static LockFactory lockFactory;

    @Test
    public void testBenchWithManyConflics() throws Exception {
        lockFactory = new LockFactory(getJedisPool());
        long t = System.currentTimeMillis();
        launchThreadsForConflics(NB_THREADS);
        long elapsed = System.currentTimeMillis() - t;

        long opsCount = lockFactory.getStatsLockSuccess()
                + lockFactory.getStatsUnlockSuccess()
                + lockFactory.getStatsLockFailed();

        LOGGER.info("Bench Speed with pool: " + ((1000 * opsCount) / elapsed) + " o/s"
                + " and " + lockFactory.getStatsLockStolen() + " locks stolen");
    }

    private static void launchThreadsForConflics(int nbThreads) throws Exception {
        List<Thread> tds = new ArrayList<Thread>();

        final AtomicInteger ind = new AtomicInteger();

        for (int i = 0; i < nbThreads; i++) {
            Thread hj = new Thread(new Runnable() {
                public void run() {
                    LOGGER.debug("Starting thread :" + Thread.currentThread().getId());

                    while (ind.get() < TOTAL_OPERATIONS) {
                        try {
                            final String key = String.valueOf(rand.nextInt(NB_DIFFERENT_KEYS));
                            LockFactory.MyLock myLock = lockFactory.getLock("default", key);
                            if (myLock.tryLock()) {

                                if ((ind.incrementAndGet() % 10) == 0) {
                                    lockFactory.printStats();
                                }

                                Thread.sleep(SLEEP_TIME);
                                myLock.tryUnlock();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                    LOGGER.debug("Ending thread :" + Thread.currentThread().getId());
                }
            });
            tds.add(hj);
            hj.start();
        }
        for (Thread t : tds) {
            t.join();
        }
    }

    @Test
    public void testBenchManyLocks() throws Exception {
        lockFactory = new LockFactory(getJedisPool());
        long t = System.currentTimeMillis();
        launchThreadsForManyLocks(NB_THREADS*10);
        long elapsed = System.currentTimeMillis() - t;

        long opsCount = lockFactory.getStatsLockSuccess()
                + lockFactory.getStatsUnlockSuccess()
                + lockFactory.getStatsLockFailed();

        LOGGER.info("Bench Speed with pool: " + ((1000 * opsCount) / elapsed) + " o/s"
                + " and " + lockFactory.getStatsLockStolen() + " locks stolen");
    }

    private static void launchThreadsForManyLocks(int nbThreads) throws Exception {
        List<Thread> tds = new ArrayList<Thread>();

        final AtomicInteger ind = new AtomicInteger();

        for (int i = 0; i < nbThreads; i++) {
            Thread hj = new Thread(new Runnable() {
                public void run() {
                    LOGGER.debug("Starting thread :" + Thread.currentThread().getId());

                    while (ind.get() < 10000) {
                        try {
                            LockFactory.MyLock myLock = lockFactory.getLock("default", UUID.randomUUID());
                            if (myLock.tryLock()) {
                                if ((ind.incrementAndGet() % 100) == 0) {
                                    lockFactory.printStats();
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    LOGGER.debug("Ending thread :" + Thread.currentThread().getId());
                }
            });
            tds.add(hj);
            hj.start();
        }
        for (Thread t : tds) {
            t.join();
        }
    }
}