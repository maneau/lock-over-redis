package org.maneau.lockoverredis;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FifoDaoTest extends BaseLockTest {

    private int REDIS_INDEX = 6;
    private String REDIS_FIFO = "myfifo";

    @Test
    public void testBasicFifoLpush() throws Exception {

        FifoDao fifo = new FifoDao(getJedisPool(), REDIS_INDEX, REDIS_FIFO);

        fifo.flush();
        assertEquals(0, fifo.count());
        fifo.addAtLast("1");
        fifo.addAtLast("2");
        fifo.addAtLast("3");
        fifo.addAtFirst("0");
        assertEquals(4, fifo.count());
        assertEquals("0", fifo.getFirst());
        assertEquals("3", fifo.getLast());
    }

    @Test
    public void testMultiFifoLpush() throws Exception {

        FifoDao fifo = new FifoDao(getJedisPool(), REDIS_INDEX, REDIS_FIFO);

        fifo.flush();
        assertEquals(0, fifo.count());
        List<String> values = new ArrayList<String>();
        for (int i = 1; i <= 100; i++) {
            values.add("myvalue " + i);
        }
        fifo.addAtLast(values);

        assertEquals(100, fifo.count());
        assertEquals("myvalue 1", fifo.getNFirst(1).get(0));
        assertEquals("myvalue 100", fifo.getNLast(1).get(0));
        assertEquals("myvalue 10", fifo.getNFirst(9).get(8));
        assertEquals("myvalue 90", fifo.getNLast(10).get(9));
        assertEquals(79, fifo.count());
        values = fifo.getNLast(100);
        assertEquals(79, values.size());
    }

}