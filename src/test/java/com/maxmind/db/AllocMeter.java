package com.maxmind.db;

import java.lang.management.ManagementFactory;
import com.sun.management.ThreadMXBean;

public class AllocMeter {
    private final ThreadMXBean tmx = (ThreadMXBean)(ManagementFactory.getThreadMXBean());
    private long overhead = 0;

    private final long MEASUREMENT_OVERHEAD;
    {
	long tid = Thread.currentThread().getId();
	long a0 = tmx.getThreadAllocatedBytes(tid);
	long a1 = tmx.getThreadAllocatedBytes(tid);
	long a2 = tmx.getThreadAllocatedBytes(tid);
	MEASUREMENT_OVERHEAD = a2 - a1;
	//System.out.println("AllocMeter: MEASUREMENT_OVERHEAD=" + MEASUREMENT_OVERHEAD);
    }

    public long allocByteCount() {
	overhead += MEASUREMENT_OVERHEAD;
        return tmx.getThreadAllocatedBytes(Thread.currentThread().getId()) - overhead;
    }

}
