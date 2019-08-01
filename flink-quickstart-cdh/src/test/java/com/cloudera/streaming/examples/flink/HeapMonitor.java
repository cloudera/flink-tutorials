package com.cloudera.streaming.examples.flink;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;

public class HeapMonitor {
    private static final int MEGA = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        ArrayList<Long> leak;
        ArrayList<ArrayList<Long>> bucket = new ArrayList<>();
        while (true) {
            Thread.sleep(1000);
            leak = new ArrayList<>(MEGA);
            bucket.add(leak);
            for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
                System.out.println(mpBean.getName() + ":" + mpBean.getUsage().getUsed());
                if (mpBean.getType() == MemoryType.HEAP && mpBean.getName().equals("PS Old Gen")) {
                    MemoryUsage memoryUsage = mpBean.getUsage();
//                    System.out.printf(
//                            "%s: %s %s\n",
//                            mpBean.getName(), mpBean.getUsage().getUsed() / (1024 * 1024), mpBean.getUsage().getMax() / MEGA
//                    );
                }
            }
        }
    }

}
