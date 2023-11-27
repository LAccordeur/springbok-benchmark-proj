package com.rogerguo.test.benchmark.benchmark;

import com.rogerguo.test.benchmark.springbok.SpringbokDriver;

public class SpringbokStopServer {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SpringbokDriver.stopServer();
        long stop = System.currentTimeMillis();
        System.out.println("stop time: " + (stop - start) + " ms");
    }
}
