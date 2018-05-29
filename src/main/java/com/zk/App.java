package com.zk;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws InterruptedException {
        ZKClient zkClient = ZKClient.instance();
        Thread.sleep(100000);
        zkClient.close();
    }
}
