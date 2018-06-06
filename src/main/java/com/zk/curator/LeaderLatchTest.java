package com.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * 群首闩测试
 *
 * @author xuyanhua
 * @description:
 * @date 2018/6/6 下午1:55
 */
public class LeaderLatchTest {
    private final static String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retry);
        client.start();
        LeaderLatch latch = new LeaderLatch(client, "/curator/master");
        LeaderLatchListener listener = new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("成为群首");
            }

            @Override
            public void notLeader() {
                System.out.println("未成为群首");
            }
        };
        latch.addListener(listener);
        latch.start();
        Thread.sleep(100000);
        client.close();

    }
}
