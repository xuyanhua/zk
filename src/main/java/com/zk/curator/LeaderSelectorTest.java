package com.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/6/6 下午5:05
 */
public class LeaderSelectorTest {
    private final static String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retry);
        client.start();

        LeaderSelectorListener listener = new LeaderSelectorListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                System.out.println("listener:" + newState);
            }

            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                System.out.println("takeLeadership");
            }
        };
        LeaderSelector selector = new LeaderSelector(client, "/curator/master", listener);
        selector.start();
        Thread.sleep(100000);
        client.close();

    }
}
