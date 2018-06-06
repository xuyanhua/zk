package com.zk.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/6/5 上午7:18
 */
public class CuratorTest1 {
    public static void main(String[] args) throws Exception {
        String path = "/curators";
        String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
        //监听器
        CuratorListener listener = new CuratorListener() {

            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println("发生了事件：" + event);
            }
        };
        client.getCuratorListenable().addListener(listener);
        //必须有start
        client.start();
        //创建、设置数据、删除
        client.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path, "".getBytes());
        client.setData().forPath(path, "hello".getBytes());
        client.create().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path + "/test1", "".getBytes());
        client.delete().forPath(path + "/test1");
        client.delete().forPath(path);
        //异步回调
        BackgroundCallback bc = new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                int rc = event.getResultCode();
                System.out.println(KeeperException.Code.get(rc));
            }
        };
        //异步执行器
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("任务完成");
            }
        });
        client.create().withMode(CreateMode.EPHEMERAL).inBackground(bc, executor).forPath(path, new byte[1024]);
        Thread.sleep(10000);
        client.close();
    }
}
