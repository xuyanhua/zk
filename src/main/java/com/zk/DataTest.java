package com.zk;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/6/4 下午5:15
 */
public class DataTest {
    final static byte[] SIZE_1MB = new byte[1024 * 1024];
    final static byte[] SIZE_1MB_LESS = new byte[1024 * 1023];
    final static byte[] SIZE_2MB = new byte[2 * 1024 * 1024];
    final static ZKClient zkClient = ZKClient.instance();


    public static void main(String[] args) throws KeeperException, InterruptedException {
//        testMaxSizeData();
        testMaxChildren();
    }

    public static void testMaxSizeData() throws KeeperException, InterruptedException {
        if (!zkClient.exists("/test", null)) {
            zkClient.create("/test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (!zkClient.exists("/test/testmaxdata", null)) {
            zkClient.create("/test/testmaxdata", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        }
        System.out.println("设置数据...");
        //测试发现，向一个节点传输大于1M数据时，会直接报CONNECTIONLOSS异常
        zkClient.setData("/test/testmaxdata", SIZE_1MB, -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                System.out.println(KeeperException.Code.get(rc));
            }
        }, null);

        Thread.sleep(100000);
    }

    public static void testMaxChildren() throws KeeperException, InterruptedException {
        if (!zkClient.exists("/test", null)) {
            zkClient.create("/test", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        if (!zkClient.exists("/test/testmaxchildren", null)) {
            zkClient.create("/test/testmaxchildren", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        }
        System.out.println("添加子节点..");
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            zkClient.create("/test/testmaxchildren/x_" + i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("x_" + i);
        }

        Thread.sleep(100000);
    }
}
