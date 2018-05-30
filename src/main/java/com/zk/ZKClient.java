package com.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/5/29 上午11:12
 */
public class ZKClient {
    private final static String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";
    private final static int sessionTimeout = 150;
    private ZooKeeper zk = null;
    private static ZKClient zkClient = null;

    private ZKClient() {
        try {
            this.zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println(watchedEvent);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static synchronized ZKClient instance() {
        if (zkClient == null) {
            zkClient = new ZKClient();
        }
        return zkClient;
    }

    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        zk.create(path, data, acl, createMode);
    }


    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        zk.create(path, data, acl, createMode, cb, ctx);
    }

    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return zk.getData(path, watch, stat);
    }

    public void getData(final String path, Watcher watcher,
                        AsyncCallback.DataCallback cb, Object ctx) {
        zk.getData(path, watcher, cb, ctx);
    }
    public void getData(final String path, boolean watch,
                        AsyncCallback.DataCallback cb, Object ctx) {
        zk.getData(path, watch, cb, ctx);
    }


    public void close() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
