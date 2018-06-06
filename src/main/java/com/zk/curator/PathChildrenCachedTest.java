package com.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/6/6 下午5:21
 */
public class PathChildrenCachedTest {
    private final static String connectString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retry);
        client.start();

        final PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/curator/children", true);


        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                boolean change = false;
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED ||
                        event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED ||
                        event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    change = true;
                } else {
                    return;
                }
                if (change) {
                    System.out.println(event.getType() + ":" + event.getData());
                }
            }
        };
        pathChildrenCache.getListenable().addListener(listener);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        Thread.sleep(100000);
        client.close();
    }
}
