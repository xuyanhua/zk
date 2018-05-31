package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 1、异步选主，2、创建元数据，3、能处理主节点崩溃的情况
 *
 * @author xuyanhua
 * @description:
 * @date 2018/5/29 下午4:09
 */
public class AsynMaster_v3 {
    ZKClient zkClient = ZKClient.instance();
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    static Logger logger = LoggerFactory.getLogger(AsynMaster_v3.class);
    boolean isLeader = false;

    public static void main(String[] args) throws InterruptedException {
        AsynMaster_v3 master = new AsynMaster_v3();
        master.bootstrap();
        master.runForMaster();
        Thread.sleep(60000);
    }

    void bootstrap() {
        //工作进程
        this.createParent("/workers", new byte[0]);
        //
        this.createParent("/assign", new byte[0]);
        //任务进程
        this.createParent("/tasks", new byte[0]);
        //任务完成状态进程
        this.createParent("/status", new byte[0]);
    }

    void createParent(String path, final byte[] data) {
        zkClient.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        //重试
                        createParent(path, (byte[]) ctx);
                        break;
                    case OK:
                        logger.info("Parent created.");
                        break;
                    case NODEEXISTS:
                        logger.warn("Parent already registered:" + path);
                        break;
                    default:
                        logger.error("Something went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
                }
            }
        }, data);
    }

    /**
     * 执行群首选举操作
     *
     * @return
     */
    void runForMaster() {
        logger.info("try election leader .");
        AsyncCallback.StringCallback sc = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkExistsMaster();
                        return;
                    case OK:
                        isLeader = true;
                        break;
                    case NODEEXISTS:
                        masterExists();
                        break;
                    default:
                        isLeader = false;
                }
                logger.info("isLeader-->" + isLeader);

            }
        };
        zkClient.create(Consts.MASTER_PATH, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, sc, null);
    }

    /**
     * 设置监视点
     */
    private void masterExists() {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        };
        AsyncCallback.StatCallback cb = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        masterExists();
                        break;
                    case OK:
                        if (stat == null) {
                            //如果不存在
                            runForMaster();
                        }
                        break;
                    default:
                        checkExistsMaster();
                        break;
                }
            }
        };
        zkClient.exists(Consts.MASTER_PATH, watcher, cb, null);
    }


    /**
     * 检查有没有群首
     *
     * @return
     */
    void checkExistsMaster() {
        AsyncCallback.DataCallback cb = new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                switch (KeeperException.Code.get(rc)) {
                    case CONNECTIONLOSS:
                        checkExistsMaster();
                        break;
                    case NONODE:
                        runForMaster();
                        return;
                    default:
                        ;
                }
            }
        };
        zkClient.getData(Consts.MASTER_PATH, false, cb, null);
    }
}
