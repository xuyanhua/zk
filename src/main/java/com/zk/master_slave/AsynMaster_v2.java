package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 1、异步选主，2、创建元数据
 *
 * @author xuyanhua
 * @description:
 * @date 2018/5/29 下午4:09
 */
public class AsynMaster_v2 {
    ZKClient zkClient = ZKClient.instance();
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    static Logger logger = LoggerFactory.getLogger(AsynMaster_v2.class);
    boolean isLeader = false;

    public static void main(String[] args) throws InterruptedException {
        AsynMaster_v2 master = new AsynMaster_v2();
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
                    default:
                        isLeader = false;
                }
                logger.info("isLeader-->" + isLeader);

            }
        };
        zkClient.create(Consts.MASTER_PATH, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, sc, null);
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
