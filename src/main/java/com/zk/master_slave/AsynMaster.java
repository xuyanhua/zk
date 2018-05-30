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
 * 异步选主
 *
 * @author xuyanhua
 * @description:
 * @date 2018/5/29 下午4:09
 */
public class AsynMaster {
    ZKClient zkClient = ZKClient.instance();
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    static Logger logger = LoggerFactory.getLogger(AsynMaster.class);
    final static String MASTER_PATH = "/master";
    boolean isLeader = false;

    public static void main(String[] args) throws InterruptedException {
        AsynMaster master = new AsynMaster();
        master.runForMaster();
        Thread.sleep(60000);
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
        zkClient.create(MASTER_PATH, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, sc, null);
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
        zkClient.getData(MASTER_PATH, false, cb, null);
    }
}
