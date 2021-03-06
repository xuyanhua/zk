package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * 同步选主
 *
 * @author xuyanhua
 * @description:
 * @date 2018/5/29 下午4:09
 */
public class Master {
    ZKClient zkClient = ZKClient.instance();
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    static Logger logger = LoggerFactory.getLogger(Master.class);

    boolean isLeader = false;

    public static void main(String[] args) throws InterruptedException {
        Master master = new Master();
        master.runForMaster();
        logger.info("isLeader-->" + master.isLeader);
        Thread.sleep(60000);
    }

    /**
     * 执行群首选举操作
     *
     * @return
     */
    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                logger.info("try election leader .");
                zkClient.create(Consts.MASTER_PATH, serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                //创建成功，成为群首
                isLeader = true;
                break;
            } catch (KeeperException.NodeExistsException e) {
                //如果群首节点存在，返回false
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e) {

            } catch (Exception e) {
                e.printStackTrace();
            }
            //如果出现这种异常，要检查是否已有群首
            if (checkExistsMaster()) {
                //如果存在群首，就退出
                break;
            }
            //如果群首不存在，继续竞选群首
        }

    }

    /**
     * 检查有没有群首
     *
     * @return
     */
    boolean checkExistsMaster() throws InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] data = zkClient.getData(Consts.MASTER_PATH, false, stat);
            this.isLeader = serverId.equals(new String(data));
            return true;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (Exception e) {

        }
        //出现其他异常时继续检查
        return checkExistsMaster();
    }
}
