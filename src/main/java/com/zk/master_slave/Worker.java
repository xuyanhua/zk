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
 * 工作节点-从节点
 *
 * @author xuyanhua
 * @description:
 * @date 2018/5/30 下午3:42
 */
public class Worker {
    ZKClient zkClient = ZKClient.instance();
    Random random = new Random();
    String serverId = Integer.toHexString(random.nextInt());
    static Logger logger = LoggerFactory.getLogger(Worker.class);
    final String WORKER_PATH = "/workers/worker-" + serverId;
    String status = null;

    void regiester() {
        RegisterCallback registerCallback = new RegisterCallback();
        zkClient.create(WORKER_PATH, "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, registerCallback, null);
    }

    class RegisterCallback implements AsyncCallback.StringCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    regiester();
                    break;
                case OK:
                    logger.info("Registered successfully:" + serverId);
                    break;
                case NODEEXISTS:
                    logger.info("Already registered:" + serverId);
                    break;
                default:
                    logger.error("Something went wrong:", KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    }

    void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    synchronized private void updateStatus(String status) {
        if (this.status == status) {
            UpdateStatusCallback statusCallback = new UpdateStatusCallback();
            zkClient.setData(WORKER_PATH, status.getBytes(), -1, statusCallback, status);
        }
    }

    class UpdateStatusCallback implements AsyncCallback.StatCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.regiester();
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
