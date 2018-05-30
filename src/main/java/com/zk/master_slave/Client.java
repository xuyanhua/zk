package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/5/30 下午5:25
 */
public class Client {
    ZKClient zkClient = ZKClient.instance();
    static Logger logger = LoggerFactory.getLogger(Client.class);

    String queueCommand(String command) throws KeeperException, InterruptedException {
        while (true) {
            try {
                String name = zkClient.create(Consts.TASK_PATH_PREFIX, command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                logger.info("task create successfully." + name);
                return name;
            } catch (KeeperException.NodeExistsException e) {
                logger.error("command already exists:" + command);
            } catch (KeeperException.ConnectionLossException e) {

            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        String name = client.queueCommand("aaa");
        logger.info("Created " + name);
    }
}
