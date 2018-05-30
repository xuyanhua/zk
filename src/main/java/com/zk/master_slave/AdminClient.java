package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/5/30 下午5:54
 */
public class AdminClient {
    static Logger logger = LoggerFactory.getLogger(AdminClient.class);

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKClient zkClient = ZKClient.instance();
        try {
            Stat stat = new Stat();
            byte[] data = zkClient.getData(Consts.MASTER_PATH, false, stat);
            Date startDate = new Date(stat.getCtime());
            logger.info("master:" + new String(data) + ",since:" + startDate);
        } catch (KeeperException.NoNodeException e) {
            logger.error("No Master.");
        }


        logger.info("Workers:");
        List<String> children = zkClient.getChildren(Consts.WORKERS_PATH, false);
        for (String w : children) {
            byte[] data = zkClient.getData(Consts.WORKERS_PATH + "/" + w, false, null);
            logger.info("\t" + w + ":" + new String(data));
        }


        logger.info("Tasks:");
        List<String> children1 = zkClient.getChildren(Consts.TASKS_PATH, false);
        for (String t : children1) {
            logger.info("\t" + t);
        }

    }
}
