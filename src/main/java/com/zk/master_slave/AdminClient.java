package com.zk.master_slave;

import com.zk.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @author xuyanhua
 * @description:
 * @date 2018/5/30 下午5:54
 */
public class AdminClient {

    public static void main(String[] args) throws KeeperException, InterruptedException {
        ZKClient zkClient = ZKClient.instance();
        Stat stat = new Stat();
        zkClient.getData(Consts.MASTER_PATH,false,stat);
        List<String> children = zkClient.getChildren(Consts.MASTER_PATH, false);

    }
}
