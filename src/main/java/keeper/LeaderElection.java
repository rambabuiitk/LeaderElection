package keeper;

import static common.LeaderElectionConstants.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private ZooKeeper zooKeeper;
    
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeCreated:
                try {
                    volunteerForLeadership();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
                break;
            case NodeDeleted:
                try {
                    reelectLeader(event);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
                break;
            case NodeDataChanged:
                System.out.println("Leader updated progress of task");
                break;
        }
    }

    public void reelectLeader(WatchedEvent watchedEvent) throws InterruptedException, KeeperException {
        Stat nodeStat = null;
        String affectedNode = watchedEvent.getPath()
                .replace(ELECTION_NAMESPACE + "/", "");
        System.out.println("Some node crashed " + affectedNode + "...");
        if (!Nodes.INSTANCE.getCurrentLeader().equalsIgnoreCase(affectedNode)) {
            System.out.println("No change in leader, some member nodes got partitioned or crashed");
            Nodes.INSTANCE.getNodes().remove(affectedNode);
            return;
        }

        System.out.println("needs re-election");

        System.out.println("Getting the volunteers or nominees...");

        List<String> nodes = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(nodes);

        if (StringUtils.isEmpty(Nodes.INSTANCE.getCurrentLeader())) {
            String currentNode = nodes.get(0)
                    .replace(ELECTION_NAMESPACE + "/", "");
            Nodes.INSTANCE.setCurrentLeader(currentNode);
            System.out.println("Current Leader is " + Nodes.INSTANCE.getCurrentLeader());
        }

        Nodes.INSTANCE.setCurrentLeader(nodes.get(0)
                .replace(ELECTION_NAMESPACE + "/", ""));
        System.out.println("Successful re-election. Elected " +
                Nodes.INSTANCE.getCurrentLeader());
        nodeStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + Nodes.INSTANCE.getCurrentLeader(),
                this);
    }

    public void connect() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        Stat stat = zooKeeper.exists(ELECTION_NAMESPACE, false);
        if (stat == null) {
            zooKeeper.create(ELECTION_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Nodes.INSTANCE.addNode(znodeFullPath.replace(ELECTION_NAMESPACE + "/", ""));
    }

    public void electLeader() throws KeeperException, InterruptedException {
        Stat nodeStat = null;
        List<String> nodes = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(nodes);
        if (StringUtils.isEmpty(Nodes.INSTANCE.getCurrentLeader())) {
            String currentNode = nodes.get(0).replace(ELECTION_NAMESPACE + "/", "");
            Nodes.INSTANCE.setCurrentLeader(currentNode);
            if (!Nodes.INSTANCE.getNodes().contains(currentNode)) {
                Nodes.INSTANCE.addNode(currentNode);
            }
        }

        if (Nodes.INSTANCE.getCurrentLeader() == nodes.get(0)) {
            System.out.println("Leader is " + Nodes.INSTANCE.getCurrentLeader());
        }

        nodeStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + Nodes.INSTANCE.getCurrentLeader(),
                this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
