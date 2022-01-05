package keeper;

import org.apache.zookeeper.common.StringUtils;

import java.util.ArrayList;
import java.util.List;

public enum Nodes {
    INSTANCE;

    private String currentLeader = "";
    private List<String> nodes = new ArrayList<>();

    public String getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(String currentLeader) {
        this.currentLeader = currentLeader;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public void addNode(String node) {
        nodes.add(node);
    }

    public void setNodes(List<String> nodes) {
        this.nodes = nodes;
    }
}
