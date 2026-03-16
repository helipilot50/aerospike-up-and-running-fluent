package upandrunning.chapter4;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class MapOperate {

    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();
        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet productSet = DataSet.of("test", "cart");
            Map<String, Object> product = new HashMap<>();
            product.put("name", "Stylish Couch");
            product.put("productId", "10013");
            product.put("purchasable", true);
        } finally {
            cluster.close();
        }
    }
}
