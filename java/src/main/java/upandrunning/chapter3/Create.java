package upandrunning.chapter3;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class Create {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;

        System.out.println("Connecting to Aerospike at " + host + ":" + port);

        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet demo = DataSet.of("test", "item");
            session.insert(demo.id(10012)).expireRecordAfterSeconds(100)
                    .bin("name").setTo("Stylish Couch")
                    .execute();
        } finally {
            cluster.close();
        }
    }
}
