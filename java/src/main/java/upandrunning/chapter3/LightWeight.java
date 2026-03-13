package upandrunning.chapter3;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class LightWeight {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet productSet = DataSet.of("test", "products");

            // upsert a record
            session.upsert(productSet.id(10013)).expireRecordAfterSeconds(100)
                    .bin("name").setTo("Stylish Couch")
                    .execute();

            // ----Touch--------------------------------------------------------------------------------

            // touch the product with id of 10013 and set the expiry to 60 seconds
            session.touch(productSet.id(10013))
                    .expireRecordAfterSeconds(60)
                    .execute();

            // ----Exists---(does-not-work)-----------------------------------------------------------------------------
            System.out.println(session.exists(productSet.id(10013)).execute().equals(true));
            if (session.exists(productSet.id(10013)).execute().equals(true)) {
                System.out.println("Record with id of 10013 exists");
            } else {
                System.out.println("Record with id of 10013 does not exist");
            }

        } finally {
            cluster.close();
        }

    }
}
