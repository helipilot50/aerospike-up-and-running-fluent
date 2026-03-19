package upandrunning.chapter3;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class Delete {
    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "3000"));
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet productSet = DataSet.of("test", "products");

            // ----Delete----------------------------------------------------------------------------------

            // delete the record with id of 10012
            session.delete(productSet.id(10012)).execute();

            // aql> select * from test.products where pk = 10012
            // Error: (2) 192.168.18.166:43000 AEROSPIKE_ERR_RECORD_NOT_FOUND

            // ----Touch--------------------------------------------------------------------------------

            // touch the product with id of 10013 and set the expiry to 60 seconds
            session.touch(productSet.id(10013))
                    .expireRecordAfterSeconds(60)
                    .execute();

            // ----Exists--------------------------------------------------------------------------------
            if (session.exists(productSet.id(10013)).execute().getFirst().equals(true)) {
                System.out.println("Record with id of 10013 exists");
            } else {
                System.out.println("Record with id of 10013 does not exist");
            }

        } finally {
            cluster.close();
        }

    }
}
