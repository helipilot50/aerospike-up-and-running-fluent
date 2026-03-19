package upandrunning.chapter3;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class Update {
    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "3000"));
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet productSet = DataSet.of("test", "products");

            // ----Key-Value-Update----------------------------------------------------------------------------------

            // update a record with a single bin with id of 10012 to add a new bin called
            // price
            // the record expires after 100 seconds
            session.update(productSet.id(10012)).expireRecordAfterSeconds(100)
                    .bin("price").setTo(50000f)
                    .execute();

            // Read the product with id of 10012 back
            try (RecordStream stream = session.query(productSet.id(10012)).execute()) {
                RecordResult result = stream.next();
                if (result != null && result.isOk() && result.recordOrNull() != null) {
                    System.out.println(result);
                }
            }

            // aql> select * from test.products where pk = 10012
            // +-------+-----------------+-------+
            // | PK | name | price |
            // +-------+-----------------+-------+
            // | 10012 | "Stylish Couch" | 50000 |
            // +-------+-----------------+-------+
            // 1 row in set (0.000 secs)

            // ----Document-Ureate---------------------------------------------------------------------------------

            // insert the product with id of 10013 as a document to add a new bin called
            // price
            // the record expires after 100 seconds
            session.update(productSet.id(10013)).expireRecordAfterSeconds(100)
                    .bin("price").setTo(50000f)
                    .execute();

            // Read the product back with id of 10013
            try (RecordStream stream = session.query(productSet.id(10013)).execute()) {
                RecordResult result = stream.next();
                if (result != null && result.isOk() && result.recordOrNull() != null) {
                    Map retrivedMap = (Map) result.recordOrNull().getValue("product");
                    System.out.println(retrivedMap);
                }
            }

            // aql> select * from test.products where pk = 10013
            // +-------+--------------------------------------------------------------------------+-------+
            // | PK | product | price |
            // +-------+--------------------------------------------------------------------------+-------+
            // | 10013 | MAP('{"name":"Stylish Couch", "productId":"10013",
            // "purchasable":true}') | 50000 |
            // +-------+--------------------------------------------------------------------------+-------+
            // 1 row in set (0.001 secs)

        } finally {
            cluster.close();
        }
    }
}
