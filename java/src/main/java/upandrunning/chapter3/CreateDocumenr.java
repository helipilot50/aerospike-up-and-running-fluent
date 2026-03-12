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

public class CreateDocumenr {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);

        // Connect to the cluster and create a session
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();
        Session session = cluster.createSession(Behavior.DEFAULT);

        // Define the namespace and set for our products
        DataSet productSet = DataSet.of("test", "product");

        try {

            // Define our product as a map
            Map<String, Object> product = new HashMap<>();
            product.put("name", "Stylish Couch");
            product.put("productId", "10012");
            product.put("purchasable", true);
            // Upsert the product document into Aerospike
            session.insert(productSet.id(10012)).expireRecordAfterSeconds(100)
                    .bin("product").setTo(product)
                    .execute();

            // Read the product back from Aerospike
            try (RecordStream stream = session.query(productSet.id(10012)).execute()) {
                RecordResult result = stream.next();
                if (result != null && result.isOk() && result.recordOrNull() != null) {
                    Map retrivedMap = (Map) result.recordOrNull().getValue("product");
                    System.out.println(retrivedMap);
                }
            }
        } finally {
            cluster.close();
        }
    }
}
