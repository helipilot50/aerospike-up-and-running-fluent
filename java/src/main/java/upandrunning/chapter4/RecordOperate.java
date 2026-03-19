package upandrunning.chapter4;

import java.util.Optional;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class RecordOperate {
    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "3000"));
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet cartSet = DataSet.of("test", "cart");

            addItemToCart(session, cartSet, 1, "shoes", 50.0);
            addItemToCart(session, cartSet, 1, "shirt", 20.0);
            addItemToCart(session, cartSet, 1, "jeans", 40.0);

            addItemToCartWithCost(session, cartSet, 1, "hat", 15.99);
        } finally {
            cluster.close();
        }
    }

    private static void addItemToCart(Session session, DataSet cartSet, int cartId, String itemDescr, double itemCost) {

        // upsert the item description to the items bin, add 1 to the totalItems bin and
        // add the cost to the cost bin
        // if the record doesn't exist, it will be created with the item description in
        // the items bin, 1 in the totalItems bin and the cost in the cost bin
        // if the record already exists, the item description will be appended to the
        // items bin, 1 will be added to the totalItems bin and the cost will be added
        // to the cost bin
        // the record be returned after the update with the new values of the bins

        Optional<RecordResult> result = session.upsert(cartSet.id(cartId))
                .bin("items").append(itemDescr + ",")
                .bin("totalItems").add(1)
                .bin("cost").add(itemCost)

                .execute().getFirst();

        if (result.isPresent() && result.get().recordOrNull() != null) {
            System.out.println(result);
        }

    }

    private static void addItemToCartWithCost(Session session, DataSet cartSet, int cartId, String itemDescr,
            double itemCost) {

        Optional<RecordResult> result = session.upsert(cartSet.id(cartId))
                .bin("items").append(itemDescr + ",")
                .bin("totalItems").add(1)
                .bin("cost").add(itemCost)
                .bin("cost").get()
                .execute().getFirst(); // TODO

        if (result.isPresent() && result.get().recordOrNull() != null) {
            System.out.println("New cost: " + result.get().recordOrNull());
        }

    }

}
