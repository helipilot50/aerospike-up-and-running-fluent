package upandrunning.chapter3;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class Batch {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();

        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet productSet = DataSet.of("test", "products");

            // upsert many records with a batch write
            session.insert(productSet.ids(1, 2, 3, 4, 5))
                    .expireRecordAfterSeconds(100)
                    .bin("name").setTo("Fred")
                    .bin("age").setTo(30)
                    .bin("value").setTo(10)
                    .execute();
            // read many records with a batch read
            session.query(productSet).execute().forEach(rec -> System.out.println(rec));

            // batch operations can be chained together and executed as a single batch
            session.insert(productSet.ids(6, 7, 8))
                    .expireRecordAfterSeconds(100)
                    .bin("name").setTo("Wilma")
                    .bin("age").setTo(33)
                    .bin("value").setTo(20)
                    .update(productSet.id(2))
                    .bin("value").add(5)
                    .delete(productSet.id(1))
                    .execute();
            // read many records with a batch read
            session.query(productSet).execute().forEach(rec -> System.out.println(rec));

            // aql> select * from test.products
            // +----+---------+-----+-------+
            // | PK | name | age | value |
            // +----+---------+-----+-------+
            // | 5 | "Fred" | 30 | 10 |
            // | 8 | "Wilma" | 33 | 20 |
            // | 2 | "Fred" | 30 | 15 |
            // | 4 | "Fred" | 30 | 10 |
            // | 7 | "Wilma" | 33 | 20 |
            // | 3 | "Fred" | 30 | 10 |
            // | 6 | "Wilma" | 33 | 20 |
            // +----+---------+-----+-------+
            // 7 rows in set (0.033 secs)

        } finally {
            cluster.close();
        }

    }
}
