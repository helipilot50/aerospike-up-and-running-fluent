package upandrunning.chapter4;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

public class Expressions {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();
        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet cartSet = DataSet.of("test", "cart");
            DataSet expressionsSet = DataSet.of("test", "expressions");

            // Filter expressions
            // set the state of the record with id of 3.14159f to PROCESSING
            session.upsert(expressionsSet.id("3.14159f"))
                    .bin("state").setTo("PROCESSING")
                    .execute();
            // some time later, we want to update the state to COMPLETE but only if the
            // current state is PROCESSING
            session.update(expressionsSet.id("3.14159f"))
                    .bin("state").setTo("COMPLETE")
                    .where("$.state == 'PROCESSING'")
                    .execute();

            // Trilean Logic
            // reset to PROCESSING
            session.upsert(expressionsSet.id("3.14159f"))
                    .bin("state").setTo("PROCESSING")
                    .execute();
            // some time later, we want to update the state to COMPLETE but only if the
            // current state is PROCESSING and the last update is less than or equal to 1
            // day ago
            session.update(expressionsSet.id("3.14159f"))
                    .bin("state").setTo("COMPLETE")
                    .where("$.state == 'PROCESSING' and $.lastUpdate <= 86400")
                    .execute();

            // Query expressions

            // Write expressions

        } finally {
            cluster.close();
        }
    }
}
