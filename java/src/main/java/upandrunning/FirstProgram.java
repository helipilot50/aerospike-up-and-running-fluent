package upandrunning;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

/**
 * Getting started with the Aerospike Fluent Java client.
 *
 * Connects to a local Aerospike node, writes a record, reads it back, then
 * deletes it. Run with a local Aerospike server on localhost:3000.
 */
public class FirstProgram {

    public static void main(String[] args) throws Exception {
        String host = "192.168.18.166";
        int port = 43000;

        System.out.println("Connecting to Aerospike at " + host + ":" + port);

        try (Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);

            DataSet demo = DataSet.of("test", "item");

            // ── Write ──────────────────────────────────────────────────────
            session.upsert(demo.id(1))
                    .bin("name").setTo("Stylish Couch")
                    .bin("cost").setTo(50000f)
                    .bin("discount").setTo(0.21f)
                    .execute();

            // ── Read ───────────────────────────────────────────────────────
            try (RecordStream stream = session.query(demo.id(1)).execute()) {
                RecordResult result = stream.next();
                if (result != null && result.isOk() && result.recordOrNull() != null) {
                    System.out.println(String.format(" %s costs $%.0f with a %d%% discount\n",
                            result.recordOrNull().getValue("name"),
                            result.recordOrNull().getValue("cost"),
                            (int) (Double.parseDouble(result.recordOrNull().getValue("discount").toString()) * 100)));

                } else {
                    System.out.println("    Record not found.");
                }
            }

            // ── Delete ─────────────────────────────────────────────────────
            session.delete(demo.id(1)).execute();
        }
    }
}
