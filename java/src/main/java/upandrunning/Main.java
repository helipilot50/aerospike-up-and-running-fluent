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
public class Main {

    public static void main(String[] args) throws Exception {
        String host = "192.168.18.166";
        int port = 43000;

        System.out.println("Connecting to Aerospike at " + host + ":" + port);

        try (Cluster cluster = new ClusterDefinition(host, port).connect()) {
            Session session = cluster.createSession(Behavior.DEFAULT);

            DataSet demo = DataSet.of("test", "upandrunning");

            // ── Write ──────────────────────────────────────────────────────
            System.out.println("\n[1] Writing record key=1");
            session.upsert(demo.id(1))
                    .bin("message").setTo("Hello from Aerospike Fluent!")
                    .bin("language").setTo("Java")
                    .bin("version").setTo(1L)
                    .execute();
            System.out.println("    Written.");

            // ── Read ───────────────────────────────────────────────────────
            System.out.println("\n[2] Reading record key=1");
            try (RecordStream stream = session.query(demo.id(1)).execute()) {
                RecordResult result = stream.next();
                if (result != null && result.isOk() && result.recordOrNull() != null) {
                    System.out.println("    message  : " + result.recordOrNull().getValue("message"));
                    System.out.println("    language : " + result.recordOrNull().getValue("language"));
                    System.out.println("    version  : " + result.recordOrNull().getValue("version"));
                } else {
                    System.out.println("    Record not found.");
                }
            }

            // ── Delete ─────────────────────────────────────────────────────
            System.out.println("\n[3] Deleting record key=1");
            session.delete(demo.id(1)).execute();
            System.out.println("    Deleted.");

            System.out.println("\nDone.");
        }
    }
}
