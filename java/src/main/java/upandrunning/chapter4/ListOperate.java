package upandrunning.chapter4;

import java.util.List;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

// TODO: Add list operations
// rank, index, value

public class ListOperate {
    public static void main(String[] args) {
        String host = "192.168.18.166";
        int port = 43000;
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();
        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet sampleSet = DataSet.of("test", "sample");

            List<Integer> list = List.of(1, 3, 4, 7, 9, 11, 26);

            // insert the list into the list bin of the record with id of 1
            session.upsert(sampleSet.id(1)).bin("list").setTo(list).execute();

            session.query(sampleSet.id(1))
                    .bin("list").get()
                    .execute()
                    .getFirst().ifPresent(result -> {
                        System.out.println("Original list: " + result);
                    });

        } finally {
            cluster.close();
        }
    }
}
