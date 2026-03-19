package upandrunning.chapter4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.policy.Behavior;

// TODO: Add list operations
// rank, index, value

public class ListOperate {

    static Map<String, Object> product(int id, String name, String category, double price, int stock, double rating,
            String brand) {
        Map<String, Object> p = new HashMap<>();
        p.put("id", id);
        p.put("name", name);
        p.put("category", category);
        p.put("price", price);
        p.put("stock", stock);
        p.put("rating", rating);
        p.put("brand", brand);
        return p;
    }

    static final List<Map<String, Object>> products = Arrays.asList(
            product(1, "Wireless Noise-Cancelling Headphones", "Electronics", 249.99, 45, 4.7, "SoundWave"),
            product(2, "Mechanical Keyboard", "Electronics", 129.99, 78, 4.5, "KeyMaster"),
            product(3, "Ergonomic Office Chair", "Furniture", 399.00, 20, 4.6, "ComfortPro"),
            product(4, "Stainless Steel Water Bottle", "Sports", 29.99, 200, 4.8, "HydroFlow"),
            product(5, "Yoga Mat", "Sports", 45.00, 150, 4.4, "ZenFlex"),
            product(6, "4K Webcam", "Electronics", 89.99, 60, 4.3, "ClearView"),
            product(7, "Standing Desk", "Furniture", 599.00, 15, 4.7, "RiseUp"),
            product(8, "Bluetooth Speaker", "Electronics", 79.99, 95, 4.5, "SoundWave"),
            product(9, "Coffee Grinder", "Kitchen", 59.99, 110, 4.6, "BrewMaster"),
            product(10, "Cast Iron Skillet", "Kitchen", 44.99, 130, 4.9, "IronChef"),
            product(11, "Air Purifier", "Home", 149.99, 40, 4.5, "CleanAir"),
            product(12, "LED Desk Lamp", "Home", 39.99, 175, 4.4, "BrightSpace"),
            product(13, "Portable Charger 20000mAh", "Electronics", 49.99, 220, 4.6, "PowerUp"),
            product(14, "Running Shoes", "Sports", 119.99, 85, 4.5, "StridePro"),
            product(15, "Resistance Bands Set", "Sports", 24.99, 300, 4.7, "ZenFlex"),
            product(16, "Smart Watch", "Electronics", 199.99, 55, 4.4, "TimeTech"),
            product(17, "French Press Coffee Maker", "Kitchen", 34.99, 140, 4.8, "BrewMaster"),
            product(18, "Bookshelf 5-Tier", "Furniture", 189.00, 25, 4.3, "WoodCraft"),
            product(19, "Bamboo Cutting Board", "Kitchen", 22.99, 250, 4.6, "IronChef"),
            product(20, "USB-C Hub 7-in-1", "Electronics", 54.99, 100, 4.5, "PowerUp"),
            product(21, "Foam Roller", "Sports", 19.99, 180, 4.4, "ZenFlex"),
            product(22, "Scented Soy Candles Set", "Home", 32.00, 160, 4.7, "AromaBliss"),
            product(23, "Electric Toothbrush", "Personal Care", 69.99, 90, 4.6, "BrightSmile"),
            product(24, "Noise Machine", "Home", 49.99, 70, 4.5, "CleanAir"),
            product(25, "Laptop Stand Adjustable", "Electronics", 44.99, 120, 4.6, "RiseUp"),
            product(26, "Insulated Lunch Bag", "Sports", 27.99, 210, 4.4, "HydroFlow"),
            product(27, "Wireless Mouse", "Electronics", 39.99, 145, 4.5, "KeyMaster"),
            product(28, "Aromatherapy Diffuser", "Personal Care", 36.99, 115, 4.6, "AromaBliss"),
            product(29, "Shower Head with Filter", "Personal Care", 54.99, 80, 4.7, "BrightSmile"),
            product(30, "Monitor Light Bar", "Electronics", 64.99, 65, 4.5, "BrightSpace"));

    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("HOST", "127.0.0.1");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "3000"));
        System.out.println("Connecting to Aerospike at " + host + ":" + port);
        Cluster cluster = new ClusterDefinition(host, port).clusterName("pdm").connect();
        try {
            Session session = cluster.createSession(Behavior.DEFAULT);
            DataSet sampleSet = DataSet.of("test", "sample");

            List<Integer> list = List.of(1, 3, 4, 7, 9, 11, 26);

            // Simple numeric list
            // insert the list into the list bin of the record with id of 1
            session.upsert(sampleSet.id(1)).bin("list").setTo(list).execute();

            session.query(sampleSet.id(1))
                    .bin("list").get()
                    .execute()
                    .getFirst().ifPresent(result -> {
                        System.out.println("\nList of numbers:\n" + result.recordOrNull().getList("list") + "\n");
                    });

            // list of products
            session.upsert(sampleSet.id(2)).bin("products").setTo(products).execute();

            // fetch the 7th product in the list (index 6)
            session.query(sampleSet.id(2))
                    .bin("products").onListIndex(6).getValues()
                    .execute()
                    .getFirst().ifPresent(result -> {
                        System.out.println("7th product:\n" + result.recordOrNull().getMap("products") + "\n");
                    });

        } finally {
            cluster.close();
        }
    }
}
