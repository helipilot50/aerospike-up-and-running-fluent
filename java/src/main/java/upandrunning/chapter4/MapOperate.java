package upandrunning.chapter4;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.fluent.Cluster;
import com.aerospike.client.fluent.ClusterDefinition;
import com.aerospike.client.fluent.DataSet;
import com.aerospike.client.fluent.RecordResult;
import com.aerospike.client.fluent.RecordStream;
import com.aerospike.client.fluent.Session;
import com.aerospike.client.fluent.cdt.ListOrder;
import com.aerospike.client.fluent.policy.Behavior;

/* product list for testing map operations
[
  { "id": 1, "name": "Wireless Noise-Cancelling Headphones", "category": "Electronics", "price": 249.99, "stock": 45, "rating": 4.7, "brand": "SoundWave" },
  { "id": 2, "name": "Mechanical Keyboard", "category": "Electronics", "price": 129.99, "stock": 78, "rating": 4.5, "brand": "KeyMaster" },
  { "id": 3, "name": "Ergonomic Office Chair", "category": "Furniture", "price": 399.00, "stock": 20, "rating": 4.6, "brand": "ComfortPro" },
  { "id": 4, "name": "Stainless Steel Water Bottle", "category": "Sports", "price": 29.99, "stock": 200, "rating": 4.8, "brand": "HydroFlow" },
  { "id": 5, "name": "Yoga Mat", "category": "Sports", "price": 45.00, "stock": 150, "rating": 4.4, "brand": "ZenFlex" },
  { "id": 6, "name": "4K Webcam", "category": "Electronics", "price": 89.99, "stock": 60, "rating": 4.3, "brand": "ClearView" },
  { "id": 7, "name": "Standing Desk", "category": "Furniture", "price": 599.00, "stock": 15, "rating": 4.7, "brand": "RiseUp" },
  { "id": 8, "name": "Bluetooth Speaker", "category": "Electronics", "price": 79.99, "stock": 95, "rating": 4.5, "brand": "SoundWave" },
  { "id": 9, "name": "Coffee Grinder", "category": "Kitchen", "price": 59.99, "stock": 110, "rating": 4.6, "brand": "BrewMaster" },
  { "id": 10, "name": "Cast Iron Skillet", "category": "Kitchen", "price": 44.99, "stock": 130, "rating": 4.9, "brand": "IronChef" },
  { "id": 11, "name": "Air Purifier", "category": "Home", "price": 149.99, "stock": 40, "rating": 4.5, "brand": "CleanAir" },
  { "id": 12, "name": "LED Desk Lamp", "category": "Home", "price": 39.99, "stock": 175, "rating": 4.4, "brand": "BrightSpace" },
  { "id": 13, "name": "Portable Charger 20000mAh", "category": "Electronics", "price": 49.99, "stock": 220, "rating": 4.6, "brand": "PowerUp" },
  { "id": 14, "name": "Running Shoes", "category": "Sports", "price": 119.99, "stock": 85, "rating": 4.5, "brand": "StridePro" },
  { "id": 15, "name": "Resistance Bands Set", "category": "Sports", "price": 24.99, "stock": 300, "rating": 4.7, "brand": "ZenFlex" },
  { "id": 16, "name": "Smart Watch", "category": "Electronics", "price": 199.99, "stock": 55, "rating": 4.4, "brand": "TimeTech" },
  { "id": 17, "name": "French Press Coffee Maker", "category": "Kitchen", "price": 34.99, "stock": 140, "rating": 4.8, "brand": "BrewMaster" },
  { "id": 18, "name": "Bookshelf 5-Tier", "category": "Furniture", "price": 189.00, "stock": 25, "rating": 4.3, "brand": "WoodCraft" },
  { "id": 19, "name": "Bamboo Cutting Board", "category": "Kitchen", "price": 22.99, "stock": 250, "rating": 4.6, "brand": "IronChef" },
  { "id": 20, "name": "USB-C Hub 7-in-1", "category": "Electronics", "price": 54.99, "stock": 100, "rating": 4.5, "brand": "PowerUp" },
  { "id": 21, "name": "Foam Roller", "category": "Sports", "price": 19.99, "stock": 180, "rating": 4.4, "brand": "ZenFlex" },
  { "id": 22, "name": "Scented Soy Candles Set", "category": "Home", "price": 32.00, "stock": 160, "rating": 4.7, "brand": "AromaBliss" },
  { "id": 23, "name": "Electric Toothbrush", "category": "Personal Care", "price": 69.99, "stock": 90, "rating": 4.6, "brand": "BrightSmile" },
  { "id": 24, "name": "Noise Machine", "category": "Home", "price": 49.99, "stock": 70, "rating": 4.5, "brand": "CleanAir" },
  { "id": 25, "name": "Laptop Stand Adjustable", "category": "Electronics", "price": 44.99, "stock": 120, "rating": 4.6, "brand": "RiseUp" },
  { "id": 26, "name": "Insulated Lunch Bag", "category": "Sports", "price": 27.99, "stock": 210, "rating": 4.4, "brand": "HydroFlow" },
  { "id": 27, "name": "Wireless Mouse", "category": "Electronics", "price": 39.99, "stock": 145, "rating": 4.5, "brand": "KeyMaster" },
  { "id": 28, "name": "Aromatherapy Diffuser", "category": "Personal Care", "price": 36.99, "stock": 115, "rating": 4.6, "brand": "AromaBliss" },
  { "id": 29, "name": "Shower Head with Filter", "category": "Personal Care", "price": 54.99, "stock": 80, "rating": 4.7, "brand": "BrightSmile" },
  { "id": 30, "name": "Monitor Light Bar", "category": "Electronics", "price": 64.99, "stock": 65, "rating": 4.5, "brand": "BrightSpace" }
] */

public class MapOperate {

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

    static final List<Map<String, Object>> PRODUCTS = Arrays.asList(
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
            DataSet productSet = DataSet.of("test", "products");

            // Single record with a list of products
            session.upsert(productSet.id(66743)).bin("products").setTo(PRODUCTS).execute();

            /*
             * to view the record in aql, run the following commands:
             * aql> set output json
             * OUTPUT = JSON
             * aql> select * from test.products where pk=66743
             */

            // fetch the 7th product in the list (index 6)
            session.query(productSet.id(66743))
                    .bin("products").onListIndex(6).getValues()
                    .execute()
                    .getFirst().ifPresent(result -> {
                        System.out.println("7th product:\n" + result.recordOrNull().getMap("products") + "\n");
                    });

            // retrieve the products with the category of "Sports" using a filter <-----
            // TODO
            session.query(productSet.id(66743))
                    .bin("products").get()
                    .execute()
                    .getFirst().ifPresent(result -> {
                        System.out.println("Products in the Sports category:\n"
                                + result.recordOrNull().getList("products") + "\n");
                    });

        } finally {
            cluster.close();
        }
    }
}
