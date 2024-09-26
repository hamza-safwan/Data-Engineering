import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataProcessingSnippet {

    public static void main(String[] args) throws Exception {
        String inputFilePath = "input_sales_data.csv";
        String outputCustomerRevenuePath = "output_customer_revenue.csv";
        String outputProductSalesPath = "output_product_sales.csv";

        ExecutorService executor = Executors.newFixedThreadPool(3);

        // Process customer revenue and product sales in parallel
        Future<Map<String, Double>> customerRevenueFuture = executor.submit(() -> processCustomerRevenue(inputFilePath));
        Future<Map<String, Integer>> productSalesFuture = executor.submit(() -> processProductSales(inputFilePath));

        // Aggregate results
        Map<String, Double> customerRevenueMap = customerRevenueFuture.get();
        Map<String, Integer> productSalesMap = productSalesFuture.get();

        // Write the results to CSV files
        writeCustomerRevenue(outputCustomerRevenuePath, customerRevenueMap);
        writeProductSales(outputProductSalesPath, productSalesMap);

        executor.shutdown();
    }

    private static Map<String, Double> processCustomerRevenue(String filePath) throws IOException {
        Map<String, Double> customerRevenueMap = new HashMap<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String customerId = nextLine[1];
                int quantity = Integer.parseInt(nextLine[4]);
                double price = Double.parseDouble(nextLine[5]);
                double revenue = quantity * price;

                customerRevenueMap.merge(customerId, revenue, Double::sum);
            }
        }
        return customerRevenueMap;
    }

    private static Map<String, Integer> processProductSales(String filePath) throws IOException {
        Map<String, Integer> productSalesMap = new HashMap<>();
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String productId = nextLine[3];
                int quantity = Integer.parseInt(nextLine[4]);

                productSalesMap.merge(productId, quantity, Integer::sum);
            }
        }
        return productSalesMap;
    }

    private static void writeCustomerRevenue(String outputFilePath, Map<String, Double> customerRevenueMap) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath))) {
            writer.writeNext(new String[]{"CustomerID", "TotalRevenue"});
            for (Map.Entry<String, Double> entry : customerRevenueMap.entrySet()) {
                writer.writeNext(new String[]{entry.getKey(), String.valueOf(entry.getValue())});
            }
        }
    }

    private static void writeProductSales(String outputFilePath, Map<String, Integer> productSalesMap) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(outputFilePath))) {
            writer.writeNext(new String[]{"ProductID", "TotalSales"});
            for (Map.Entry<String, Integer> entry : productSalesMap.entrySet()) {
                writer.writeNext(new String[]{entry.getKey(), String.valueOf(entry.getValue())});
            }
        }
    }
}
