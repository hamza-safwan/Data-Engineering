import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class ComplexDataEngineeringTask {
    private static final Logger logger = LoggerFactory.getLogger(ComplexDataEngineeringTask.class);

    public static void main(String[] args) {
        String inputFilePath = "input_sales_data.csv";
        String outputFilePath = "output_customer_revenue.csv";
        String outputProductSalesFilePath = "output_product_sales.csv";

        // Create a thread pool
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try {
            // Step 1: Read the CSV file in chunks
            List<Future<Map<String, Double>>> customerRevenueFutures = new ArrayList<>();
            List<Future<Map<String, Integer>>> productSalesFutures = new ArrayList<>();

            int batchSize = 10000;
            int offset = 0;

            while (true) {
                Future<Map<String, Double>> customerRevenueFuture = executor.submit(new DataProcessor(inputFilePath, offset, batchSize, true));
                Future<Map<String, Integer>> productSalesFuture = executor.submit(new DataProcessor(inputFilePath, offset, batchSize, false));
                customerRevenueFutures.add(customerRevenueFuture);
                productSalesFutures.add(productSalesFuture);

                offset += batchSize;
                if (!DataProcessor.hasMoreData(inputFilePath, offset)) {
                    break;
                }
            }

            // Step 2: Aggregate results
            Map<String, Double> customerRevenueMap = new HashMap<>();
            Map<String, Integer> productSalesMap = new HashMap<>();

            for (Future<Map<String, Double>> future : customerRevenueFutures) {
                Map<String, Double> result = future.get();
                for (Map.Entry<String, Double> entry : result.entrySet()) {
                    customerRevenueMap.merge(entry.getKey(), entry.getValue(), Double::sum);
                }
            }

            for (Future<Map<String, Integer>> future : productSalesFutures) {
                Map<String, Integer> result = future.get();
                for (Map.Entry<String, Integer> entry : result.entrySet()) {
                    productSalesMap.merge(entry.getKey(), entry.getValue(), Integer::sum);
                }
            }

            // Step 3: Write the results to new CSV files
            writeCustomerRevenue(outputFilePath, customerRevenueMap);
            writeProductSales(outputProductSalesFilePath, productSalesMap);

            logger.info("Data processing completed successfully!");

        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error("Error during data processing", e);
        } finally {
            executor.shutdown();
        }
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

class DataProcessor implements Callable<Map<String, ?>> {
    private final String filePath;
    private final int offset;
    private final int batchSize;
    private final boolean isCustomerRevenue;

    DataProcessor(String filePath, int offset, int batchSize, boolean isCustomerRevenue) {
        this.filePath = filePath;
        this.offset = offset;
        this.batchSize = batchSize;
        this.isCustomerRevenue = isCustomerRevenue;
    }

    @Override
    public Map<String, ?> call() throws Exception {
        Map<String, Double> customerRevenueMap = new HashMap<>();
        Map<String, Integer> productSalesMap = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            reader.skip(offset);
            String[] nextLine;
            int count = 0;

            while ((nextLine = reader.readNext()) != null && count < batchSize) {
                String customerId = nextLine[1];
                String productId = nextLine[3];
                int quantity = Integer.parseInt(nextLine[4]);
                double price = Double.parseDouble(nextLine[5]);

                if (quantity > 0) {
                    if (isCustomerRevenue) {
                        double revenue = quantity * price;
                        customerRevenueMap.put(customerId, customerRevenueMap.getOrDefault(customerId, 0.0) + revenue);
                    } else {
                        productSalesMap.put(productId, productSalesMap.getOrDefault(productId, 0) + quantity);
                    }
                }
                count++;
            }
        }

        return isCustomerRevenue ? customerRevenueMap : productSalesMap;
    }

    static boolean hasMoreData(String filePath, int offset) throws IOException {
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            reader.skip(offset);
            return reader.readNext() != null;
        }
    }
}
