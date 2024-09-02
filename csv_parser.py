import csv
import concurrent.futures
from collections import defaultdict

def process_chunk(file_path, offset, batch_size, is_customer_revenue):
    customer_revenue_map = defaultdict(float)
    product_sales_map = defaultdict(int)
    
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        # Skip to the offset
        for _ in range(offset):
            next(reader, None)
        
        count = 0
        for row in reader:
            if count >= batch_size:
                break
            
            customer_id = row[1]
            product_id = row[3]
            quantity = int(row[4])
            price = float(row[5])
            
            if quantity > 0:
                if is_customer_revenue:
                    revenue = quantity * price
                    customer_revenue_map[customer_id] += revenue
                else:
                    product_sales_map[product_id] += quantity
            
            count += 1
    
    return customer_revenue_map if is_customer_revenue else product_sales_map

def has_more_data(file_path, offset):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for _ in range(offset):
            next(reader, None)
        return next(reader, None) is not None

def write_customer_revenue(output_file_path, customer_revenue_map):
    with open(output_file_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["CustomerID", "TotalRevenue"])
        for customer_id, total_revenue in customer_revenue_map.items():
            writer.writerow([customer_id, total_revenue])

def write_product_sales(output_file_path, product_sales_map):
    with open(output_file_path, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["ProductID", "TotalSales"])
        for product_id, total_sales in product_sales_map.items():
            writer.writerow([product_id, total_sales])

def main():
    input_file_path = "input_sales_data.csv"
    output_customer_revenue_path = "output_customer_revenue.csv"
    output_product_sales_path = "output_product_sales.csv"

    batch_size = 10000
    offset = 0

    customer_revenue_futures = []
    product_sales_futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        while True:
            customer_revenue_future = executor.submit(process_chunk, input_file_path, offset, batch_size, True)
            product_sales_future = executor.submit(process_chunk, input_file_path, offset, batch_size, False)
            
            customer_revenue_futures.append(customer_revenue_future)
            product_sales_futures.append(product_sales_future)
            
            offset += batch_size
            if not has_more_data(input_file_path, offset):
                break

        customer_revenue_map = defaultdict(float)
        product_sales_map = defaultdict(int)

        for future in concurrent.futures.as_completed(customer_revenue_futures):
            result = future.result()
            for customer_id, revenue in result.items():
                customer_revenue_map[customer_id] += revenue

        for future in concurrent.futures.as_completed(product_sales_futures):
            result = future.result()
            for product_id, sales in result.items():
                product_sales_map[product_id] += sales

    write_customer_revenue(output_customer_revenue_path, customer_revenue_map)
    write_product_sales(output_product_sales_path, product_sales_map)

    print("Data processing completed successfully!")

if __name__ == "__main__":
    main()
