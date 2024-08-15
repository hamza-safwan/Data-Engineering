from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Complex Data Engineering Task") \
    .config("spark.master", "local") \
    .getOrCreate()

# Set log level to ERROR to reduce log output
spark.sparkContext.setLogLevel("ERROR")

# Function to read CSV file into DataFrame
def read_csv(spark, path):
    schema = "id INT, name STRING, age INT, salary DOUBLE, department STRING"
    return spark.read.csv(path, header=True, schema=schema)

# Function to process data
def process_data(df):
    # Calculate average salary by department
    avg_salary_df = df.groupBy("department").agg(avg("salary").alias("avg_salary"))

    # Calculate the maximum age in the company
    max_age_df = df.agg(max("age").alias("max_age"))

    # Join the average salary data with the original DataFrame
    joined_df = df.join(avg_salary_df, "department")

    # Add a new column indicating if the employee's salary is above the department average
    final_df = joined_df.withColumn("above_avg_salary", col("salary") > col("avg_salary"))

    # Select relevant columns
    return final_df.select("id", "name", "age", "salary", "department", "avg_salary", "above_avg_salary")

# Function to write DataFrame to CSV file
def write_csv(df, path):
    df.write.option("header", "true").csv(path)

# Main function
if __name__ == "__main__":
    # Read CSV file into DataFrame
    input_path = "path/to/your/input.csv"
    df = read_csv(spark, input_path)

    # Process the data
    processed_df = process_data(df)

    # Write the results to another CSV file
    output_path = "path/to/your/output.csv"
    write_csv(processed_df, output_path)

    # Stop the SparkSession
    spark.stop()
