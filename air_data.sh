#!/bin/bash

# Variables
RAW_DATA_DIR="raw_data"
PROCESSED_DATA_DIR="processed_data"
OUTPUT_DIR="output"
TEMP_DIR="temp"
DATA_FILE_URL="https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"
RAW_DATA_FILE="$RAW_DATA_DIR/airtravel.csv"
PROCESSED_DATA_FILE="$PROCESSED_DATA_DIR/processed_airtravel.csv"
SUMMARY_REPORT="$OUTPUT_DIR/summary_report.txt"

# Create directories
echo "Creating directories..."
mkdir -p $RAW_DATA_DIR $PROCESSED_DATA_DIR $OUTPUT_DIR $TEMP_DIR
echo "Directories created."

# Download raw data
echo "Downloading raw data..."
curl -o $RAW_DATA_FILE $DATA_FILE_URL
if [ $? -ne 0 ]; then
  echo "Failed to download data. Exiting."
  exit 1
fi
echo "Data downloaded."

# Process data: remove header and convert commas to tabs
echo "Processing data..."
tail -n +2 $RAW_DATA_FILE | sed 's/,/\t/g' > $PROCESSED_DATA_FILE
if [ $? -ne 0 ]; then
  echo "Failed to process data. Exiting."
  exit 1
fi
echo "Data processed."

# Generate summary report
echo "Generating summary report..."
{
  echo "Air Travel Summary Report"
  echo "========================="
  echo ""
  echo "Total Records: $(wc -l < $PROCESSED_DATA_FILE)"
  echo ""
  echo "Monthly Data:"
  awk -F '\t' '{for (i=2; i<=NF; i++) sum[i]+=$i; n=NF} END {for (i=2; i<=n; i++) printf "%s\t%s\n", NR==1?i:NR, sum[i]}' $PROCESSED_DATA_FILE
} > $SUMMARY_REPORT

if [ $? -ne 0 ]; then
  echo "Failed to generate summary report. Exiting."
  exit 1
fi
echo "Summary report generated."

# Clean up temporary files
echo "Cleaning up..."
rm -r $TEMP_DIR
echo "Cleanup complete."

echo "Data engineering task completed successfully."
#!/bin/bash

# Directory and file names
DIR_NAME="JavaHelloWorld"
JAVA_FILE="HelloWorld.java"
CLASS_FILE="HelloWorld.class"

# Create a directory for the Java program
if [ ! -d "$DIR_NAME" ]; then
  mkdir $DIR_NAME
  echo "Directory $DIR_NAME created."
else
  echo "Directory $DIR_NAME already exists."
fi

# Change to the created directory
cd $DIR_NAME

# Create the Java source file
echo "Creating Java source file..."
cat <<EOL > $JAVA_FILE
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello, World!");
    }
}
EOL

echo "Java source file $JAVA_FILE created."

# Compile the Java program
echo "Compiling Java program..."
javac $JAVA_FILE

if [ $? -eq 0 ]; then
  echo "Compilation successful."
else
  echo "Compilation failed."
  exit 1
fi

# Run the compiled Java program
echo "Running Java program..."
java HelloWorld

# Clean up: remove the Java source file and compiled class file
echo "Cleaning up..."
rm $JAVA_FILE $CLASS_FILE

echo "Done."
