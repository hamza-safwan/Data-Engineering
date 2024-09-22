#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <iomanip>
#include <algorithm>
#include <ctime>

// Employee structure
struct Employee {
    int id;
    std::string name;
    int age;
    double salary;
    std::string department;
    std::string date_joined;
};

// Function to split a string by a delimiter
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to read a CSV file and parse the data
std::vector<Employee> read_csv(const std::string& file_path) {
    std::vector<Employee> employees;
    std::ifstream file(file_path);

    if (!file.is_open()) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return employees;
    }

    std::string line;
    std::getline(file, line); // Skip the header

    while (std::getline(file, line)) {
        std::vector<std::string> columns = split(line, ',');
        if (columns.size() != 6) continue;

        Employee emp;
        emp.id = std::stoi(columns[0]);
        emp.name = columns[1];
        emp.age = std::stoi(columns[2]);
        emp.salary = std::stod(columns[3]);
        emp.department = columns[4];
        emp.date_joined = columns[5];

        employees.push_back(emp);
    }

    file.close();
    return employees;
}

// Function to merge two datasets
std::vector<Employee> merge_datasets(const std::vector<Employee>& dataset1, const std::vector<Employee>& dataset2) {
    std::vector<Employee> combined = dataset1;
    combined.insert(combined.end(), dataset2.begin(), dataset2.end());
    return combined;
}

// Function to clean data (e.g., handle missing values, normalize formats)
std::vector<Employee> clean_data(const std::vector<Employee>& employees) {
    std::vector<Employee> cleaned_data;
    for (const auto& emp : employees) {
        if (emp.name.empty() || emp.department.empty() || emp.date_joined.empty()) {
            continue; // Skip incomplete records
        }
        cleaned_data.push_back(emp);
    }
    return cleaned_data;
}

// Function to filter employees based on multiple criteria
std::vector<Employee> filter_employees(const std::vector<Employee>& employees, int age_threshold, double salary_threshold) {
    std::vector<Employee> filtered_employees;
    for (const auto& emp : employees) {
        if (emp.age > age_threshold && emp.salary > salary_threshold) {
            filtered_employees.push_back(emp);
        }
    }
    return filtered_employees;
}

// Function to calculate average and standard deviation salary by department
std::unordered_map<std::string, std::pair<double, double>> calculate_statistics(const std::vector<Employee>& employees) {
    std::unordered_map<std::string, double> total_salary;
    std::unordered_map<std::string, int> count;
    std::unordered_map<std::string, std::vector<double>> salaries;

    for (const auto& emp : employees) {
        total_salary[emp.department] += emp.salary;
        count[emp.department]++;
        salaries[emp.department].push_back(emp.salary);
    }

    std::unordered_map<std::string, std::pair<double, double>> statistics;
    for (const auto& dept : total_salary) {
        double mean = dept.second / count[dept.first];
        double sum_of_squares = 0.0;
        for (double salary : salaries[dept.first]) {
            sum_of_squares += (salary - mean) * (salary - mean);
        }
        double stddev = sqrt(sum_of_squares / count[dept.first]);
        statistics[dept.first] = { mean, stddev };
    }

    return statistics;
}

// Function to write the filtered data to a CSV file
void write_filtered_csv(const std::string& file_path, const std::vector<Employee>& employees) {
    std::ofstream file(file_path);

    if (!file.is_open()) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return;
    }

    file << "id,name,age,salary,department,date_joined\n";
    for (const auto& emp : employees) {
        file << emp.id << "," << emp.name << "," << emp.age << "," << emp.salary << "," << emp.department << "," << emp.date_joined << "\n";
    }

    file.close();
}

// Function to write the statistics to a CSV file
void write_aggregated_csv(const std::string& file_path, const std::unordered_map<std::string, std::pair<double, double>>& statistics) {
    std::ofstream file(file_path);

    if (!file.is_open()) {
        std::cerr << "Error opening file: " << file_path << std::endl;
        return;
    }

    file << "department,avg_salary,stddev_salary\n";
    for (const auto& dept : statistics) {
        file << dept.first << "," << std::fixed << std::setprecision(2) << dept.second.first << "," << dept.second.second << "\n";
    }

    file.close();
}

// Function to log the process
void log_process(const std::string& file_path, const std::string& message) {
    std::ofstream log_file(file_path, std::ios_base::app);
    if (log_file.is_open()) {
        time_t now = time(0);
        std::string dt = ctime(&now);
        dt.pop_back(); // Remove newline character
        log_file << "[" << dt << "] " << message << "\n";
        log_file.close();
    }
}

int main() {
    // File paths
    const std::string input_file1 = "data/input1.csv";
    const std::string input_file2 = "data/input2.csv";
    const std::string filtered_output_file = "data/output/filtered.csv";
    const std::string aggregated_output_file = "data/output/aggregated.csv";
    const std::string log_file = "data/logs/process.log";

    // Log start of process
    log_process(log_file, "Process started.");

    // Read data from input CSV files
    std::vector<Employee> dataset1 = read_csv(input_file1);
    std::vector<Employee> dataset2 = read_csv(input_file2);
    log_process(log_file, "Data read from input files.");

    // Merge datasets
    std::vector<Employee> employees = merge_datasets(dataset1, dataset2);
    log_process(log_file, "Datasets merged.");

    // Clean data
    employees = clean_data(employees);
    log_process(log_file, "Data cleaned.");

    // Filter employees based on criteria: age > 30 and salary > 50000
    std::vector<Employee> filtered_employees = filter_employees(employees, 30, 50000);
    log_process(log_file, "Employees filtered based on criteria.");

    // Calculate statistics: average and standard deviation of salary by department
    auto statistics = calculate_statistics(filtered_employees);
    log_process(log_file, "Statistics calculated.");

    // Write filtered data to CSV
    write_filtered_csv(filtered_output_file, filtered_employees);
    log_process(log_file, "Filtered data written to CSV.");

    // Write aggregated data to CSV
    write_aggreg
