package com.spark.main;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.spark.pojo.Employee;
import java.util.Arrays;
import java.util.List;

public class EmployeeApp {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Employee Application")
                .master("local[*]") // Runs Spark locally with all available cores
                .getOrCreate();

        // Create sample data (list of Employee objects)
        List<Employee> employees = Arrays.asList(
                new Employee(1, "John", "HR"),
                new Employee(2, "Jane", "IT"),
                new Employee(3, "Joe", "")
        );


        // Convert list to Dataset using Encoders.bean for the Employee POJO
        Dataset<Employee> employeeDs = spark.createDataset(employees, Encoders.bean(Employee.class));

        // Use the validation method before further processing
        Dataset<Employee> validatedDs = validateEmployeeDataset(employeeDs);

        // Perform a transformation (example: filter by department)
        Dataset<Employee> filteredDs = validatedDs.filter(employeeDs.col("Department").equalTo("IT"));
        		

        // Show the filtered results
        filteredDs.show();

        // Stop the Spark session
        spark.stop();
    }
    
 // Validation method to check if department matches any allowed patterns
    public static Dataset<Employee> validateDepartmentWithPatterns(Dataset<Employee> employeeDs) {
    	
        // List of allowed department patterns
        final List<String> allowedDepartmentPatterns = Arrays.asList(
            "HR",          // Exact match for HR
            "IT",          // Exact match for IT
            "Sales.*",      // Pattern match for departments starting with "Sales"
            "Marketing Analytics"
        );
    	
        // Combine all patterns into one regex using "|", which acts like "OR"
        String regexPattern = String.join("|", allowedDepartmentPatterns);

        // Apply the regex filter using rlike to match the department against the allowed patterns
        return employeeDs.filter(employeeDs.col("Department").rlike(regexPattern));
    }

    
 // Private validation method to check if name is not null or empty, and ID is greater than 0
    public static Dataset<Employee> validateEmployeeNameAndId(Dataset<Employee> employeeDs) {
        return employeeDs.filter(
            employeeDs.col("Name").isNotNull()
            .and(employeeDs.col("Name").notEqual(""))
            .and(employeeDs.col("ID").gt(0)) // ID must be greater than 0
        );
    }

    // Private validation method to check if department is not null or empty
    private static Dataset<Employee> validateEmployeeDataset(Dataset<Employee> employeeDs) {
        return employeeDs.filter(
            employeeDs.col("Department").isNotNull()
            .and(employeeDs.col("Department").notEqual(""))
        );
    }
    
    public Dataset<Employee> processEmployeeDataset(Dataset<Employee> employeeDs) {
        return validateEmployeeDataset(employeeDs);
    }
}
