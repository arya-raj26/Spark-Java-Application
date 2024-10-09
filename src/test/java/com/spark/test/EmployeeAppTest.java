package com.spark.test;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import com.spark.main.EmployeeApp;
import com.spark.pojo.Employee;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class EmployeeAppTest extends JavaDatasetSuiteBase {

    @Test
    public void testValidateEmployeeDataset() {
        // Create sample data (list of Employee objects)
        List<Employee> employees = Arrays.asList(
                new Employee(1, "John", "HR"),
                new Employee(2, "Jane", "IT"),
                new Employee(3, "Joe", ""),
                new Employee(4, "Jim", null)
        );

        // Convert list to Dataset using Encoders.bean for the Employee POJO
        Dataset<Employee> employeeDs = sqlContext().createDataset(employees, Encoders.bean(Employee.class));

        // Create an instance of EmployeeApp to call the public method
        EmployeeApp app = new EmployeeApp();

        // Call the public method to validate the dataset
        Dataset<Employee> validatedDs = invokeValidateEmployeeDataset(employeeDs);

        // Collect the results as a list
        List<Employee> result = validatedDs.collectAsList();

        // Assertions to check that the empty and null departments are removed
        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(e -> e.getName().equals("John"))); // Valid
        assertTrue(result.stream().anyMatch(e -> e.getName().equals("Jane"))); // Valid
        assertFalse(result.stream().anyMatch(e -> e.getName().equals("Joe"))); // Invalid (empty department)
        assertFalse(result.stream().anyMatch(e -> e.getName().equals("Jim"))); // Invalid (null department)
    }

    @Test
    public void testFilteredByDepartment() {
        // Create sample data (list of Employee objects)
        List<Employee> employees = Arrays.asList(
                new Employee(1, "John", "HR"),
                new Employee(2, "Jane", "IT"),
                new Employee(3, "Joe", "IT"),
                new Employee(0, "InvalidID", "IT"), // Invalid ID
                new Employee(3, null, "Sales"),     // Null name
                new Employee(4, "", "Sales") 
        );

        // Convert list to Dataset using Encoders.bean for the Employee POJO
        Dataset<Employee> employeeDs = sqlContext().createDataset(employees, Encoders.bean(Employee.class));

        // Create an instance of EmployeeApp to call the public method
        EmployeeApp app = new EmployeeApp();

        // Call the public method to validate the dataset
        Dataset<Employee> validatedDs = app.processEmployeeDataset(employeeDs);
        
        // Call the new validation method
        Dataset<Employee> validatedNameIdDs = app.validateEmployeeNameAndId(employeeDs);
        
        // Collect results
        List<Employee> resultNameId = validatedNameIdDs.collectAsList();
        
        // Filter the dataset by Department = 'IT'
        Dataset<Employee> filteredDs = validatedDs.filter(validatedDs.col("Department").equalTo("IT"));

        // Collect the results as a list
        List<Employee> result = filteredDs.collectAsList();

        // Assertions to check the results
        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(e -> e.getName().equals("Jane"))); // IT department
        assertTrue(result.stream().anyMatch(e -> e.getName().equals("Joe")));  // IT department
        assertFalse(result.stream().anyMatch(e -> e.getName().equals("John"))); // Not IT
        
        // Verify that only employees with valid name and ID are retained
        assertEquals(2, resultNameId.size());
        assertTrue(resultNameId.stream().anyMatch(emp -> emp.getName().equals("John")));
        assertTrue(resultNameId.stream().anyMatch(emp -> emp.getName().equals("Jane")));
    }
    // Reflection to call the private method
    private Dataset<Employee> invokeValidateEmployeeDataset(Dataset<Employee> employeeDs) {
        try {
            // Access the private method
            java.lang.reflect.Method method = EmployeeApp.class.getDeclaredMethod("validateEmployeeDataset", Dataset.class);
            method.setAccessible(true);
            return (Dataset<Employee>) method.invoke(null, employeeDs);
        } catch (Exception e) {
            throw new RuntimeException("Failed to invoke validation method", e);
        }
    }

}
