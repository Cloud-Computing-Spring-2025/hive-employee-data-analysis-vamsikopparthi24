# Assignment #2 Hive: Employee and Department Data Analysis

## Problem Statement
This assignment involves analyzing employee and department data using Hive queries. The datasets provided are employees.csv and departments.csv. The goal is to perform various data analysis tasks using HiveQL queries.

## Dataset Details

### employees.csv
This dataset contains information about employees, including their department, job role, salary, and project assignment.

- *emp_id*: Unique employee ID
- *name*: Employee's full name
- *age*: Employee's age
- *job_role*: Designation of the employee
- *salary*: Annual salary of the employee
- *project*: Assigned project (One of: Alpha, Beta, Gamma, Delta, Omega)
- *join_date*: Date when the employee joined
- *department*: Department to which the employee belongs (Used for partitioning)

### departments.csv
This dataset contains information about different departments in the company.

- *dept_id*: Unique department ID
- *department_name*: Name of the department
- *location*: Location of the department

## Tasks

1. **Load data from employees.csv into a temporary Hive table.**
2. *Transform and move data to the actual partitioned table in Hive.*
   - Ensure that data is loaded correctly into a partitioned table using the ALTER TABLE statement rather than specifying partitions at table creation.
3. *Perform the following queries:*
   - Retrieve all employees who joined after 2015.
   - Find the average salary of employees in each department.
   - Identify employees working on the 'Alpha' project.
   - Count the number of employees in each job role.
   - Retrieve employees whose salary is above the average salary of their department.
   - Find the department with the highest number of employees.
   - Check for employees with null values in any column and exclude them from analysis.
   - Join the employees and departments tables to display employee details along with department locations.
   - Rank employees within each department based on salary.
   - Find the top 3 highest-paid employees in each department.

## Steps to Execute the Queries

### 1. Create Employee table

```sql
CREATE TABLE employees ( emp_id INT, name STRING, age INT, job_role STRING, salary FLOAT, project STRING, join_date STRING ) PARTITIONED BY (department STRING) STORED AS ORC
```



### 2. Create Department table

```sql
CREATE TABLE departments ( dept_id INT, department_name STRING, location STRING ) STORED AS ORC
```

### 3. Partition employee table

```sql
SET hive.exec.dynamic.partition.mode=nonstrict
INSERT OVERWRITE TABLE employees PARTITION (department) SELECT emp_id, name, age, job_role, salary, project, join_date, department FROM hue__tmp_employees
```

### 4. Overwrite department table

```sql
INSERT OVERWRITE TABLE departments SELECT dept_id, department_name, location FROM hue__tmp_departments
```

### 5. Overwrite department table

```sql
INSERT OVERWRITE TABLE departments SELECT dept_id, department_name, location FROM hue__tmp_departments
```

### 6. Retrieve all employees who joined after 2015.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/emp_after_2015'  
SELECT * FROM employees WHERE YEAR(join_date) > 2015
```

output - [output](./output/emp_after_2015.txt)

### 7. Find the average salary of employees in each department.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/average_sal'
SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department
```
output - [output](./output/average_sal.txt)

### 8. Identify employees working on the 'Alpha' project.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/project_alpha'  
SELECT * FROM employees WHERE project = 'Alpha'
```

output - [output](./output/project_alpha.txt)


### 9. Count the number of employees in each job role.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/cnt_emp_in_job_role'  
SELECT job_role, COUNT(*) AS employee_count FROM employees GROUP BY job_role
```
output - [output](./output/cnt_emp_in_job_role.txt)

### 10. Retrieve employees whose salary is above the average salary of their department.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/emp_sal_gt_avg_sal'  
SELECT e.* FROM employees e JOIN ( SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department ) d ON e.department = d.department WHERE e.salary > d.avg_salary
```
output - [output](./output/emp_sal_gt_avg_sal.txt)

### 11. Find the department with the highest number of employees.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/dpt_high_emp'  
SELECT department, COUNT(*) AS employee_count FROM employees GROUP BY department ORDER BY employee_count DESC LIMIT 1
```
output - [output](./output/dpt_high_emp.txt)

### 12. Check for employees with null values in any column and exclude them from analysis.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/exclude_null'  
SELECT * FROM employees WHERE emp_id IS NOT NULL AND name IS NOT NULL AND age IS NOT NULL AND job_role IS NOT NULL AND salary IS NOT NULL AND project IS NOT NULL AND join_date IS NOT NULL AND department IS NOT NULL
```
output - [output](./output/exclude_null.txt)

### 13. Join the employees and departments tables to display employee details along with department locations.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/join_emp_dept'  
SELECT e.*, d.location FROM employees e JOIN departments d ON e.department = d.department_name
```

output - [output](./output/join_emp_dept.txt)

### 14. Rank employees within each department based on salary.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/rank_emp'  
SELECT emp_id, name, salary, department, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank FROM employees
```

output - [output](./output/rank_emp.txt)

### 15. Find the top 3 highest-paid employees in each department.

```sql
INSERT OVERWRITE DIRECTORY '/user/hue/output/3_high_paid'  
SELECT * FROM ( SELECT emp_id, name, salary, department, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank FROM employees ) ranked_employees WHERE salary_rank <= 3
```

output - [output](./output/3_high_paid.txt)

## Steps to retreive the output 

Run these commands in bash

```bash
hdfs dfs -getmerge /user/hue/output/emp_after_2015 emp_after_2015.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/3_high_paid 3_high_paid.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/average_s
al average_sal.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/cnt_emp_in_job_role cnt_emp_in_job_role.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/dpt_high_emp dpt_high_emp.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/emp_sal_gt_avg_sal emp_sal_gt_avg_sal.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/exclude_null exclude_null.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/join_emp_dept join_emp_dept.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/project_alpha project_alpha.txt
```

```bash
hdfs dfs -getmerge /user/hue/output/rank_emp 
rank_emp.txt
```

Exit and run these commands

```bash
docker cp resourcemanager:/emp_after_2015.txt output/
```

```bash
docker cp resourcemanager:/3_high_paid.txt output/
```

```bash
docker cp resourcemanager:/average_sal.txt output/
```

```bash
docker cp resourcemanager:/cnt_emp_in_job_role.txt output/
```
```bash
docker cp resourcemanager:/dpt_high_emp.txt output
```

```bash
docker cp resourcemanager:/emp_sal_gt_avg_sal.txt output/
```

```bash
docker cp resourcemanager:/exclude_null.txt output/
```

```bash
docker cp resourcemanager:/join_emp_dept.txt output/
```

```bash
docker cp resourcemanager:/project_alpha.txt output/
```

```bash
docker cp resourcemanager:/rank_emp.txt output/
```
