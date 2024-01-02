CREATE TABLE local.hired_employees (
    id INT PRIMARY KEY,
    name VARCHAR(255),  -- Adjust the size based on your needs
    hire_datetime DATETIME,
    department_id INT,
    job_id INT
);

CREATE TABLE local.departments (
    id INT PRIMARY KEY,
    department VARCHAR(255)
);

CREATE TABLE local.jobs (
    id INT PRIMARY KEY,
    job VARCHAR(255)
);


SELECT * FROM local.jobs;

SELECT * FROM local.departments;

SELECT * FROM local.hired_employees;

