CREATE DATABASE IF NOT EXISTS local;
USE local;

CREATE TABLE IF NOT EXISTS local.hired_employees (
    id INT PRIMARY KEY,
    name VARCHAR(255),  -- Adjust the size based on your needs
    hire_datetime DATETIME,
    department_id INT,
    job_id INT
);

CREATE TABLE IF NOT EXISTS local.departments (
    id INT PRIMARY KEY,
    department VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS local.jobs (
    id INT PRIMARY KEY,
    job VARCHAR(255)
);