SELECT 
	department as Department,
    job as Job,
	SUM(CASE WHEN QUARTER(e.hire_datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN QUARTER(e.hire_datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN QUARTER(e.hire_datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN QUARTER(e.hire_datetime) = 4 THEN 1 ELSE 0 END) AS Q4
FROM 
	local.hired_employees e
JOIN
	local.departments d
ON
	(e.department_id = d.id)
JOIN
	local.jobs j
ON
	(e.job_id = j.id)
WHERE 
	YEAR(e.hire_datetime) = 2021
GROUP BY    
	Department, Job
ORDER BY 
	Department ASC, Job ASC;