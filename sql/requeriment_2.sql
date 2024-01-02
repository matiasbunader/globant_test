SELECT
	d.id as ID,
    d.department as Department,
    COUNT(e.id) as Hired 
FROM
	local.hired_employees e
JOIN
	local.departments d
ON
	(e.department_id = d.id)
GROUP BY
	ID, Department
HAVING
	COUNT(e.id) >= (SELECT COUNT(id)/COUNT(distinct department_id) FROM local.hired_employees WHERE YEAR(hire_datetime) = 2021)    
ORDER BY
	Hired DESC;