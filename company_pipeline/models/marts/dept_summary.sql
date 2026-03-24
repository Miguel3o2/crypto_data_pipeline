SELECT
    department,
    COUNT(*)        AS headcount,
    AVG(salary)     AS avg_salary,
    SUM(salary)     AS total_salary,
    MAX(salary)     AS top_salary
FROM {{ ref('stg_employees') }}
GROUP BY department