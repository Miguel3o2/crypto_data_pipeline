SELECT
    id,
    name,
    department,
    salary,
    active,
    salary * 0.10              AS bonus,
    salary + (salary * 0.10)   AS salary_after_raise
FROM public.employees
WHERE active = TRUE