SELECT
    id,
    UPPER(name)         AS name,
    UPPER(department)   AS department,
    salary,
    active
FROM public.employees