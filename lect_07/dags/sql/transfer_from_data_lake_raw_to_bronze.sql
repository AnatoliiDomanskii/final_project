DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.bronze.sales` (
    client,
    purchase_date,
    product,
    price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    client,
    purchase_date,
    product,
    price,

    GENERATE_UUID() AS _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
    CAST('{{ dag_run.start_date }}'AS TIMESTAMP) AS _job_start_dt
FROM sales_csv
;