from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

default_args = {
    'start_date': days_ago(1),
}

with DAG('P2', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    with TaskGroup('TaskGroup1') as tg1:
        task1 = BigQueryInsertJobOperator(
            task_id='query_top_five_products',
            configuration={
                'query': {
                    'query': """
                        WITH SalesSummary AS (
                            SELECT 
                                p.product_id,
                                p.product_name,
                                SUM(s.quantity_sold) AS total_quantity_sold
                            FROM 
                                Sales s
                            JOIN 
                                Products p ON s.product_id = p.product_id
                            GROUP BY 
                                p.product_id, p.product_name
                        ),
                        TopFiveProducts AS (
                            SELECT 
                                product_id,
                                product_name,
                                total_quantity_sold,
                                ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS row_num
                            FROM 
                                SalesSummary
                        )
                        SELECT 
                            product_id,
                            product_name,
                            total_quantity_sold
                        FROM 
                            TopFiveProducts
                        WHERE 
                            row_num <= 5;
                    """,
                    'useLegacySql': False,
                }
            }
        )

        quality_check1 = BigQueryInsertJobOperator(
            task_id='quality_check_top_five_products',
            configuration={
                'query': {
                    'query': """
                        SELECT 
                            COUNT(*) AS num_records
                        FROM (
                            SELECT 
                                product_id,
                                product_name,
                                total_quantity_sold
                            FROM 
                                TopFiveProducts
                            WHERE 
                                row_num <= 5
                        )
                        HAVING num_records = 5;
                    """,
                    'useLegacySql': False,
                }
            }
        )

        task1 >> quality_check1

    with TaskGroup('TaskGroup2') as tg2:
        task2 = BigQueryInsertJobOperator(
            task_id='query_top_five_products_last_month',
            configuration={
                'query': {
                    'query': """
                        WITH SalesData AS (
                            SELECT 
                                p.product_id,
                                p.product_name,
                                SUM(s.quantity_sold) AS total_quantity_sold
                            FROM 
                                sales s
                            JOIN 
                                products p ON s.product_id = p.product_id
                            WHERE 
                                s.sale_date >= DATE_TRUNC(CURRENT_DATE() - INTERVAL 1 MONTH, MONTH)
                                AND s.sale_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
                            GROUP BY 
                                p.product_id, p.product_name
                        ),
                        RankedProducts AS (
                            SELECT 
                                product_id,
                                product_name,
                                total_quantity_sold,
                                ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS rank
                            FROM 
                                SalesData
                        )
                        SELECT 
                            product_id,
                            product_name,
                            total_quantity_sold
                        FROM 
                            RankedProducts
                        WHERE 
                            rank <= 5;
                    """,
                    'useLegacySql': False,
                }
            }
        )

        quality_check2 = BigQueryInsertJobOperator(
            task_id='quality_check_top_five_products_last_month',
            configuration={
                'query': {
                    'query': """
                        SELECT 
                            COUNT(*) AS num_records
                        FROM (
                            SELECT 
                                product_id,
                                product_name,
                                total_quantity_sold
                            FROM 
                                RankedProducts
                            WHERE 
                                rank <= 5
                        )
                        HAVING num_records = 5;
                    """,
                    'useLegacySql': False,
                }
            }
        )

        task2 >> quality_check2

    tg1 >> tg2