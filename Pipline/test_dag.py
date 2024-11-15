from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def data_quality_check(**kwargs):
    # Placeholder for data quality check logic
    pass

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'P2',
    default_args=default_args,
    description='DAG for BigQuery operations with data quality checks',
    schedule_interval=timedelta(days=1),
)

with dag:
    with TaskGroup('sales_analysis') as sales_analysis:
        query_task = BigQueryExecuteQueryOperator(
            task_id='execute_sales_query',
            sql="""
            WITH LastMonthSales AS (
                SELECT 
                    p.product_id,
                    p.product_name,
                    SUM(s.quantity) AS total_quantity_sold
                FROM 
                    sales s
                JOIN 
                    products p ON s.product_id = p.product_id
                WHERE 
                    s.sale_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                    AND s.sale_date < DATE_TRUNC('month', CURRENT_DATE)
                GROUP BY 
                    p.product_id, p.product_name
            ),
            TopFiveProducts AS (
                SELECT 
                    product_id,
                    product_name,
                    total_quantity_sold,
                    RANK() OVER (ORDER BY total_quantity_sold DESC) AS rank
                FROM 
                    LastMonthSales
            )
            SELECT 
                product_id,
                product_name,
                total_quantity_sold
            FROM 
                TopFiveProducts
            WHERE 
                rank <= 5;
            """,
            use_legacy_sql=False,
            location='US',
        )

        quality_check_task = PythonOperator(
            task_id='sales_query_quality_check',
            python_callable=data_quality_check,
        )

        query_task >> quality_check_task

    sales_analysis