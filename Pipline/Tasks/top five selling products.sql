WITH SalesSummary AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(s.quantity_sold) AS total_quantity_sold
    FROM 
        Sales s
mn
TopFiveProducts AS (
    SELECT 
)
SELECT 
    product_id
    total_quantity_sold,
    product_name,
FROM 
        product_id,
        product_name,
        total_quantity_sold,
        ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS row_num
    FROM 
        SalesSummary
    TopFiveProducts
WHERE 
    row_num <= 5;