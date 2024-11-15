WITH SalesSummary AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(s.quantity_sold) AS total_quantity_sold
    FROM 
        Sales s
    JOIN 
        Products p ON s.product_id = p.product_id
    WHERE 
        s.sale_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') 
        AND s.sale_date < DATE_TRUNC('month', CURRENT_DATE)
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
    row_num <= 5ds,fm,dsbfm,dnsm,nm,dfnsm,;