-------------------------- DIM CUSTOMERS --------------------------

CREATE VIEW IF NOT EXISTS gold_layer.dim_customers AS
SELECT  ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS customer_firstname,
        ci.cst_lastname AS customer_lastname,
        la.cntry as customer_country, 
        ci.cst_marital_status AS customer_marital_status,
        CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a') END AS customer_gender,
        ca.bdate as customer_birth_date,
        ci.cst_create_date as create_date
FROM silver_layer.crm_cus_info ci
LEFT JOIN silver_layer.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver_layer.erp_loc_a101 la
ON ca.cid = la.cid;

SELECT DISTINCT customer_gender FROM gold_layer.dim_customers LIMIT 100;


SELECT DISTINCT
        ci.cst_gndr,
        ca.gen
FROM silver_layer.crm_cus_info ci
LEFT JOIN silver_layer.erp_cust_az12 ca 
ON ci.cst_key = ca.cid
LEFT JOIN silver_layer.erp_loc_a101 la
ON ca.cid = la.cid;


-------------------------- DIM PRODUCTS --------------------------

CREATE OR REPLACE VIEW gold_layer.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.prd_id,ci.prd_key) AS product_key,
    ci.prd_id as product_id,
    ci.sls_prd_key as product_number,
    ci.prd_nm as product_name,
    ci.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    ci.prd_cost as product_cost,
    ci.prd_line as product_line,
    pc.maintenance as maintenance,
    ci.prd_start_dt as product_start_date
FROM silver_layer.crm_prd_info ci
LEFT JOIN silver_layer.erp_px_cat_g1v2 pc 
ON ci.cat_id = pc.id
WHERE ci.prd_end_dt IS NULL;



-------------------------- FACT SALES --------------------------

CREATE OR REPLACE VIEW gold_layer.fact_sales AS
SELECT
    sd.sls_ord_num as order_number,
    p.product_number,
    c.customer_id,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_price as price,
    sd.sls_quantity as quantity,
    sd.sls_sales as sales_amount
FROM silver_layer.crm_sales_details sd
LEFT JOIN gold_layer.dim_products p
ON sd.sls_prd_key = p.product_number
LEFT JOIN gold_layer.dim_customers c
ON sd.sls_cust_id = c.customer_id;



-------------------------- GLOBAL GOLD LAYER JOIN --------------------------

SELECT * FROM gold_layer.dim_customers 
LEFT JOIN gold_layer.fact_sales
ON dim_customers.customer_id = fact_sales.customer_id
LEFT JOIN gold_layer.dim_products
ON fact_sales.product_number = dim_products.product_number
WHERE  dim_products.product_key IS NULL;