-- Active: 1750521811453@@127.0.0.1@3306@bronze_layer
CREATE OR REPLACE TABLE bronze_layer.crm_cus_info (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE
);

CREATE OR REPLACE TABLE bronze_layer.crm_prd_info (
    prd_id INT,
    prd_key STRING,
    prd_nm STRING,
    prd_cost INT,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE
);

CREATE OR REPLACE TABLE bronze_layer.crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

CREATE OR REPLACE TABLE bronze_layer.erp_cust_az12 (
    cid STRING,
    bdate DATE,
    gen STRING
);

CREATE OR REPLACE TABLE bronze_layer.erp_loc_a101 (
  CID STRING,
  CNTRY STRING
);

CREATE OR REPLACE TABLE bronze_layer.erp_px_cat_g1v2 (
  ID STRING,
  CAT STRING,
  SUBCAT STRING,
  MAINTENANCE BOOLEAN
);


------- EXTERNAL TABLES ----------

-- ===== CRM CUSTOMER INFO =====
-- Create external table for CRM customer info
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.crm_cus_info` (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_crm/cust_info.csv'],
  skip_leading_rows = 1
);

SELECT * FROM `internproject-461520.bronze_layer.crm_cus_info` ORDER BY cst_key ASC LIMIT 100;

-- ===== CRM PRODUCT INFO =====
-- Create external table for CRM product info
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.crm_prd_info` (
    prd_id INT,
    prd_key STRING,
    prd_nm STRING,
    prd_cost INT,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_crm/prd_info.csv'],
  skip_leading_rows = 1
);


SELECT * FROM `internproject-461520.bronze_layer.crm_prd_info` ORDER BY prd_id ASC LIMIT 100;

-- ===== CRM SALES DETAILS =====
-- Create external table for CRM sales details
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.crm_sales_details` (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_crm/sales_details.csv'],
  skip_leading_rows = 1
);

SELECT * FROM `internproject-461520.bronze_layer.crm_sales_details` ORDER BY sls_ord_num ASC LIMIT 100;

-- ===== ERP CUSTOMER AZ12 =====
-- Create external table for ERP customer AZ12
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_cust_az12` (
    cid STRING,
    bdate DATE,
    gen STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_erp/CUST_AZ12.csv'],
  skip_leading_rows = 1
);

SELECT * FROM `internproject-461520.bronze_layer.erp_cust_az12` ORDER BY CID ASC LIMIT 100;

-- ===== ERP LOCATION A101 =====
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_loc_a101` (
  cid STRING,
  cntry STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_erp/LOC_A101.csv'],
  skip_leading_rows = 1
);

-- Test the external table
SELECT * FROM `internproject-461520.bronze_layer.erp_loc_a101` LIMIT 5;


-- ===== ERP PX CAT G1V2 =====
-- Create external table for ERP PX CAT G1V2
CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_px_cat_g1v2` (
  ID STRING,
  CAT STRING,
  SUBCAT STRING,
  MAINTENANCE BOOLEAN
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://my-bucket-21222/source_erp/PX_CAT_G1V2.csv'],
  skip_leading_rows = 1,
  allow_jagged_rows = true
);

SELECT * FROM `internproject-461520.bronze_layer.erp_px_cat_g1v2` ORDER BY ID ASC LIMIT 100;