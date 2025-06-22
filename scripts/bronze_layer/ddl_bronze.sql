CREATE OR REPLACE PROCEDURE `internproject-461520.bronze_layer.create_all_external_tables`()
BEGIN
  -- ===== CRM CUSTOMER INFO =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.crm_cus_info` (
        cst_id INT,
        cst_key STRING,
        cst_firstname STRING,
        cst_lastname STRING,
        cst_marital_status STRING,
        cst_gndr STRING,
        cst_create_date DATE
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_crm/cust_info.csv'],
      skip_leading_rows = 1
    )
  """;

  -- ===== CRM PRODUCT INFO =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.crm_prd_info` (
        prd_id INT,
        prd_key STRING,
        prd_nm STRING,
        prd_cost INT,
        prd_line STRING,
        prd_start_dt DATE,
        prd_end_dt DATE
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_crm/prd_info.csv'],
      skip_leading_rows = 1
    )
  """;

  -- ===== CRM SALES DETAILS =====
  EXECUTE IMMEDIATE """
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
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_crm/sales_details.csv'],
      skip_leading_rows = 1
    )
  """;

  -- ===== ERP CUSTOMER AZ12 =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_cust_az12` (
        cid STRING,
        bdate DATE,
        gen STRING
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_erp/CUST_AZ12.csv'],
      skip_leading_rows = 1
    )
  """;

  -- ===== ERP LOCATION A101 =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_loc_a101_external` (
      cid STRING,
      cntry STRING
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_erp/LOC_A101.csv'],
      skip_leading_rows = 1
    )
  """;

  -- ===== ERP PX CAT G1V2 =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE EXTERNAL TABLE `internproject-461520.bronze_layer.erp_px_cat_g1v2` (
      ID STRING,
      CAT STRING,
      SUBCAT STRING,
      MAINTENANCE BOOLEAN
    ) OPTIONS (
      format = 'CSV',
      uris = ['gs://my-bucket-21222/source_erp/PX_CAT_G1V2.csv'],
      skip_leading_rows = 1,
      allow_jagged_rows = true
    )
  """;

  -- Log completion message
  SELECT 'All external tables created successfully' AS status;

END;



-- Execute the procedure to create all external tables
CALL `internproject-461520.bronze_layer.create_all_external_tables`();