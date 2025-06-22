-- ===== SILVER LAYER TABLE CREATION PROCEDURE =====
CREATE OR REPLACE PROCEDURE `internproject-461520.silver_layer.create_all_silver_tables`()
BEGIN
  -- ===== CRM CUSTOMER INFO SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.crm_cus_info` (
        -- Original data columns
        cst_id INT,
        cst_key STRING,
        cst_firstname STRING,
        cst_lastname STRING,
        cst_marital_status STRING,
        cst_gndr STRING,
        cst_create_date DATE,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

    )
    PARTITION BY DATE(loaded_at)
    CLUSTER BY cst_key
  """;

  -- ===== CRM PRODUCT INFO SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.crm_prd_info` (
        prd_id INT,
        prd_key STRING,
        cat_id STRING,
        sls_prd_key STRING,
        prd_nm STRING,
        prd_cost INT,
        prd_line STRING,
        prd_start_dt DATE,
        prd_end_dt DATE,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(loaded_at)
    CLUSTER BY prd_key
  """;

  -- ===== CRM SALES DETAILS SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.crm_sales_details` (
        -- Original data columns
        sls_ord_num STRING,
        sls_prd_key STRING,
        sls_cust_id INT,
        sls_order_dt DATE,
        sls_ship_dt DATE,
        sls_due_dt DATE,
        sls_sales INT,
        sls_quantity INT,
        sls_price INT,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        
    )
    PARTITION BY sls_order_dt
    CLUSTER BY sls_cust_id, sls_prd_key
  """;

  -- ===== ERP CUSTOMER AZ12 SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.erp_cust_az12` (
        -- Original data columns
        cid STRING,
        bdate DATE,
        gen STRING,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()

    )
    PARTITION BY DATE(loaded_at)
    CLUSTER BY cid
  """;

  -- ===== ERP LOCATION A101 SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.erp_loc_a101` (
        -- Original data columns
        cid STRING,
        cntry STRING,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(loaded_at)
    CLUSTER BY cid
  """;

  -- ===== ERP PX CAT G1V2 SILVER =====
  EXECUTE IMMEDIATE """
    CREATE OR REPLACE TABLE `internproject-461520.silver_layer.erp_px_cat_g1v2` (
        -- Original data columns
        ID STRING,
        CAT STRING,
        SUBCAT STRING,
        MAINTENANCE BOOLEAN,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(loaded_at)
    CLUSTER BY ID
  """;

  SELECT 'All silver layer tables created successfully' AS status;
END;

CALL `internproject-461520.silver_layer.create_all_silver_tables`();
