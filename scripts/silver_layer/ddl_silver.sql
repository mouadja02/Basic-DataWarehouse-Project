CREATE OR REPLACE PROCEDURE `internproject-461520.silver_layer.load_all_silver_tables`()
BEGIN
  DECLARE error_message STRING;  -- Add this declaration
  
  BEGIN 
  
  -- ===== CRM CUSTOMER INFO =====
  SELECT 'Starting CRM Customer Info cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.crm_cus_info`;
  INSERT INTO `internproject-461520.silver_layer.crm_cus_info` (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
  )
  SELECT 
    cst_id, 
    cst_key, 
    TRIM(cst_firstname) as cst_firstname, 
    TRIM(cst_lastname) as cst_lastname,
    CASE 
      WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
      WHEN UPPER(TRIM(cst_marital_status)) = 'D' THEN 'Divorced'
      WHEN UPPER(TRIM(cst_marital_status)) = 'W' THEN 'Widowed'
      ELSE 'n/a'
    END AS cst_marital_status, 
    CASE 
      WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
      ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM `internproject-461520.bronze_layer.crm_cus_info`
  )
  WHERE flag = 1 AND cst_id IS NOT NULL;

  -- ===== CRM PRODUCT INFO =====
  SELECT 'Starting CRM Product Info cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.crm_prd_info`;
  INSERT INTO `internproject-461520.silver_layer.crm_prd_info` (
    prd_id, prd_key, cat_id, sls_prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
  )
  SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS sls_prd_key,
    prd_nm,
    IFNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
      WHEN 'R' THEN 'Road'
      WHEN 'M' THEN 'Mountain'
      WHEN 'S' THEN 'other Sales'
      WHEN 'T' THEN 'Touring'
      ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
  FROM `internproject-461520.bronze_layer.crm_prd_info`;

  -- ===== CRM SALES DETAILS =====
  SELECT 'Starting CRM Sales Details cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.crm_sales_details`;
  INSERT INTO `internproject-461520.silver_layer.crm_sales_details` (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
  )
  SELECT 
    sls_ord_num,
    T.sls_prd_key as sls_prd_key,
    T.sls_cust_id as sls_cust_id,
    T.sls_order_dt as sls_order_dt,
    T.sls_ship_dt as sls_ship_dt,
    T.sls_due_dt as sls_due_dt,
    T.sls_sales as sls_sales,
    T.sls_quantity as sls_quantity,
    CAST(
      CASE 
        WHEN T.sls_price IS NULL THEN T.sls_sales / NULLIF(T.sls_quantity, 0)
        ELSE T.sls_price
      END
      AS INT64) AS sls_price
  FROM (
    SELECT 
      sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      CASE 
        WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
        ELSE PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING))
      END AS sls_order_dt,
      CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
        ELSE PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING))
      END AS sls_ship_dt,
      CASE 
        WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
        ELSE PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING))
      END AS sls_due_dt,
      CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
      END AS sls_sales,
      sls_quantity,
      CASE 
        WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
        WHEN sls_price < 0 THEN -sls_price
        ELSE sls_price
      END AS sls_price
    FROM `internproject-461520.bronze_layer.crm_sales_details`
  ) T;

  -- ===== ERP CUSTOMER AZ12 =====
  SELECT 'Starting ERP Customer AZ12 cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.erp_cust_az12`;
  INSERT INTO `internproject-461520.silver_layer.erp_cust_az12` (cid, bdate, gen)
  SELECT  
    CASE 
      WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) 
      ELSE cid
    END as cid,
    CASE 
      WHEN bdate > CURRENT_DATE() THEN NULL
      ELSE bdate
    END as bdate,
    CASE 
      WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
    END as gen
  FROM `internproject-461520.bronze_layer.erp_cust_az12`;

  -- ===== ERP LOCATION A101 =====
  SELECT 'Starting ERP Location A101 cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.erp_loc_a101`;
  INSERT INTO `internproject-461520.silver_layer.erp_loc_a101` (cid, cntry)
  SELECT 
    REPLACE(cid, '-', '') as cid,
    CASE IFNULL(UPPER(TRIM(cntry)), '')
      WHEN 'AT' THEN 'Austria'
      WHEN 'BE' THEN 'Belgium'
      WHEN 'CH' THEN 'Switzerland'
      WHEN 'CZ' THEN 'Czech Republic'
      WHEN 'DK' THEN 'Denmark'
      WHEN 'FI' THEN 'Finland'
      WHEN 'NL' THEN 'Netherlands'
      WHEN 'NO' THEN 'Norway'
      WHEN 'DE' THEN 'Germany'
      WHEN 'FR' THEN 'France'
      WHEN 'IT' THEN 'Italy'
      WHEN 'ES' THEN 'Spain'
      WHEN 'US' THEN 'United States'
      WHEN 'USA' THEN 'United States'
      WHEN 'GB' THEN 'United Kingdom'
      WHEN 'UK' THEN 'United Kingdom'
      WHEN '' THEN 'n/a'
      ELSE cntry
    END AS cntry
  FROM `internproject-461520.bronze_layer.erp_loc_a101`;

  -- ===== ERP PX CAT G1V2 =====
  SELECT 'Starting ERP PX CAT G1V2 cleaning...' AS status;
  
  TRUNCATE TABLE `internproject-461520.silver_layer.erp_px_cat_g1v2`;
  INSERT INTO `internproject-461520.silver_layer.erp_px_cat_g1v2` (id, cat, subcat, maintenance)
  SELECT 
    ID,
    TRIM(CAT) AS CAT,
    TRIM(SUBCAT) AS SUBCAT,
    MAINTENANCE
  FROM `internproject-461520.bronze_layer.erp_px_cat_g1v2`;

  -- ===== COMPLETION MESSAGE =====
  SELECT '🎉 All silver layer tables successfully truncated and loaded!' AS final_status;
  
  -- ===== SUMMARY COUNTS =====
  SELECT 'Data Loading Summary:' AS summary;
  
  SELECT 
    'crm_cus_info' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.crm_cus_info`
  
  UNION ALL
  
  SELECT 
    'crm_prd_info' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.crm_prd_info`
  
  UNION ALL
  
  SELECT 
    'crm_sales_details' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.crm_sales_details`
  
  UNION ALL
  
  SELECT 
    'erp_cust_az12' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.erp_cust_az12`
  
  UNION ALL
  
  SELECT 
    'erp_loc_a101' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.erp_loc_a101`
  
  UNION ALL
  
  SELECT 
    'erp_px_cat_g1v2' as table_name,
    COUNT(*) as record_count
  FROM `internproject-461520.silver_layer.erp_px_cat_g1v2`;

    EXCEPTION WHEN ERROR THEN
    SET error_message = @@error.message;
    SELECT CONCAT('❌ Error processing table: ', table_name, ' - ', error_message) AS error_status;
    RAISE USING MESSAGE = CONCAT('Failed to process silver layer table: ', table_name, ' - ', error_message);
  END;
END;


CALL `internproject-461520.silver_layer.load_all_silver_tables`();
