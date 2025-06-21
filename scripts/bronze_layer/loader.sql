--TRUNCATE TABLE `internproject-461520.bronze_layer.crm_cus_info`;

LOAD DATA INTO `internproject-461520.bronze_layer.crm_cus_info`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = false,
  allow_jagged_rows = false,
  uris = ['gs://my-bucket-21222/source_crm/cust_info.csv']
);


SELECT * FROM `internproject-461520.bronze_layer.crm_cus_info` ORDER BY cst_key ASC LIMIT 100;


--TRUNCATE TABLE `internproject-461520.bronze_layer.crm_prd_info`;

LOAD DATA INTO `internproject-461520.bronze_layer.crm_prd_info`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = false,
  allow_jagged_rows = false,
  uris = ['gs://my-bucket-21222/source_crm/prd_info.csv']
);


SELECT * FROM `internproject-461520.bronze_layer.crm_prd_info` ORDER BY prd_id ASC LIMIT 100;


--TRUNCATE TABLE `internproject-461520.bronze_layer.crm_sales_details`;

LOAD DATA INTO `internproject-461520.bronze_layer.crm_sales_details`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = false,
  allow_jagged_rows = false,
  uris = ['gs://my-bucket-21222/source_crm/sales_details.csv']
);


SELECT * FROM `internproject-461520.bronze_layer.crm_sales_details` ORDER BY sls_ord_num ASC LIMIT 100;



--TRUNCATE TABLE `internproject-461520.bronze_layer.erp_cust_az12`;

LOAD DATA INTO `internproject-461520.bronze_layer.erp_cust_az12`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = false,
  allow_jagged_rows = false,
  uris = ['gs://my-bucket-21222/source_erp/CUST_AZ12.csv']
);


SELECT * FROM `internproject-461520.bronze_layer.erp_cust_az12` ORDER BY CID ASC LIMIT 100;



--TRUNCATE TABLE `internproject-461520.bronze_layer.erp_loc_a101`;

INSERT INTO `internproject-461520.bronze_layer.erp_loc_a101`
SELECT 
  TRIM(cid) as cid,
  CASE 
    WHEN TRIM(cntry) = '' THEN NULL 
    ELSE TRIM(cntry) 
  END as cntry
FROM `internproject-461520.bronze_layer.erp_loc_a101_external`;


SELECT * FROM `internproject-461520.bronze_layer.erp_loc_a101` ORDER BY CID ASC LIMIT 100;


--TRUNCATE TABLE `internproject-461520.bronze_layer.erp_erp_px_cat_g1v2`;

LOAD DATA INTO `internproject-461520.bronze_layer.erp_px_cat_g1v2`
FROM FILES (
  format = 'CSV',
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_jagged_rows = true,  -- This helps with inconsistent rows
  allow_quoted_newlines = false,
  uris = ['gs://my-bucket-21222/source_erp/PX_CAT_G1V2.csv']
);


SELECT * FROM `internproject-461520.bronze_layer.erp_px_cat_g1v2` ORDER BY CID ASC LIMIT 100;