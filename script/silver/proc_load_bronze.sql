/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    Exec Silver.load_silver;
===============================================================================
*/

Exec Silver.load_silver
Create or Alter Procedure silver.load_silver as
Begin
 Declare @Start_Time Datetime, @End_Time Datetime, @batch_Start_Time Datetime, @batch_end_time Datetime;
   BEGIN TRY
     Set @batch_Start_Time = GETDATE();
     Print '======================================';
     Print 'Loading Silver Layer';
     Print '======================================';

     Print'---------------------------------------';
     Print'Loading ERP Tables';
     Print'---------------------------------------';

     Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.crm_cust_info';
Truncate Table silver.crm_cust_info;
Print '>> Inserting Data into: silver.crm_cust_info'; 
Insert into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)
SELECT  
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM
(
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1
  AND cst_id IS NOT NULL;  
   Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

 Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.crm_prd_info';
Truncate Table silver.crm_prd_info;
Print '>> Inserting Data into: silver.crm_prd_info'; 
Insert into silver.crm_prd_info
(
prd_id,
cat_id, 
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
Select
   prd_id,
   Replace(SUBSTRING (prd_key,1,5),'-','_') as cat_id,
   SUBSTRING (prd_key,7,len(prd_key)) as prd_key, 
   prd_nm,
   isnull(prd_cost, 0) as  prd_cost,
   case when upper(Trim(prd_line)) = 'M' then 'Mountain'
        when upper(Trim(prd_line)) = 'R' then 'Road'
        when upper(Trim(prd_line)) = 'S' then 'Other Sales'
        when upper(Trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
   end prd_line, 
   cast(prd_start_dt as date) as prd_start_dt,
  cast(lead(prd_start_dt) over (partition by Prd_key order by prd_start_dt) -1 as date) as prd_end_dt 
   from bronze.crm_prd_info;
   Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.crm_sales_details';
Truncate Table silver.crm_sales_details;
Print '>> Inserting Data into: silver.crm_sales_details'; 
Insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)

Select
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or Len(sls_order_dt)!=8 then Null
     else Cast(Cast (sls_order_dt as varchar) as Date)
     end as sls_order_dt,
case when sls_ship_dt = 0 or Len(sls_ship_dt)!=8 then Null
     else Cast(Cast (sls_ship_dt as varchar) as Date)
     end as sls_ship_dt,
case when sls_due_dt = 0 or Len(sls_due_dt)!=8 then Null
     else Cast(Cast (sls_due_dt as varchar) as Date)
     end as sls_due_dt,
 case when sls_sales is null or sls_sales<=0 or sls_sales!= sls_quantity * ABS(sls_price)
          then sls_quantity *  ABS(sls_price)
          else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price <= 0
     then sls_sales / Nullif(sls_quantity,0)
     else sls_price
end as sls_price
from bronze.crm_sales_details
Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'


     Print'---------------------------------------';
     Print'Loading ERP Tables';
     Print'---------------------------------------';

 Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.erp_loc_a101';
Truncate Table silver.erp_loc_a101;
Print '>> Inserting Data into: silver.erp_loc_a101';
Insert into silver.erp_loc_a101 
(cid,
cntry
)
Select 
    Replace(cid,'-','') as cid,
    Case When Trim(cntry)= 'DE' Then 'Germany'
         When Trim(cntry) in ('US', 'USA') then 'United States'
         When Trim(cntry) = '' or cntry is null then 'n/a'
         Else Trim(cntry)
    End as cntry 
 from bronze.erp_loc_a101;
  Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'     

     
Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.erp_cust_az12';
Truncate Table silver.erp_cust_az12;
Print '>> Inserting Data into: silver.erp_cust_az12'; 
Insert into silver.erp_cust_az12(cid, bdate, gen)
select 
case when cid like 'NAS%' Then Substring(cid, 4, len(cid))
     else cid
end as cid,
Case When bdate > GETDATE() Then null
     else bdate
end as bdate,
case when Upper(Trim(gen)) in ('F', 'Female') Then 'Female'
     when Upper(Trim(gen)) in ('M', 'Male') Then 'Male'
     Else 'n/a'
end as gen
from bronze.erp_cust_az12;
 Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'


Set @Start_Time = GETDATE();
Print '>> Truncating Table: silver.erp_px_cat_g1v2';
Truncate Table silver.erp_px_cat_g1v2;
Print '>> Inserting Data into: silver.erp_px_cat_g1v2'
Insert into silver.erp_px_cat_g1v2 (
   id,
   cat,
   subcat,
   maintenance
   )
   Select
   id,
   cat,
   subcat,
   maintenance
   from bronze.erp_px_cat_g1v2;
    Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'     

     Set @batch_end_time = GETDATE();
     Print'======================================='
     Print'Loading Silver layer is Completed';
     Print'>> Total Load Duration:'+ Cast(DateDiff(second, @batch_start_time, @batch_end_time) As NVARCHAR) + 'seconds';
     Print'----------------------------'
     END TRY
     BEGIN CATCH
     Print '======================================';
     Print 'Error Occured During Loading Silver Layer';
     Print 'Error Message' + Error_Message();
     Print 'Error Message' + Cast (Error_Number() as NVARCHAR);
     Print 'Error Message' + Cast (Error_State() as NVARCHAR);
     Print '======================================';
     END CATCH
END

