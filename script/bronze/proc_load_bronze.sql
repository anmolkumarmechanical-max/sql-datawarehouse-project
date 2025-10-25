/*
==============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==============================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==============================================================
*/


Create or Alter Procedure bronze.load_bronze AS
BEGIN
   Declare @Start_Time Datetime, @End_Time Datetime, @batch_Start_Time Datetime, @batch_end_time Datetime;
   BEGIN TRY
     Set @batch_Start_Time = GETDATE();
     Print '======================================';
     Print 'Loading Bronze Layer';
     Print '======================================';

     Print'---------------------------------------';
     Print'Loading ERP Tables';
     Print'---------------------------------------';

     Set @Start_Time = GETDATE();
     Print'>> Truncating Table: bronze.crm_cust_info'
Truncate Table bronze.crm_cust_info;

     Print'Inserting Data into: bronze.crm_cust_info'
Bulk Insert bronze.crm_cust_info
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
     Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

      Set @Start_Time = GETDATE();
     Print'>> Truncating Table:  bronze.crm_prd_info'
Truncate Table bronze.crm_prd_info;

     Print'Inserting Data into: bronze.crm_prd_info'
Bulk Insert bronze.crm_prd_info
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
     Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

     Set @Start_Time = GETDATE();
     Print'>> Truncating Table: bronze.crm_sales_details'
Truncate Table bronze.crm_sales_details;

     Print'Inserting Data into: bronze.crm_sales_details'
Bulk Insert bronze.crm_sales_details
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
    Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'


     Print'---------------------------------------';
     Print'Loading ERP Tables';
     Print'---------------------------------------';

     Set @Start_Time = GETDATE();
     Print'>> Truncating Table: bronze.erp_loc_a101'
Truncate Table bronze.erp_loc_a101;

     Print'Inserting Data into: bronze.erp_loc_a101'
Bulk Insert bronze.erp_loc_a101
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
     Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

     Set @Start_Time = GETDATE();
     Print'>> Truncating Table: bronze.erp_cust_az12'
Truncate Table bronze.erp_cust_az12;
     
     Print'Inserting Data into: bronze.erp_cust_az12'
Bulk Insert bronze.erp_cust_az12
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
     Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

     Set @Start_Time = GETDATE();
     Print'>> Truncating Table: bronze.erp_px_cat_g1v2'
Truncate Table bronze.erp_px_cat_g1v2;

     Print'Inserting Data into: bronze.erp_px_cat_g1v2'
Bulk Insert bronze.erp_px_cat_g1v2
From 'D:\sql1 - Copy\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
With (
     Firstrow = 2,
     FieldTerminator = ',',
     Tablock
     );
     Set @End_Time = GETDATE();
     Print'>> Load Duration:'+ Cast(DateDiff(second, @Start_Time, @End_Time) As NVARCHAR) + 'seconds';
     Print'-----------------'

     Set @batch_end_time = GETDATE();
     Print'======================================='
     Print'Loading Bronze layer is Completed';
     Print'>> Total Load Duration:'+ Cast(DateDiff(second, @batch_start_time, @batch_end_time) As NVARCHAR) + 'seconds';
     Print'----------------------------'
     END TRY
     BEGIN CATCH
     Print '======================================';
     Print 'Error Occured During Loading Bronze Layer';
     Print 'Error Message' + Error_Message();
     Print 'Error Message' + Cast (Error_Number() as NVARCHAR);
     Print 'Error Message' + Cast (Error_State() as NVARCHAR);
     Print '======================================';
     END CATCH
END

     
