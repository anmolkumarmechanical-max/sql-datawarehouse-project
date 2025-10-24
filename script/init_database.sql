/*
===========================================================
Create Datbase and Schemas
===========================================================

Puprose of Script:
A new database 'Datawarehouse' is created  under this script after checking if it already exists.
If the database exists, it is dropped and recreated. Additioally, the script setups three schemas 
within the database: 'bronze', 'silver and 'gold'.

Caution:
Running this script will drop the entire 'Datawarehouse' database if it exists.
All the data in the database will be permanently deleted. Ensure you have proper 
backups before running this script.
/*

Use master;
GO


-- Drop and recreate 'DataWarehouse' database
If exists (Select 1 from sys.databases Where name = 'DataWarehouse')
BEGIN
    Alter Database Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP Database DataWarehouse;
END;
GO

-- Create Database DataWarehouse
Create Database DataWarehouse;

Use DataWarehouse;
GO

-- Create Schemas
Create Schema bronze;
GO

Create Schema silver;
GO

Create Schema gold;
