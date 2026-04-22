-- ============================================================
-- Step 1: Create DbaMonitor database
-- Run on each SQL Server you want to monitor locally.
-- ============================================================
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DbaMonitor')
BEGIN
    CREATE DATABASE DbaMonitor;
    PRINT 'DbaMonitor database created.';
END
ELSE
    PRINT 'DbaMonitor already exists — skipped.';
GO

USE DbaMonitor;
GO

-- Grant dba_agent user access
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'dba_agent')
BEGIN
    CREATE USER dba_agent FOR LOGIN dba_agent;
    ALTER ROLE db_owner ADD MEMBER dba_agent;
    PRINT 'dba_agent user created in DbaMonitor.';
END
GO
