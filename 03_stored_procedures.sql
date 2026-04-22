-- ============================================================
-- Step 3: Stored procedures called by SQL Agent jobs
-- ============================================================
USE DbaMonitor;
GO

-- ── sp_collect_metrics ────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.sp_collect_metrics
    @server_id INT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @snapshot_id BIGINT;
    DECLARE @cpu_pct     FLOAT   = 0;
    DECLARE @used_mb     FLOAT   = 0;
    DECLARE @total_mb    FLOAT   = 0;
    DECLARE @mem_pct     FLOAT   = 0;
    DECLARE @active_conn INT     = 0;
    DECLARE @qps         FLOAT   = 0;
    DECLARE @ple         FLOAT   = 0;
    DECLARE @uptime_hrs  INT     = 0;
    DECLARE @db_status   NVARCHAR(64) = 'UNKNOWN';
    DECLARE @block_count INT     = 0;
    DECLARE @lrq_count   INT     = 0;

    -- CPU
    BEGIN TRY
        SELECT TOP 1 @cpu_pct = (100 - r.SystemIdle)
        FROM (
            SELECT rx.record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]','int') AS SystemIdle,
                   rx.record.value('(./Record/@id)[1]','int') AS record_id
            FROM (SELECT CONVERT(XML,record) AS record
                  FROM sys.dm_os_ring_buffers WITH (NOLOCK)
                  WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%') rx
        ) r ORDER BY r.record_id DESC;
    END TRY BEGIN CATCH END CATCH;

    -- Memory
    BEGIN TRY
        SELECT @used_mb  = CONVERT(FLOAT,(total_physical_memory_kb - available_physical_memory_kb))/(1024*1024),
               @total_mb = CONVERT(FLOAT,total_physical_memory_kb)/(1024*1024)
        FROM sys.dm_os_sys_memory;
        SET @mem_pct = CASE WHEN @total_mb > 0 THEN ROUND(@used_mb/@total_mb*100,1) ELSE 0 END;
    END TRY BEGIN CATCH END CATCH;

    -- Active connections
    BEGIN TRY
        SELECT @active_conn = COUNT(*) FROM sys.dm_exec_sessions WITH (NOLOCK)
        WHERE is_user_process = 1 AND status != 'sleeping';
    END TRY BEGIN CATCH END CATCH;

    -- QPS (cumulative counter)
    BEGIN TRY
        SELECT TOP 1 @qps = CONVERT(FLOAT,cntr_value)
        FROM sys.dm_os_performance_counters WITH (NOLOCK)
        WHERE counter_name LIKE 'Batch Requests/sec%';
    END TRY BEGIN CATCH END CATCH;

    -- PLE
    BEGIN TRY
        SELECT TOP 1 @ple = CONVERT(FLOAT,cntr_value)
        FROM sys.dm_os_performance_counters WITH (NOLOCK)
        WHERE counter_name = 'Page life expectancy'
          AND object_name LIKE '%Buffer Manager%';
    END TRY BEGIN CATCH END CATCH;

    -- Uptime
    BEGIN TRY
        SELECT @uptime_hrs = DATEDIFF(HOUR, sqlserver_start_time, GETDATE())
        FROM sys.dm_os_sys_info;
    END TRY BEGIN CATCH END CATCH;

    -- DB Status
    BEGIN TRY
        SELECT TOP 1 @db_status = state_desc FROM sys.databases WHERE name = DB_NAME();
    END TRY BEGIN CATCH END CATCH;

    -- Blocking count
    BEGIN TRY
        SELECT @block_count = COUNT(DISTINCT blocking_session_id)
        FROM sys.dm_exec_requests WITH (NOLOCK)
        WHERE blocking_session_id > 0;
    END TRY BEGIN CATCH END CATCH;

    -- Long running queries (> 60s)
    BEGIN TRY
        SELECT @lrq_count = COUNT(*)
        FROM sys.dm_exec_requests r WITH (NOLOCK)
        JOIN sys.dm_exec_sessions s WITH (NOLOCK) ON r.session_id = s.session_id
        WHERE s.is_user_process = 1
          AND DATEDIFF(SECOND, r.start_time, GETDATE()) > 60;
    END TRY BEGIN CATCH END CATCH;

    -- Insert snapshot
    INSERT INTO dbo.metric_snapshots
        (server_id, cpu_pct, mem_used_mb, mem_total_mb, mem_usage_pct,
         active_connections, queries_per_sec, page_life_expectancy,
         uptime_hours, db_status, blocking_count, long_query_count)
    VALUES
        (@server_id, @cpu_pct, @used_mb, @total_mb, @mem_pct,
         @active_conn, @qps, @ple, @uptime_hrs, @db_status,
         @block_count, @lrq_count);

    SET @snapshot_id = SCOPE_IDENTITY();

    -- Capture sessions
    BEGIN TRY
        INSERT INTO dbo.session_log
            (server_id, snapshot_id, session_id, login_name, host_name,
             database_name, status, wait_type, wait_time_ms, cpu_time_ms,
             logical_reads, is_blocking, blocked_by_spid, command_text)
        SELECT
            @server_id, @snapshot_id,
            s.session_id,
            CAST(s.login_name AS NVARCHAR(256)),
            CAST(s.host_name AS NVARCHAR(256)),
            CAST(DB_NAME(r.database_id) AS NVARCHAR(256)),
            CAST(s.status AS NVARCHAR(64)),
            CAST(r.wait_type AS NVARCHAR(64)),
            ISNULL(r.wait_time, 0),
            ISNULL(r.cpu_time, 0),
            ISNULL(r.logical_reads, 0),
            CASE WHEN EXISTS (SELECT 1 FROM sys.dm_exec_requests r2 WITH (NOLOCK)
                              WHERE r2.blocking_session_id = s.session_id) THEN 1 ELSE 0 END,
            r.blocking_session_id,
            CAST(SUBSTRING(REPLACE(REPLACE(CAST(st.text AS NVARCHAR(MAX)),CHAR(13),' '),CHAR(10),' '),1,500) AS NVARCHAR(500))
        FROM sys.dm_exec_sessions s WITH (NOLOCK)
        LEFT JOIN sys.dm_exec_requests r WITH (NOLOCK) ON s.session_id = r.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
        WHERE s.is_user_process = 1;
    END TRY BEGIN CATCH END CATCH;

    -- Capture blockers
    IF @block_count > 0
    BEGIN TRY
        INSERT INTO dbo.blocker_log
            (server_id, blocker_spid, blocked_spid, blocker_login, blocked_login,
             wait_time_ms, wait_type, blocked_resource, blocker_sql, blocked_sql)
        SELECT
            @server_id,
            r.blocking_session_id,
            r.session_id,
            CAST(bs.login_name AS NVARCHAR(256)),
            CAST(s.login_name AS NVARCHAR(256)),
            r.wait_time,
            CAST(r.wait_type AS NVARCHAR(64)),
            CAST(r.wait_resource AS NVARCHAR(256)),
            CAST(SUBSTRING(REPLACE(REPLACE(CAST(bt.text AS NVARCHAR(MAX)),CHAR(13),' '),CHAR(10),' '),1,500) AS NVARCHAR(500)),
            CAST(SUBSTRING(REPLACE(REPLACE(CAST(st.text AS NVARCHAR(MAX)),CHAR(13),' '),CHAR(10),' '),1,500) AS NVARCHAR(500))
        FROM sys.dm_exec_requests r WITH (NOLOCK)
        JOIN sys.dm_exec_sessions s  WITH (NOLOCK) ON r.session_id = s.session_id
        JOIN sys.dm_exec_sessions bs WITH (NOLOCK) ON r.blocking_session_id = bs.session_id
        OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
        OUTER APPLY (SELECT sql_handle FROM sys.dm_exec_requests WITH (NOLOCK)
                     WHERE session_id = r.blocking_session_id) br
        OUTER APPLY sys.dm_exec_sql_text(br.sql_handle) bt
        WHERE r.blocking_session_id > 0;
    END TRY BEGIN CATCH END CATCH;

    -- Alert check
    BEGIN TRY
        IF @cpu_pct > 85
            INSERT INTO dbo.alert_events (server_id, snapshot_id, metric_name, severity, threshold_val, actual_val)
            VALUES (@server_id, @snapshot_id, 'cpu_pct', 'HIGH', 85, @cpu_pct);

        IF @mem_pct > 90
            INSERT INTO dbo.alert_events (server_id, snapshot_id, metric_name, severity, threshold_val, actual_val)
            VALUES (@server_id, @snapshot_id, 'mem_usage_pct', 'HIGH', 90, @mem_pct);

        IF @ple < 300 AND @ple > 0
            INSERT INTO dbo.alert_events (server_id, snapshot_id, metric_name, severity, threshold_val, actual_val)
            VALUES (@server_id, @snapshot_id, 'page_life_expectancy', 'MED', 300, @ple);

        IF @block_count >= 3
            INSERT INTO dbo.alert_events (server_id, snapshot_id, metric_name, severity, threshold_val, actual_val)
            VALUES (@server_id, @snapshot_id, 'blocking_count', 'HIGH', 3, @block_count);
    END TRY BEGIN CATCH END CATCH;
END;
GO

-- ── sp_purge_old_data ─────────────────────────────────────────
CREATE OR ALTER PROCEDURE dbo.sp_purge_old_data
    @retention_days INT = 7
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cutoff DATETIME2 = DATEADD(DAY, -@retention_days, SYSUTCDATETIME());

    DELETE FROM dbo.blocker_log   WHERE captured_at < @cutoff;
    DELETE FROM dbo.session_log   WHERE captured_at < @cutoff;
    DELETE FROM dbo.alert_events  WHERE triggered_at < @cutoff;
    DELETE FROM dbo.metric_snapshots WHERE collected_at < @cutoff;

    PRINT 'Purge complete. Cutoff: ' + CONVERT(NVARCHAR, @cutoff, 120);
END;
GO
