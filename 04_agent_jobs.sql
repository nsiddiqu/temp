-- ============================================================
-- Step 4: SQL Server Agent Jobs
-- Run on each monitored SQL Server.
-- Requires SQL Server Agent service to be running.
-- ============================================================
USE msdb;
GO

-- ── Job 1: Collect metrics every 60 seconds ───────────────────
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'DbaAgent - Collect Metrics')
BEGIN
    EXEC msdb.dbo.sp_add_job
        @job_name = N'DbaAgent - Collect Metrics',
        @description = N'Collects SQL Server health metrics into DbaMonitor every 60 seconds.',
        @enabled = 1;

    EXEC msdb.dbo.sp_add_jobstep
        @job_name    = N'DbaAgent - Collect Metrics',
        @step_name   = N'Run sp_collect_metrics',
        @command     = N'EXEC DbaMonitor.dbo.sp_collect_metrics @server_id = 0;',
        @database_name = N'DbaMonitor';

    EXEC msdb.dbo.sp_add_schedule
        @schedule_name      = N'Every 60 seconds',
        @freq_type          = 4,       -- daily
        @freq_interval      = 1,
        @freq_subday_type   = 2,       -- seconds
        @freq_subday_interval = 60;

    EXEC msdb.dbo.sp_attach_schedule
        @job_name      = N'DbaAgent - Collect Metrics',
        @schedule_name = N'Every 60 seconds';

    EXEC msdb.dbo.sp_add_jobserver
        @job_name = N'DbaAgent - Collect Metrics';

    PRINT 'Job [DbaAgent - Collect Metrics] created.';
END
GO

-- ── Job 2: Purge old data daily at 02:00 ─────────────────────
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = 'DbaAgent - Purge Old Data')
BEGIN
    EXEC msdb.dbo.sp_add_job
        @job_name = N'DbaAgent - Purge Old Data',
        @description = N'Deletes monitoring data older than 7 days from DbaMonitor.',
        @enabled = 1;

    EXEC msdb.dbo.sp_add_jobstep
        @job_name      = N'DbaAgent - Purge Old Data',
        @step_name     = N'Run sp_purge_old_data',
        @command       = N'EXEC DbaMonitor.dbo.sp_purge_old_data @retention_days = 7;',
        @database_name = N'DbaMonitor';

    EXEC msdb.dbo.sp_add_schedule
        @schedule_name    = N'Daily 2AM',
        @freq_type        = 4,
        @freq_interval    = 1,
        @active_start_time = 020000;   -- 02:00:00

    EXEC msdb.dbo.sp_attach_schedule
        @job_name      = N'DbaAgent - Purge Old Data',
        @schedule_name = N'Daily 2AM';

    EXEC msdb.dbo.sp_add_jobserver
        @job_name = N'DbaAgent - Purge Old Data';

    PRINT 'Job [DbaAgent - Purge Old Data] created.';
END
GO
