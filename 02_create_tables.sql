-- ============================================================
-- Step 2: Create monitoring tables in DbaMonitor
-- ============================================================
USE DbaMonitor;
GO

-- ── metric_snapshots ─────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = 'metric_snapshots')
BEGIN
    CREATE TABLE dbo.metric_snapshots (
        snapshot_id          BIGINT        IDENTITY(1,1) NOT NULL,
        server_id            INT           NOT NULL DEFAULT 0,
        collected_at         DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        cpu_pct              FLOAT         NOT NULL DEFAULT 0,
        mem_used_mb          FLOAT         NOT NULL DEFAULT 0,
        mem_total_mb         FLOAT         NOT NULL DEFAULT 0,
        mem_usage_pct        FLOAT         NOT NULL DEFAULT 0,
        active_connections   INT           NOT NULL DEFAULT 0,
        queries_per_sec      FLOAT         NOT NULL DEFAULT 0,
        page_life_expectancy FLOAT         NOT NULL DEFAULT 0,
        uptime_hours         INT           NOT NULL DEFAULT 0,
        db_status            NVARCHAR(64)  NOT NULL DEFAULT 'UNKNOWN',
        blocking_count       INT           NOT NULL DEFAULT 0,
        long_query_count     INT           NOT NULL DEFAULT 0,
        exported_to_databricks BIT         NOT NULL DEFAULT 0,

        CONSTRAINT PK_metric_snapshots PRIMARY KEY (snapshot_id)
    );
    CREATE INDEX IX_metric_snapshots_collected
        ON dbo.metric_snapshots (server_id, collected_at DESC);
    PRINT 'Table metric_snapshots created.';
END
GO

-- ── session_log ───────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = 'session_log')
BEGIN
    CREATE TABLE dbo.session_log (
        log_id          BIGINT        IDENTITY(1,1) NOT NULL,
        server_id       INT           NOT NULL DEFAULT 0,
        snapshot_id     BIGINT        NULL,
        captured_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        session_id      INT           NOT NULL,
        login_name      NVARCHAR(256) NULL,
        host_name       NVARCHAR(256) NULL,
        database_name   NVARCHAR(256) NULL,
        status          NVARCHAR(64)  NULL,
        wait_type       NVARCHAR(64)  NULL,
        wait_time_ms    INT           NOT NULL DEFAULT 0,
        cpu_time_ms     INT           NOT NULL DEFAULT 0,
        logical_reads   BIGINT        NOT NULL DEFAULT 0,
        is_blocking     BIT           NOT NULL DEFAULT 0,
        blocked_by_spid INT           NULL,
        command_text    NVARCHAR(500) NULL,

        CONSTRAINT PK_session_log PRIMARY KEY (log_id)
    );
    CREATE INDEX IX_session_log_captured
        ON dbo.session_log (server_id, captured_at DESC);
    CREATE INDEX IX_session_log_blocking
        ON dbo.session_log (is_blocking, captured_at DESC);
    PRINT 'Table session_log created.';
END
GO

-- ── blocker_log ───────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = 'blocker_log')
BEGIN
    CREATE TABLE dbo.blocker_log (
        blocker_id       BIGINT        IDENTITY(1,1) NOT NULL,
        server_id        INT           NOT NULL DEFAULT 0,
        captured_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        blocker_spid     INT           NOT NULL,
        blocked_spid     INT           NOT NULL,
        blocker_login    NVARCHAR(256) NULL,
        blocked_login    NVARCHAR(256) NULL,
        wait_time_ms     INT           NOT NULL DEFAULT 0,
        wait_type        NVARCHAR(64)  NULL,
        blocked_resource NVARCHAR(256) NULL,
        blocker_sql      NVARCHAR(500) NULL,
        blocked_sql      NVARCHAR(500) NULL,

        CONSTRAINT PK_blocker_log PRIMARY KEY (blocker_id)
    );
    CREATE INDEX IX_blocker_log_captured
        ON dbo.blocker_log (server_id, captured_at DESC);
    PRINT 'Table blocker_log created.';
END
GO

-- ── alert_events ──────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = 'alert_events')
BEGIN
    CREATE TABLE dbo.alert_events (
        alert_id      BIGINT        IDENTITY(1,1) NOT NULL,
        server_id     INT           NOT NULL DEFAULT 0,
        snapshot_id   BIGINT        NULL,
        triggered_at  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        metric_name   NVARCHAR(64)  NOT NULL,
        severity      NVARCHAR(16)  NOT NULL DEFAULT 'MED', -- LOW | MED | HIGH
        threshold_val FLOAT         NOT NULL DEFAULT 0,
        actual_val    FLOAT         NOT NULL DEFAULT 0,
        is_resolved   BIT           NOT NULL DEFAULT 0,
        resolved_at   DATETIME2     NULL,

        CONSTRAINT PK_alert_events PRIMARY KEY (alert_id)
    );
    CREATE INDEX IX_alert_events_triggered
        ON dbo.alert_events (server_id, triggered_at DESC);
    PRINT 'Table alert_events created.';
END
GO

-- ── export_log ────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE name = 'export_log')
BEGIN
    CREATE TABLE dbo.export_log (
        export_id        INT           IDENTITY(1,1) NOT NULL,
        server_id        INT           NOT NULL DEFAULT 0,
        table_name       NVARCHAR(128) NOT NULL,
        last_exported_at DATETIME2     NULL,
        last_exported_id BIGINT        NOT NULL DEFAULT 0,
        rows_exported    INT           NOT NULL DEFAULT 0,

        CONSTRAINT PK_export_log PRIMARY KEY (export_id),
        CONSTRAINT UQ_export_log UNIQUE (server_id, table_name)
    );
    -- Seed one row per table
    INSERT INTO dbo.export_log (server_id, table_name, last_exported_id)
    VALUES (0, 'metric_snapshots', 0),
           (0, 'session_log',      0),
           (0, 'blocker_log',      0),
           (0, 'alert_events',     0);
    PRINT 'Table export_log created and seeded.';
END
GO
