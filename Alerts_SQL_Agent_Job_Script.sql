USE [msdb]
GO

/****** Object:  Job [[Alert] DBCC workday check]    Script Date: 6/6/2026 9:59:12 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[UHT Infrastructure]]    Script Date: 6/6/2026 9:59:12 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[UHT Infrastructure]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[UHT Infrastructure]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] DBCC workday check', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[UHT Infrastructure]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [DBCC command]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DBCC command', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_DBCCCheck'') is not null
    drop table #SQL_DBCCCheck

declare @AlertType varchar(50)
set @AlertType = ''SQL_DBCCCheck''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @SessionID int
declare @Database_Name varchar(120)
declare @cmd varchar(20)
declare @Text varchar(max)
declare @login_name varchar(50)
declare @host varchar(50)
declare @Program varchar(400)
declare @Last_Wait_Type varchar(20)
declare @max_used_memory_kb int

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ss.SessionID,ss.Database_Name, ss.cmd, ss.Text, ss.[Program], ss.login_name, ss.Host, cast(ss.[Last_Wait_Type] as varchar) as [Last_Wait_Type], 
ss.[max_used_memory_kb]
---------------------
into #SQL_DBCCCheck
FROM [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] ss with (nolock)
inner join [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) on  ss.SQL_Instance_Name = si.SQL_Instance_Name
where ([cmd] like ''%DBCC%'' or [cmd] like ''%UPDATE STATISTIC%'' or [cmd] like ''%ALTER INDEX%'')
--and Program like ''%agent%'' 
-- and WaitTime > 20000 and si.role_desc not like ''%DR%''


WHILE EXISTS(SELECT * FROM #SQL_DBCCCheck)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Program =[Program], @Database_Name = [Database_Name] ,@cmd = [cmd], @Text  = [Text] ,@login_name = [login_name], @SessionID = [SessionID],
@Host = [Host], @Last_Wait_Type = [Last_Wait_Type], @max_used_memory_kb = [max_used_memory_kb]
---------------------
from #SQL_DBCCCheck order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] DBCC command ''+@cmd+'' Found''
set @bodytext = ''Server ''+ @SQL_Instance_Name +'' SQL Agent running DBCC command found.
AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''

Database  ''+@Database_Name +''
Command   ''+@cmd+''
Last Wait ''+@Last_Wait_Type+''

DBCC below

''+ SUBSTRING(@Text,1,1800)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_DBCCCheck where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_DBCCCheck order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'DBCCRUN', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=62, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20201029, 
		@active_end_date=99991231, 
		@active_start_time=60000, 
		@active_end_time=175959, 
		@schedule_uid=N'c153e34e-342d-4c69-8537-5082761baf18'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] Log Space Used]    Script Date: 6/6/2026 9:59:12 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[UHT Infrastructure]]    Script Date: 6/6/2026 9:59:12 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[UHT Infrastructure]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[UHT Infrastructure]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] Log Space Used', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[UHT Infrastructure]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Log space used]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Log space used', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Drive_Log'') is not null
    drop table #SQL_Drive_Log

declare @AlertType varchar(50)
set @AlertType = ''SQL_Drive_Log''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Drive varchar(200)
declare @Log_Size_GB int
declare @Log_Used_Size_GB Int
declare @Drive_Free_Space_GB int
declare @Drive_Full_Percent int
declare @log_reuse_wait_desc varchar (200)
declare @Database_name varchar (100)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sdd.[Database_name],recovery_model_desc,[Log_Size_GB],[Log_Used_Size_GB],[log_reuse_wait_desc],[Percent_Log_Used]
,Drive,Drive_Free_Space_GB,Drive_Full_Percent
---------------------
into #SQL_Drive_Log
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) 
inner join[DBA_Watcher_DB].[General].[SQL_Database_Details_Current] sdd  with (nolock) on sdd.SQL_Instance_Name = si.SQL_Instance_Name
Inner Join [DBA_Watcher_DB].[General].[SQL_Database_File_Details_Current] sdfd with (nolock) on  sdd.SQL_Instance_Name = sdfd.SQL_Instance_Name and sdd.Database_name = sdfd.database_name
where Log_Used_Size_GB >150 and sdd.Database_name not like ''_Total'' and sdd.state_desc = ''ONLINE'' and user_access_desc = ''MULTI_USER'' and sdfd.type_desc = ''LOG''
--and Department like ''%Prod%'' 
and (si.[role_desc] like ''%Main%'' or si.dns_Name like '''')
order by Log_Used_Size_GB desc

WHILE EXISTS(SELECT * FROM #SQL_Drive_Log)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Database_name = [Database_name],@Drive = [Drive], @Log_Size_GB = [Log_Size_GB], @Log_Used_Size_GB = Log_Used_Size_GB, @Drive_Free_Space_GB = [Drive_Free_Space_GB]
,@Drive_Full_Percent = [Drive_Full_Percent],@log_reuse_wait_desc = log_reuse_wait_desc
---------------------
from #SQL_Drive_Log order by SQL_Instance_Name


---------------------
set @EmailAddress = @Pager

Set @header = ''[''+ @DepartmentShort +''][''+ cast(@role_desc as varchar) +''] ''+ @Database_name +'' log is '' + cast(@Log_Used_Size_GB as varchar) + ''GB used Wait=''+@log_reuse_wait_desc+''''
set @bodytext = ''AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''

Database     = ''+ cast(@Database_name as varchar)+''
Drive        = ''+ cast(@Drive as varchar)+''
Percent Full = ''+ cast(@Drive_Full_Percent as varchar)+''
Log Size     = ''+ cast(@Log_Size_GB as varchar)+''
Log Size Used= ''+ cast(@Log_Used_Size_GB as varchar)+''
Space Free   = ''+ cast(@Drive_Free_Space_GB as varchar)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Drive_Log where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Drive_Log order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Logspacesched', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=127, 
		@freq_subday_type=8, 
		@freq_subday_interval=4, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20221118, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'39958c90-7e9a-41bf-ab75-c9b6737da188'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] Missing Log Backup]    Script Date: 6/6/2026 9:59:12 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[UHT Infrastructure]]    Script Date: 6/6/2026 9:59:12 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[UHT Infrastructure]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[UHT Infrastructure]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] Missing Log Backup', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[UHT Infrastructure]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Missing Backup]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Missing Backup', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_Backup'') is not null
    drop table ##SQL_Backup

declare @AlertType varchar(50)
set @AlertType = ''SQL_Backup''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @count int
declare @Log_Size_GB int
declare @Log_Used_Size_GB Int
declare @log_reuse_wait_desc varchar (200)
declare @Database_name varchar (100)

select si.SQL_Instance_Name, si.[role_desc],si.team, si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sdd.[Database_name],recovery_model_desc,[Log_Size_GB],[Log_Used_Size_GB],[log_reuse_wait_desc],[Percent_Log_Used]
[Backup_Duration_Min],Max([LastBackupStart])[LastBackupStart],max([LastBackupFinish])[LastBackupFinish]
---------------------
into ##SQL_Backup
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Database_Backup_Current] aj  with (nolock) on aj.[SQL_Instance_Name] = si.[SQL_Instance_Name]
inner join [DBA_Watcher_DB].[General].[SQL_Database_Details_Current] sdd  with (nolock) on sdd.SQL_Instance_Name = si.SQL_Instance_Name and aj.[Database_name] like sdd.[Database_name]
Inner Join [DBA_Watcher_DB].[General].[SQL_Database_File_Details_Current] sdfd with (nolock) on  sdd.SQL_Instance_Name = sdfd.SQL_Instance_Name and sdd.Database_name = sdfd.database_name
where BackupType = ''DB Log'' and Recovery = ''FULL'' and aj.Database_name not like ''uhtdba'' and sdd.state_desc = ''ONLINE'' and si.Department like ''%prod%''
and (role_desc like '''' or role_desc like ''%Main%'') and Patchtime = ''''
group by si.SQL_Instance_Name, si.[role_desc],si.team, si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager],sdd.[Database_name],recovery_model_desc
,[Log_Size_GB],[Log_Used_Size_GB],[log_reuse_wait_desc],[Percent_Log_Used],aj.[Pull_Time]
having max([LastBackupStart]) < dateadd(minute, -120, aj.[Pull_Time]) and max([LastBackupFinish]) > dateadd(day, -2, aj.[Pull_Time])  

delete from ##SQL_Backup where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_Backup)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Database_name = [Database_name], @Log_Size_GB = [Log_Size_GB], @Log_Used_Size_GB = Log_Used_Size_GB, @log_reuse_wait_desc = log_reuse_wait_desc
---------------------
from ##SQL_Backup order by SQL_Instance_Name

---------------------
Set @count = (select distinct count(*) from ##SQL_Backup where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_Backup))

set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' No log backup in 2 hours''
set @bodytext = ''No log backup in over 2 hours for Server ''+ cast(@role_desc as varchar) +''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
Customer  = '' +@customer_email+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[Database_name],[LastBackupStart],[LastBackupFinish],[Log_Used_Size_GB],[log_reuse_wait_desc] from ##SQL_Backup where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_Backup)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @query_result_no_padding = 1,
    @attach_query_result_as_file= 0;

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_Backup where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_Backup order by SQL_Instance_Name)
END  

if object_id(''tempdb..##SQL_Backup'') is not null
    drop table ##SQL_Backup
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'missingbackup', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=30, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20230922, 
		@active_end_date=99991231, 
		@active_start_time=400, 
		@active_end_time=235959, 
		@schedule_uid=N'671e04fd-2a14-4d0a-80b8-f71b392691f3'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Database Alerts]    Script Date: 6/6/2026 9:59:12 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 6/6/2026 9:59:12 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Database Alerts', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Drive Space]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Drive Space', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Drive'') is not null
    drop table #SQL_Drive

declare @AlertType varchar(50)
set @AlertType = ''SQL_Drive''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Drive varchar(200)
declare @Drive_Size_GB int
declare @Drive_Free_Space_GB int
declare @Drive_Full_Percent int
declare @Ticket varchar(200)
declare @Date_Entered varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sfd.[Drive],sfd.[Drive_Size_GB],sfd.[Drive_Free_Space_GB],sfd.[Drive_Full_Percent],[Ticket],[Date_Entered]
---------------------
into #SQL_Drive
  FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) 
  join [DBA_Watcher_DB].[General].[SQL_Database_File_Details_Current] sfd with (nolock) on si.SQL_Instance_Name = sfd.SQL_Instance_name
  join [DBA_Watcher_DB].[General].[SQL_Database_Details_Current] sdd  with (nolock) on sfd.SQL_Instance_name = sdd.SQL_Instance_Name and sfd.database_name = sdd.Database_name 
	and sdd.database_name not in (''master'',''tempdb'',''model'',''msdb'',''uhtdba'')
  join [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] sidd with (nolock) on sfd.SQL_Instance_name = sidd.SQL_Instance_Name 
  left outer join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_File_Details] sfd2 with (nolock) on sfd.SQL_Instance_Name = sfd2.SQL_Instance_name and sfd.Drive = sfd2.drive
  left outer join [DBA_Watcher_DB].[Settings].[DriveSpace_Request] dr with (nolock) on si.SQL_Instance_Name = dr.SQL_Instance_Name COLLATE DATABASE_DEFAULT and sfd.Drive = dr.[Drive] COLLATE DATABASE_DEFAULT
  where sdd.is_read_only not like  ''Read Only'' and sfd.state_desc like ''ONLINE'' and sfd.database_name not like ''uhtdba'' and sfd.growth !=0  and sfd.[Drive_Full_Percent] >=80
  and ((sfd.[Drive_Full_Percent] >=01 and sfd2.[Drive_Full_Percent] < 80)
  or (sfd.[Drive_Full_Percent] >=85 and sfd2.[Drive_Full_Percent] < 85)
  or (sfd.[Drive_Full_Percent] >=90 and sfd2.[Drive_Full_Percent] < 90)
  or (sfd.[Drive_Full_Percent] >=95 and sfd2.[Drive_Full_Percent] < 95)
  or (sfd.[Drive_Full_Percent] >=98 and sfd2.[Drive_Full_Percent] < 98)
  or (sfd.[Drive_Full_Percent] >=99 and sfd2.[Drive_Full_Percent] < 99)
  or (sfd.[Drive_Full_Percent] >=100 and sfd2.[Drive_Full_Percent] < 100)
  or sidd.LastRestartTime> DATEADD (MINUTE, -10, GETDATE()))

delete from #SQL_Drive where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Drive)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Drive = [Drive], @Drive_Size_GB = [Drive_Size_GB], @Drive_Free_Space_GB = [Drive_Free_Space_GB],@Drive_Full_Percent = [Drive_Full_Percent],@Ticket = [Ticket]
,@Date_Entered = [Date_Entered]
---------------------
from #SQL_Drive order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] ''+ @Drive +'' is '' + cast(@Drive_Full_Percent as varchar) + ''% Full''
set @bodytext = ''AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Drive        = ''+ cast(@Drive as varchar)+''
Percent Full = ''+ cast(@Drive_Full_Percent as varchar)+''
Space Free   = ''+ cast(@Drive_Free_Space_GB as varchar)+''
Total Space  = ''+ cast(@Drive_Size_GB as varchar)+''

Ticket= ''+ cast(@Ticket as varchar)+'' ''+ cast(@Date_Entered as varchar)+''''


EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Drive where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Drive order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Drive Space Alert Cleared]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Drive Space Alert Cleared', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_DriveClear'') is not null
    drop table #SQL_DriveClear

declare @AlertType varchar(50)
set @AlertType = ''SQL_DriveClear''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Drive varchar(200)
declare @Drive_Size_GB int
declare @Drive_Free_Space_GB int
declare @Drive_Full_Percent int

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sfd.[Drive],sfd.[Drive_Size_GB],sfd.[Drive_Free_Space_GB],sfd.[Drive_Full_Percent]
---------------------
into #SQL_DriveClear
  FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
  join [DBA_Watcher_DB].[General].[SQL_Database_File_Details_Current] sfd with (nolock) on si.SQL_Instance_Name = sfd.SQL_Instance_name
  join [DBA_Watcher_DB].[General].[SQL_Database_Details_Current] sdd  with (nolock) on sfd.SQL_Instance_name = sdd.SQL_Instance_Name and sfd.database_name = sdd.Database_name 
	and sdd.database_name not in (''master'',''tempdb'',''model'',''msdb'',''uhtdba'')
  join [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] sidd with (nolock) on sfd.SQL_Instance_name = sidd.SQL_Instance_Name 
  left outer join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_File_Details] sfd2 with (nolock) on sfd.SQL_Instance_Name = sfd2.SQL_Instance_name and sfd.Drive = sfd2.drive
  where sdd.is_read_only not like  ''Read Only'' and sfd.state_desc like ''ONLINE'' and sfd.growth !=0 and sfd.database_name not like ''uhtdba''
  and (sfd.[Drive_Full_Percent] < 80 and sfd2.[Drive_Full_Percent] >= 80)

delete from #SQL_DriveClear where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_DriveClear)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Drive = [Drive], @Drive_Size_GB = [Drive_Size_GB], @Drive_Free_Space_GB = [Drive_Free_Space_GB],@Drive_Full_Percent = [Drive_Full_Percent]
---------------------
from #SQL_DriveClear order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] ''+ @Drive +'' Alert Cleared''
set @bodytext = ''AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Drive        = ''+ cast(@Drive as varchar)+''
Percent Full = ''+ cast(@Drive_Full_Percent as varchar)+''
Space Free   = ''+ cast(@Drive_Free_Space_GB as varchar)+''
Total Space  = ''+ cast(@Drive_Size_GB as varchar)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_DriveClear where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_DriveClear order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [AlwaysOn not in healthy state]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'AlwaysOn not in healthy state', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_AlwaysOn_Health'') is not null
    drop table #SQL_AlwaysOn_Health

declare @AlertType varchar(50)
set @AlertType = ''SQL_AlwaysOn_Health''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_AlwaysOn_Health
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Database_AlwaysOn_Current] sda with (nolock) on si.SQL_Instance_Name = sda.SQL_Instance_Name
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_AlwaysOn] sdal with (nolock) on sda.SQL_Instance_Name = sdal.SQL_Instance_Name
and sda.[synchronization_health_desc] not like ''HEALTHY'' and sdal.[synchronization_health_desc] not like ''HEALTHY'' 
and sdal.[synchronization_health_desc] not like '''' and sda.[synchronization_health_desc] not like ''''
and si.SQL_Instance_Name not in (select SQL_Instance_Name from [DBA_Watcher_DB].[Notifications].[Alert_History] with (nolock) where [Send_Time]>dateadd(minute, -30, getdate()) 
and [Method] = ''Email'' and Header like ''% not Healthy%'')

delete from #SQL_AlwaysOn_Health where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_AlwaysOn_Health)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_AlwaysOn_Health order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ @dns_name +'']''+@Patchtime+'' AlwaysOn node ''+ cast(@role_desc as varchar) +'' not Healthy''
set @bodytext = ''''+ @Department +'' AlwaysOn Server ''+ @dns_name +'' node ''+ @SQL_Instance_Name +'' not in healthy state.

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_AlwaysOn_Health where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_AlwaysOn_Health order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [AlwaysOn Failover]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'AlwaysOn Failover', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_AlwaysOn_FailOver'') is not null
    drop table #SQL_AlwaysOn_FailOver

declare @AlertType varchar(50)
set @AlertType = ''SQL_AlwaysOn_FailOver''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], CASE WHEN si.[Patch] = '''' THEN si2.[Patch] else si.[Patch] end as [Patch], CASE WHEN si.[Patchtime] = '''' THEN si2.[Patchtime] else si.[Patchtime] end as [Patchtime]
,si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_AlwaysOn_FailOver
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Database_AlwaysOn_Current] sdac with(nolock) on si.SQL_Instance_Name = sdac.SQL_Instance_Name
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_AlwaysOn] sdal with(nolock) on sdac.[dns_name] = sdal.[dns_name]
inner join [DBA_Watcher_DB].[dbo].[Inventory_All] si2 with (nolock) on sdal.SQL_Instance_Name = si2.SQL_Instance_Name
where sdac.[database_name] = sdal.[database_name] and sdac.SQL_Instance_Name = si.SQL_Instance_Name and
sdac.role_desc = ''PRIMARY'' and sdal.role_desc = ''PRIMARY'' and sdac.SQL_Instance_Name not like sdal.SQL_Instance_Name

delete from #SQL_AlwaysOn_FailOver where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_AlwaysOn_FailOver)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_AlwaysOn_FailOver order by SQL_Instance_Name


---------------------
set @EmailAddress = @Customer_Email_group

Set @header = ''['' + @DepartmentShort + ''][''+ @dns_name +'']''+@Patchtime+'' AlwaysOn Failover found''
set @bodytext = ''''+ @Department +'' AlwaysOn Server ''+ @dns_name +'' Failover found.  Current Primary node is ''+ @SQL_Instance_Name +''

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_AlwaysOn_FailOver where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_AlwaysOn_FailOver order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [AlwaysOn Synchronized]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'AlwaysOn Synchronized', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_AlwaysOn_Sync'') is not null
    drop table #SQL_AlwaysOn_Sync

declare @AlertType varchar(50)
set @AlertType = ''SQL_AlwaysOn_Sync''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_AlwaysOn_Sync
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Database_AlwaysOn_Current] sda with (nolock) on si.SQL_Instance_Name = sda.SQL_Instance_Name
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_AlwaysOn] sdal with (nolock) on sda.SQL_Instance_Name = sdal.SQL_Instance_Name
and sda.[synchronization_state_desc] not like ''SYNCHRONIZED'' and sdal.[synchronization_state_desc] not like ''SYNCHRONIZED'' and sdal.[synchronization_state_desc] not like '''' 
and sda.[synchronization_state_desc] not like '''' and sda.[synchronization_health_desc] like ''HEALTHY'' and sda.[availability_mode_desc] not like ''ASYNCHRONOUS_COMMIT%''
and si.SQL_Instance_Name not in (select SQL_Instance_Name from [DBA_Watcher_DB].[Notifications].[Alert_History] with (nolock) where [Send_Time]>dateadd(minute, -30, getdate()) 
and [Method] = ''Email'' and Header like ''% SYNCHRONIZED%'')

delete from #SQL_AlwaysOn_Sync where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_AlwaysOn_Sync)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_AlwaysOn_Sync order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ @dns_name +''] AlwaysOn node ''+ cast(@role_desc as varchar) +'' not Synchronized''
set @bodytext = ''''+ @Department +'' AlwaysOn Server ''+ @dns_name +'' node ''+ @SQL_Instance_Name +'' not in SYNCHRONIZED state.

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_AlwaysOn_Sync where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_AlwaysOn_Sync order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Database Status]    Script Date: 6/6/2026 9:59:12 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Database Status', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_DBStatus'') is not null
    drop table #SQL_DBStatus

declare @AlertType varchar(50)
set @AlertType = ''SQL_DBStatus''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @database_name varchar(200)
declare @State_Current varchar(200)
declare @State_Before varchar(200)
declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime,sfd.Database_name, sfd.state_desc as [State_Current], sfd2.state_desc as [State_Before]
---------------------
into #SQL_DBStatus
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
join [DBA_Watcher_DB].[General].[SQL_Database_File_Details_Current] sfd with (nolock) on si.SQL_Instance_Name = sfd.SQL_Instance_name
join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Database_File_Details] sfd2 with (nolock) on sfd.SQL_Instance_Name = sfd2.SQL_Instance_name and sfd.database_name = sfd2.database_name
where si.Department like ''%Prod%'' and sfd.state_desc not like sfd2.state_desc and sfd.Database_name not like ''_Total''

delete from #SQL_DBStatus where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_DBStatus)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar), @database_name = [Database_name],@State_Current = [State_Current], @State_Before = [State_Before]
---------------------
from #SQL_DBStatus order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' ''+cast(@database_name as varchar)+'' Database Status Change''
set @bodytext = ''AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Database Name  = ''+ cast(@database_name as varchar)+''
Current Status = ''+ cast(@State_Current as varchar)+''
previous status= ''+ cast(@State_Before as varchar)+''

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_DBStatus where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_DBStatus order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [TempDB Free space]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'TempDB Free space', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_tempdb'') is not null
    drop table #SQL_tempdb

declare @AlertType varchar(50)
set @AlertType = ''SQL_tempdb''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Free_Space_in_tempdb_GB int
declare @Data_Size_GB int
declare @Drive_Free_Space_GB int
declare @Used_Percent int
declare @SessionID int
declare @Database_Name varchar(120)
declare @cmd varchar(20)
declare @Text varchar(max)
declare @login_name varchar(50)
declare @host varchar(50)
declare @InternalMB float

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sses.SessionID, sses.Database_Name, sses.cmd, sses.Text, sses.login_name, sses.Host, sses.[InternalMB],ss.[Free_Space_in_tempdb_GB] ,sdd.[Data_Size_GB]
, round(((sdd.[Data_Size_GB]-ss.[Free_Space_in_tempdb_GB])/sdd.[Data_Size_GB])*100,2) as [Used_Percent]
---------------------
into #SQL_tempdb
from [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Statistics_Usage_Current] ss with (nolock) on si.SQL_Instance_Name = ss.SQL_Instance_Name
inner join [DBA_Watcher_DB].[General].[SQL_Database_Details_Current] sdd with (nolock) on (si.SQL_Instance_Name = sdd.SQL_Instance_Name and sdd.Database_name like ''tempdb'')
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Statistics_Usage] lss with (nolock) on si.SQL_Instance_Name = lss.SQL_Instance_Name
inner join [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] sses with (nolock) on si.SQL_Instance_Name = sses.SQL_Instance_Name
where sdd.Data_Size_GB >=10 and round(((sdd.[Data_Size_GB]-ss.[Free_Space_in_tempdb_GB])/sdd.[Data_Size_GB])*100,2)>=80
and (round(((sdd.[Data_Size_GB]-lss.[Free_Space_in_tempdb_GB])/sdd.[Data_Size_GB])*100,2)<80 or (round(((sdd.[Data_Size_GB]-ss.[Free_Space_in_tempdb_GB])/sdd.[Data_Size_GB])*100,0) > round(((sdd.[Data_Size_GB]-lss.[Free_Space_in_tempdb_GB])/sdd.[Data_Size_GB])*100,0)))
and sses.[InternalMB]>0 and Department like ''%prod%'' order by sses.[InternalMB] desc

delete from #SQL_tempdb where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_tempdb)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Free_Space_in_tempdb_GB = [Free_Space_in_tempdb_GB], @Data_Size_GB = [Data_Size_GB], @Database_Name = [Database_Name] ,@cmd = [cmd], @Text  = [Text] 
,@login_name = [login_name], @SessionID = [SessionID],@Host = [Host], @InternalMB = [InternalMB],@Used_Percent = [Used_Percent]
---------------------
from #SQL_tempdb order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] TempDB is '' + cast(@Used_Percent as varchar) + ''% Full''
set @bodytext = ''TempDB Total Space ''+ cast(@Data_Size_GB as varchar) +'' GB
TempDB Space Free  ''+ cast(@Free_Space_in_tempdb_GB as varchar) +'' GB

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Top tempDB query
Command   ''+@cmd+''
Database  ''+@Database_Name +''
LoginName ''+@login_name+''
Host      ''+@host+''
Max Memory KB ''+cast(@InternalMB as varchar)+''

''+ SUBSTRING(@Text,1,1600)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_tempdb where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_tempdb order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Run_disk', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190404, 
		@active_end_date=99991231, 
		@active_start_time=600, 
		@active_end_time=235959, 
		@schedule_uid=N'87cb153b-82e0-430f-8e72-ccf16006ed36'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Error Log Alerts]    Script Date: 6/6/2026 9:59:13 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 6/6/2026 9:59:13 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Error Log Alerts', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Failed login]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Failed login', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_FailedLogin'') is not null
    drop table ##SQL_FailedLogin

declare @AlertType varchar(50)
set @AlertType = ''SQL_FailedLogin''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @count int

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_FailedLogin
	FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
	inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
	inner join [DBA_Watcher_DB].[Daily].[SQL_Server_Logins] sl with (nolock) on si.SQL_Instance_Name = sl.SQL_Instance_Name
	where ssec.text like ''%Login failed for%'' and (si.Department like ''%Prod%'' or si.Customer_Email not like '''') and ssec.text not like ''%secure_id_maint%''
	and ssec.text not like ''%Failed to open the explicitly specified database%'' and ssec.text not like ''%Could not find a login matching the name%''
	and ssec.text not like ''%Login failed for user sa. Reason: An error occurred while evaluating the password%''
	order by si.sql_instance_name, ssec.LogDate desc

delete from ##SQL_FailedLogin where SQL_Instance_Name in (select distinct SQL_Instance_Name from ##SQL_FailedLogin group by SQL_Instance_Name having count(*)<4)

delete from ##SQL_FailedLogin where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])


WHILE EXISTS(SELECT * FROM ##SQL_FailedLogin)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------

---------------------
from ##SQL_FailedLogin order by SQL_Instance_Name

Set @count = (select distinct count(*) from ##SQL_FailedLogin where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_FailedLogin))
---------------------
set @EmailAddress = @Customer_Email_group

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' ''+cast(@count as varchar)+'' Failed login events''
set @bodytext = ''Failed login events for ''+@dns_Name+'' Server ''+ cast(@role_desc as varchar) +''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
Customer  = '' +@customer_email+''
''+@Patch+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_FailedLogin where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_FailedLogin)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @query_result_no_padding = 1,
    @attach_query_result_as_file= 0;

delete from ##SQL_FailedLogin where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_FailedLogin)
END  

if object_id(''tempdb..##SQL_FailedLogin'') is not null
    drop table ##SQL_FailedLogin

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Error Logs]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Error Logs', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_ErrorLog'') is not null
    drop table ##SQL_ErrorLog

declare @AlertType varchar(50)
set @AlertType = ''SQL_ErrorLog''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @count int

select distinct si.SQL_Instance_Name, si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_ErrorLog
	FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
	inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
	inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec2 with (nolock) on ssec.SQL_Instance_Name = ssec2.SQL_Instance_Name 
		and ssec.LogDate = ssec2.LogDate and ssec.ProcessInfo = ssec2.ProcessInfo
	where ssec.text not like ''%The process could not execute sp_repldone/sp_replcounters%''  and ssec.text not like ''%Change Tracking autocleanup%'' and ssec.text not like ''%The process could not execute sp_MSpub_adjust_identity%''  and ssec.text not like ''%The client was unable to reuse a session with SPID%'' 
	and ssec.text not like ''%The login is from an untrusted domain%'' and ssec.text not like ''%SSPI%'' and ssec.text not like ''%Either data movement is suspended or the availability replica is not enabled for read access%'' 
	and ssec.text not like ''%IMPERSONATE%'' and ssec.text not like ''%errors occurred in the Security Check procedure%'' and ssec.text not like ''%connections when the application intent is set to read only%'' and
	ssec.text not like ''%connection timeout has occurred on a previously established%'' and ssec.text not like ''%Login failed for%'' and ssec.text not like ''%connection is structurally invalid%'' and ssec.text not like ''%reading the input stream from the network%'' and
	ssec.text not like ''%NEW TRACE NOT CREATED%'' and ssec.text not like ''%use of xp_cmdshell by using sp_configure%'' 
	and ssec.text not like ''%ERROR INSERTING INTO SQLTRACE%'' and ssec.text not like ''%Length specified in network packet%'' and ssec.text not like ''%An error occurred in a Service Broker/Database Mirroring%''
	and ssec.text not like ''%administrator connections already exists%'' and ssec.text not like ''%TLS certificate is not configured to accept strict%'' and ssec.text not like ''%error number was found in sys.messages%''
	and ((ssec2.text like ''%Severity:%'' and ssec.text not like ''%Severity:%''))
	order by si.sql_instance_name, ssec.LogDate desc

WHILE EXISTS(SELECT * FROM ##SQL_ErrorLog)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------

---------------------
from ##SQL_ErrorLog order by SQL_Instance_Name

Set @count = (select distinct count(*) from ##SQL_ErrorLog where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_ErrorLog))
---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' ''+cast(@count as varchar)+'' Critical SQL Error events''
set @bodytext = ''Critical SQL Error events for ''+@dns_Name+'' Server ''+ cast(@role_desc as varchar) +''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
Customer  = '' +@customer_email+''
''+@Patch+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_ErrorLog where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_ErrorLog)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @query_result_no_padding = 1,
    @attach_query_result_as_file= 0;

delete from ##SQL_ErrorLog where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_ErrorLog)
END  

if object_id(''tempdb..##SQL_ErrorLog'') is not null
    drop table ##SQL_ErrorLog

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Account Locked out]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Account Locked out', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_LoginLock'') is not null
    drop table ##SQL_LoginLock

declare @AlertType varchar(50)
set @AlertType = ''SQL_LoginLock''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @loginname varchar(200)
declare @AccountOwner varchar(200)
declare @AccountOwnerEmail varchar(200)
declare @SupervisorName varchar(200)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,sse.loginname, cast(isnull([FullName],'''') as varchar) as AccountOwner, cast(isnull([SupervisorName],'''') as varchar) as SupervisorName ,cast(isnull([Email],'''') as varchar) as AccountOwnerEmail
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_LoginLock
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
inner join [DBA_Watcher_DB].[Daily].[SQL_Server_Logins] sse with (nolock) on si.SQL_Instance_Name = sse.SQL_Instance_Name and ssec.[Text] like ''%''+loginname+''%'' 
left outer join [DBA_Watcher_ServiceNow].[Secure].[SQLAccountOwnerDetails] saod with (nolock) on sse.loginname = saod.UserId
left outer join [DBA_Watcher_ServiceNow].[ServiceNow].[sm_contactsAll] SCN with(nolock) on saod.EmployeeId = SCN.[contact_name]
where ssec.text like ''%The account is currently locked out%'' and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
and ssec.[Text] not like ''%secure_id_maint%'' and ssec.[Text] not like ''%SVC_ORX_Reporting%''
order by si.sql_instance_name

delete from ##SQL_LoginLock where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_LoginLock)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@loginname = [loginname], @AccountOwner = [AccountOwner], @AccountOwnerEmail = [AccountOwnerEmail], @SupervisorName = [SupervisorName]
---------------------
from ##SQL_LoginLock order by SQL_Instance_Name


---------------------
set @EmailAddress = @Customer_Email_group

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Account ''+@loginname+'' Locked out''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' Account ''+@loginname+'' Locked out

Account = ''+ cast(@loginname as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Account Owner = ''+@AccountOwner+''
Account Owner Email = ''+@AccountOwnerEmail+''
Supervisor = ''+@SupervisorName+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_LoginLock where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_LoginLock)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
	@query_result_no_padding = 1,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_LoginLock where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_LoginLock order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Resolving state]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Resolving state', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_failoverError'') is not null
    drop table ##SQL_failoverError

declare @AlertType varchar(50)
set @AlertType = ''Resolving''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @loginname varchar(200)
declare @AccountOwner varchar(200)
declare @AccountOwnerEmail varchar(200)
declare @SupervisorName varchar(200)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_failoverError
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
where ssec.text like ''%"RESOLVING"%'' and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
order by si.sql_instance_name

delete from ##SQL_failoverError where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_failoverError)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
---------------------
from ##SQL_failoverError order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Failover Resolving state found ''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +''

Account = ''+ cast(@loginname as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_failoverError where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_failoverError)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
	@query_result_no_padding = 1,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_failoverError where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_failoverError order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Stack Dump]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Stack Dump', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_stackdump'') is not null
    drop table ##SQL_stackdump

declare @AlertType varchar(50)
set @AlertType = ''Stackdump''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @loginname varchar(200)
declare @AccountOwner varchar(200)
declare @AccountOwnerEmail varchar(200)
declare @SupervisorName varchar(200)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_stackdump
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
where ssec.text like ''%stack dump%''
order by si.sql_instance_name

delete from ##SQL_stackdump where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_stackdump)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
---------------------
from ##SQL_stackdump order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Stack Dump found ''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +''

Account = ''+ cast(@loginname as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_stackdump where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_stackdump)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
	@query_result_no_padding = 1,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_stackdump where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_stackdump order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Failover stall]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Failover stall', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_failoverStallError'') is not null
    drop table ##SQL_failoverStallError

declare @AlertType varchar(50)
set @AlertType = ''Resolving''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @loginname varchar(200)
declare @AccountOwner varchar(200)
declare @AccountOwnerEmail varchar(200)
declare @SupervisorName varchar(200)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_failoverStallError
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
where ssec.text like ''%Failed to update Replica status due to exception%'' and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
order by si.sql_instance_name

delete from ##SQL_failoverStallError where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_failoverStallError)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
---------------------
from ##SQL_failoverStallError order by SQL_Instance_Name


---------------------
set @EmailAddress = @Pager

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Failover Stalled ''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +''

If no database can be accessed take cluster offline then back on to reset the status

Account = ''+ cast(@loginname as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_failoverStallError where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_failoverStallError)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
	@query_result_no_padding = 1,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_failoverStallError where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_failoverStallError order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Page Allocate]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Page Allocate', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..##SQL_SpaceError'') is not null
    drop table ##SQL_SpaceError

declare @AlertType varchar(50)
set @AlertType = ''SQL_SpaceError''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @count int

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ssec.[ProcessInfo],ssec.[LogDate],ssec.[Text]
---------------------
into ##SQL_SpaceError
	FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
	inner join [DBA_Watcher_DB].[General].[SQL_Server_ErrorLog_Current] ssec with (nolock) on cast(ssec.SQL_Instance_Name as varchar) = si.SQL_Instance_Name
	where ssec.text like ''%Could not allocate a new page%'' and ssec.text not like ''%TEMPDB%'' 
	order by si.sql_instance_name, ssec.LogDate desc

delete from ##SQL_SpaceError where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM ##SQL_SpaceError)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------

---------------------
from ##SQL_SpaceError order by SQL_Instance_Name

Set @count = (select distinct count(*) from ##SQL_SpaceError where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_SpaceError))
---------------------
set @EmailAddress = @Pager

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' ''+cast(@count as varchar)+'' Page allocate events''
set @bodytext = ''Could not allocate page events for ''+@dns_Name+'' Server ''+ cast(@role_desc as varchar) +''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
Customer  = '' +@customer_email+''
''+@Patch+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query = ''select distinct [SQL_Instance_Name],[LogDate],[ProcessInfo],[Text] from ##SQL_SpaceError where SQL_Instance_Name in (select top 1 SQL_Instance_Name from ##SQL_SpaceError)'',
    @query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @query_result_no_padding = 1,
    @attach_query_result_as_file= 0;

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from ##SQL_SpaceError where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from ##SQL_SpaceError)
END  

if object_id(''tempdb..##SQL_SpaceError'') is not null
    drop table ##SQL_SpaceError

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [VMware Latancy]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'VMware Latancy', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#VM_lat'') is not null
    drop table #VM_lat

declare @AlertType varchar(50)
set @AlertType = ''VM_Latency''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Latency float
declare @webpage varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,round(avg([value]),2) as [Latency]
---------------------
into #VM_lat
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si
join [DBA_Watcher_DB].[General].[VMWareVMMetrics_Current] vmm with (nolock) on si.SQL_Instance_Name = vmm.resourceName
where  si.Department like ''%prod%'' and vmm.metric like ''%Latency%'' and [value] > 50
and  DATEADD(HOUR, 1,cast([timestamp] as datetime))> getdate()
group by si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
order by Latency desc

delete from #VM_lat where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #VM_lat)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Latency = [Latency]
---------------------
from #VM_lat order by SQL_Instance_Name


---------------------
set @webpage = ''''

set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] VMware Avg Latency ''+ cast(@Latency as varchar) +'' last hour''
set @bodytext = ''VMware Latency high over last hour for ''+@dns_Name+'' Server ''+ cast(@role_desc as varchar) +'' 

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
Customer  = '' +@customer_email+''

Last hour Avg Latency
Latency = ''+cast(@Latency as varchar)+''

''+@webpage+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #VM_lat where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #VM_lat order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'runlog', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190416, 
		@active_end_date=99991231, 
		@active_start_time=400, 
		@active_end_time=235959, 
		@schedule_uid=N'3254c34c-3a50-42ce-9272-5b43ba82e7fb'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Instances Details Alerts]    Script Date: 6/6/2026 9:59:13 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 6/6/2026 9:59:13 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Instances Details Alerts', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Restart]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Restart', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Restart'') is not null
    drop table #SQL_Restart

declare @AlertType varchar(50)
set @AlertType = ''SQL_Restart''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,csid.LastRestartTime
---------------------
into #SQL_Restart
from [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] csid with (nolock) on csid.[SQL_Instance_Name] = si.[SQL_Instance_Name]
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Instances_Details] lsid with (nolock) on csid.[SQL_Instance_Name] = lsid.[SQL_Instance_Name]
where cast(csid.LastRestartTime as datetime) > cast(lsid.LastRestartTime as datetime)

delete from #SQL_Restart where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])


WHILE EXISTS(SELECT * FROM #SQL_Restart)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_Restart order by SQL_Instance_Name


---------------------
set @EmailAddress = @Customer_Email_group

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Restart found''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL Restart found 

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Restart where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Restart order by SQL_Instance_Name)
END', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Cluster Status]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Cluster Status', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_ServerCluster'') is not null
    drop table #SQL_ServerCluster

declare @AlertType varchar(50)
set @AlertType = ''SQL_ServerCluster''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_ServerCluster
from [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] sidc with (nolock)
inner Join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Instances_Details] sidl with (nolock) on sidc.[SQL_Instance_Name] = sidl.[SQL_Instance_Name] 
inner join [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) on sidl.[SQL_Instance_Name] = si.[SQL_Instance_Name]
and sidc.[AlwaysOn_Feature_Status] like ''Enabled'' and sidc.[AlwaysOn_Manager_Status] not like ''Started'' and sidl.[AlwaysOn_Feature_Status] like ''Enabled'' 
and sidl.[AlwaysOn_Manager_Status] not like ''Started''
and si.SQL_Instance_Name not in (select SQL_Instance_Name from [DBA_Watcher_DB].[Notifications].[Alert_History] with (nolock) where [Send_Time]>dateadd(minute, -20, getdate()) 
and [Method] = ''Email'' and Header like ''%SQL Cluster Service down%'')

delete from #SQL_ServerCluster where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_ServerCluster)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_ServerCluster order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Cluster Service down''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL cluster services is not up in most resent collection.  Please verify it is running.

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName  = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_ServerCluster where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_ServerCluster order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Agent Status]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Agent Status', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_ServerAgent'') is not null
    drop table #SQL_ServerAgent

declare @AlertType varchar(50)
set @AlertType = ''SQL_ServerAgent''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_ServerAgent
from [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] sidc with (nolock)
inner Join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Instances_Details] sidl with (nolock) on sidc.[SQL_Instance_Name] = sidl.[SQL_Instance_Name] 
inner join [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) on sidl.[SQL_Instance_Name] = si.[SQL_Instance_Name]
and sidc.[SQL_Agent_Status] not like ''Running'' and sidl.[SQL_Agent_Status] not like ''Running''
and si.SQL_Instance_Name not in (select SQL_Instance_Name from [DBA_Watcher_DB].[Notifications].[Alert_History] with (nolock) where [Send_Time]>dateadd(minute, -20, getdate()) 
and [Method] = ''Email'' and Header like ''%SQL Agent Missing%'')

delete from #SQL_ServerAgent where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_ServerAgent)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_ServerAgent order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Agent Missing''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL Agent session connection is missing from the most resent collection.  Please verify it is running.

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_ServerAgent where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_ServerAgent order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Down 3 hours]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Down 3 hours', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_ServerHoursDown'') is not null
    drop table #SQL_ServerHoursDown

declare @AlertType varchar(50)
set @AlertType = ''SQL_ServerHoursDown''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,si.LastRestartTime
---------------------
into #SQL_ServerHoursDown
from [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
where si.[SQL_Instance_Name] not in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] with (nolock))
and si.[SQL_Instance_Name] not in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] with (nolock) where Pull_Time > dateadd(minute, -180, GetDate()))
and si.SQL_Instance_Name not in (select SQL_Instance_Name from [DBA_Watcher_DB].[Notifications].[Alert_History] with (nolock) where [Send_Time]>dateadd(minute, -180, getdate()) 
and [Method] = ''Email'' and (Header like ''%SQL Server Down over 3 hours%'' or Header like ''%SQL Server Down%''))
and si.[SQL_Instance_Name] in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] with (nolock))

delete from #SQL_ServerHoursDown where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_ServerHoursDown)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_ServerHoursDown order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Server Down over 3 hours''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL Server Down over 3 hours

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_ServerHoursDown where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_ServerHoursDown order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Server Up]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Server Up', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_ServerUp'') is not null
    drop table #SQL_ServerUp

declare @AlertType varchar(50)
set @AlertType = ''SQL_ServerUp''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)


SELECT si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner],
si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email], si.[Pager], 
---------------------
si.LastRestartTime
---------------------
into #SQL_ServerUp
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] AS si WITH (NOLOCK)
OUTER APPLY (SELECT TOP (1) dc.Pull_Time
FROM [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] AS dc WITH (NOLOCK)
WHERE dc.SQL_Instance_Name = si.SQL_Instance_Name
ORDER BY dc.Pull_Time DESC ) AS lastStatus
OUTER APPLY (SELECT TOP (1) ah.Send_Time, ah.[Header], ah.[Method]
FROM [DBA_Watcher_DB].[Notifications].[Alert_History] AS ah WITH (NOLOCK)
WHERE ah.SQL_Instance_Name = si.SQL_Instance_Name and ah.[Method] = ''Email''
ORDER BY ah.Send_Time DESC
) AS lastAlert
WHERE (lastStatus.Pull_Time IS NOT NULL) and (lastAlert.[Header] LIKE ''%SQL Server Down%'')


delete from #SQL_ServerUp where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])


WHILE EXISTS(SELECT * FROM #SQL_ServerUp)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_ServerUp order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Server Up''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL Server Up

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_ServerUp where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_ServerUp order by SQL_Instance_Name)
END  

', 
		@database_name=N'model', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Server Down]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Server Down', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_ServerDown'') is not null
    drop table #SQL_ServerDown

declare @AlertType varchar(50)
set @AlertType = ''SQL_ServerDown''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @LastRestartTime varchar(200)

SELECT si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner],
si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email], si.[Pager], 
---------------------
si.LastRestartTime
---------------------
into #SQL_ServerDown
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] AS si WITH (NOLOCK)
OUTER APPLY (SELECT TOP (1) dc.Pull_Time
FROM [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] AS dc WITH (NOLOCK)
WHERE dc.SQL_Instance_Name = si.SQL_Instance_Name
ORDER BY dc.Pull_Time DESC ) AS lastStatus
OUTER APPLY (SELECT TOP (1) dc2.Pull_Time
FROM [DBA_Watcher_DB].[General].[SQL_Instances_Details] AS dc2 WITH (NOLOCK)
WHERE dc2.SQL_Instance_Name = si.SQL_Instance_Name
ORDER BY dc2.Pull_Time DESC ) AS lastStatus2
OUTER APPLY (SELECT TOP (1) ah.Send_Time, ah.[Header], ah.[Method]
FROM [DBA_Watcher_DB].[Notifications].[Alert_History] AS ah WITH (NOLOCK)
WHERE ah.SQL_Instance_Name = si.SQL_Instance_Name and ah.[Method] = ''Email''
ORDER BY ah.Send_Time DESC
) AS lastAlert
WHERE (lastStatus.Pull_Time IS NULL) and (lastStatus2.Pull_Time <= DATEADD(MINUTE, -10, GETDATE())) and (lastAlert.[Header] not LIKE ''%SQL Server Down%'')


delete from #SQL_ServerDown where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_ServerDown)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@LastRestartTime=cast(LastRestartTime as varchar)
---------------------
from #SQL_ServerDown order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Server Down''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' SQL Server Down 

Last Restart = ''+ cast(@LastRestartTime as varchar)+''
''+@Patch+''

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
    @query_result_header=1,
    @body = @bodytext ,
    @query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_ServerDown where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_ServerDown order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Change in Databases]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Change in Databases', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Database_Change'') is not null
    drop table #SQL_Database_Change

declare @AlertType varchar(50)
set @AlertType = ''SQL_Database_Change''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Databases_Current varchar(400)
declare @Databases_Last varchar(400)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,csid.[Databases] as [Databases_Current] ,lsid.[Databases] as [Databases_Last]
---------------------
into #SQL_Database_Change
from [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Instances_Details_Current] csid with (nolock) on csid.[SQL_Instance_Name] = si.[SQL_Instance_Name]
inner join [DBA_Watcher_DB_Tempdb].[Scratch].[SQL_Instances_Details] lsid with (nolock) on csid.[SQL_Instance_Name] = lsid.[SQL_Instance_Name]
where csid.[Databases] not like lsid.[Databases] and si.Department like ''%prod%''

delete from #SQL_Database_Change where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Database_Change)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Databases_Current = [Databases_Current], @Databases_Last = [Databases_Last]
---------------------
from #SQL_Database_Change order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Change in server databases''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' Change in server databases 

AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''
UserEmail = ''+@Customer_Email+''

Current Databases  = ''+@Databases_Current+''

Original Databases = ''+@Databases_Last+'''' 

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Database_Change where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Database_Change order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'RunTime', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190227, 
		@active_end_date=99991231, 
		@active_start_time=440, 
		@active_end_time=235959, 
		@schedule_uid=N'a1b4feb8-f6eb-4563-852e-3f8a69e60362'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Job Alert]    Script Date: 6/6/2026 9:59:13 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[UHT Infrastructure]]    Script Date: 6/6/2026 9:59:13 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[UHT Infrastructure]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[UHT Infrastructure]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Job Alert', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[UHT Infrastructure]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Job failed check]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Job failed check', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Job'') is not null
    drop table #SQL_Job

declare @AlertType varchar(50)
set @AlertType = ''SQL_Job''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @JobName varchar(200)
declare @step_name varchar(200)
declare @Message varchar(800)
declare @Last_Job_Duration int
declare @AVG_Step_Duration_Min int
declare @start_execution_date datetime
declare @stop_execution_date datetime
declare @next_scheduled_run_date datetime

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,[JobName],[step_name],[last_run_outcome_status],[Message],aj.[Status],[Last_Job_Duration],[AVG_Step_Duration_Min],[Max_Step_Duration_Min],[start_execution_date],[stop_execution_date]
,[next_scheduled_run_date]
---------------------
into #SQL_Job
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
join [DBA_Watcher_DB].[General].[SQL_AgentJobs] aj  with (nolock) on aj.[SQL_Instance_Name] = si.[SQL_Instance_Name]
where last_run_outcome_status like ''Failed''
and JobName not like ''DBA - SQL Trace'' and step_name not like ''Check If Primary'' and step_name not like ''Start push to TSM'' and step_name not like ''Primary Check'' and step_name not like ''Check if Node is Primary''
and (role_desc like '''' or role_desc like ''%Main%'') and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
and stop_execution_date>dateadd(minute, -10, Pull_Time) 

delete from #SQL_Job where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Job)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@JobName = [JobName], @step_name = [step_name],@Message=[Message],@Last_Job_Duration=[Last_Job_Duration],
@AVG_Step_Duration_Min=[AVG_Step_Duration_Min],@start_execution_date=[start_execution_date],@stop_execution_date=[stop_execution_date],@next_scheduled_run_date=[next_scheduled_run_date]
---------------------
from #SQL_Job order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''[''+ @DepartmentShort +''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Job ''+cast(@JobName as varchar)+'' failed''
set @bodytext = ''Job ''+cast(@JobName as varchar)+'' failed at step ''+cast(@step_name as varchar)+''

AppName        = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email      = ''+@Service_Level_Owner_Email+''
Customer Email = ''+@Customer_Email+''

Job       = ''+cast(@JobName as varchar)+''
Step	  = ''+cast(@step_name as varchar)+''
Job start = ''+cast(@start_execution_date as varchar)+''
Job stop  = ''+cast(@stop_execution_date as varchar)+''

Next run = ''+cast(@next_scheduled_run_date as varchar)+''
Mesasge  = ''+cast(@Message as varchar)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
    @body = @bodytext ;

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Job where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Job order by SQL_Instance_Name)
END  
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [CDC Replication jobs not running]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'CDC Replication jobs not running', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Job_CDC'') is not null
    drop table #SQL_Job_CDC

declare @AlertType varchar(50)
set @AlertType = ''SQL_Job_CDC''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @JobName varchar(200)
declare @step_name varchar(200)
declare @Message varchar(800)
declare @Last_Job_Duration int
declare @AVG_Step_Duration_Min int
declare @start_execution_date datetime
declare @stop_execution_date datetime
declare @next_scheduled_run_date datetime

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,[JobName],[step_name],[last_run_outcome_status],[Message],aj.[Status],[Last_Job_Duration],[AVG_Step_Duration_Min],[Max_Step_Duration_Min],[start_execution_date],[stop_execution_date]
,[next_scheduled_run_date]
---------------------
into #SQL_Job_CDC
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si  with (nolock)
join [DBA_Watcher_DB].[General].[SQL_AgentJobs] aj  with (nolock) on aj.[SQL_Instance_Name] = si.[SQL_Instance_Name]
where [stop_execution_date] != ''1900-01-01 00:00:00''
and (JobName like ''%cdc.Formulary_capture%'' or JobName like ''%InetPharmacy-1%'' or JobName like ''%inetformulary-2%'' 
or JobName like ''%cdc.yni0001_capture'' or JobName like ''%cdc.FMS_DATA_capture%'' or JobName like ''%cdc.doms_prd_capture%'' or JobName like ''%cdc.p2p_prd_capture%'' or JobName like ''%cdc.ORx_CPR_capture%'') 
and si.Department like ''%prod%'' and [role_desc] like ''%main%''
and Pull_Time>dateadd(minute, -10, GetDate())  

delete from #SQL_Job_CDC where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Job_CDC)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@JobName = [JobName], @step_name = [step_name],@Message=[Message],@Last_Job_Duration=[Last_Job_Duration],
@AVG_Step_Duration_Min=[AVG_Step_Duration_Min],@start_execution_date=[start_execution_date],@stop_execution_date=[stop_execution_date],@next_scheduled_run_date=[next_scheduled_run_date]
---------------------
from #SQL_Job_CDC order by SQL_Instance_Name

---------------------
set @EmailAddress = @Pager

Set @header = ''[''+ @DepartmentShort +''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' Job ''+cast(@JobName as varchar)+'' replication job not running''
set @bodytext = ''Job ''+cast(@JobName as varchar)+'' failed at step ''+cast(@step_name as varchar)+''

AppName        = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email      = ''+@Service_Level_Owner_Email+''
Customer Email = ''+@Customer_Email+''

Job       = ''+cast(@JobName as varchar)+''
Step	  = ''+cast(@step_name as varchar)+''
Job start = ''+cast(@start_execution_date as varchar)+''
Job stop  = ''+cast(@stop_execution_date as varchar)+''

Next run = ''+cast(@next_scheduled_run_date as varchar)+''
Mesasge  = ''+cast(@Message as varchar)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
    @body = @bodytext ;

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Pager'',@header,@bodytext,@EmailAddress)

delete from #SQL_Job_CDC where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Job_CDC order by SQL_Instance_Name)
END  

', 
		@database_name=N'model', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'jobfailedcheck', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20230105, 
		@active_end_date=99991231, 
		@active_start_time=800, 
		@active_end_time=235959, 
		@schedule_uid=N'3f1c06af-3764-4c48-a019-714eafd5e9e8'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Sessions Alerts]    Script Date: 6/6/2026 9:59:13 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 6/6/2026 9:59:13 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Sessions Alerts', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Blocking Over 5 mins]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Blocking Over 5 mins', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Session_Block'') is not null
    drop table #SQL_Session_Block

declare @AlertType varchar(50)
set @AlertType = ''SQL_Session_Block''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @SessionID int
declare @Database_Name varchar(120)
declare @cmd varchar(20)
declare @Text varchar(max)
declare @login_name varchar(50)
declare @host varchar(50)
declare @Count_blocked int
declare @Program varchar(400)
declare @Max_Wait_sec float
declare @Last_Wait_Type varchar(20)
declare @max_used_memory_kb int

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,ss.SessionID,ss.Database_Name, ss.cmd, ss.Text, ss.login_name, ss.Host, ss2.[Count_blocked],ss.[Program],ss2.Max_Wait_sec, cast(ss.[Last_Wait_Type] as varchar) as [Last_Wait_Type], 
ss.[max_used_memory_kb], ss.Blocking_Session
---------------------
into #SQL_Session_Block
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)
inner join [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] ss with (nolock) on si.SQL_Instance_Name = ss.SQL_Instance_Name
inner join (select SQL_Instance_Name, count(sessionid) as [Count_blocked], max (WaitTime_Sec) as [Max_Wait_sec] 
    from [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] with (nolock) where Last_Wait_Type like ''LCK_%'' and WaitTime > 300000  
    and Blocking_Session <>0 
	group by SQL_Instance_Name ) ss2 on  ss.SQL_Instance_Name = ss2.SQL_Instance_Name
where (ss.LeadBlocker = ''Lead'' or ss.SessionID in (SELECT distinct [Blocking_Session] FROM [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] ssh 
where ss.SQL_Instance_Name = ssh.SQL_Instance_Name and ssh.Last_Wait_Type like ''LCK_%'' and ssh.WaitTime > 300000  and ssh.Blocking_Session <>0))
-- and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
order by si.SQL_Instance_Name, ss.Blocking_Session,  ss.Text desc

delete from #SQL_Session_Block where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

Delete from #SQL_Session_Block where text like ''%I_OPT_WMS_CLS_NONSHIP_ORD_INTF%'' and [Count_blocked]<3
Delete from #SQL_Session_Block where text like ''%sp_batchinsert_%'' and [Count_blocked]<4
Delete from #SQL_Session_Block where text like ''%sp_replcmds%'' and [Count_blocked]<4
Delete from #SQL_Session_Block where text like ''%OracleGG_%'' and [Count_blocked]<4

WHILE EXISTS(SELECT * FROM #SQL_Session_Block)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Program =[Program], @Database_Name = [Database_Name] ,@cmd = [cmd], @Text  = [Text] ,@login_name = [login_name], @SessionID = [SessionID],
@Host = [Host] ,@Count_blocked = [Count_blocked],@Max_Wait_sec = [Max_Wait_sec], @Last_Wait_Type = [Last_Wait_Type], @max_used_memory_kb = [max_used_memory_kb]
---------------------
from #SQL_Session_Block order by SQL_Instance_Name, Blocking_Session 


---------------------
set @EmailAddress = @Customer_Email_group

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] Spid ''+ cast(@SessionID as varchar) +'' blocking ''+ cast(@Count_blocked as varchar) +'' for ''+ cast(round((@Max_Wait_sec/60),0) as varchar)+'' mins''
set @bodytext = ''''+ @Department +'' Server ''+ cast(@role_desc as varchar) +'' Spid ''+ cast(@SessionID as varchar) +'' blocking ''+ cast(@Count_blocked as varchar) +'' session for ''+ cast(round((@Max_Wait_sec/60),0) as varchar)+'' mins.
AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''

Program   ''+@Program+''
Host      ''+@host+''
LoginName ''+@login_name+''

If you need this lead blocking session killed just respond to this email requesting that

Database  ''+@Database_Name +''
Command   ''+@cmd+''
Last Wait ''+@Last_Wait_Type+''
Memory Grant KB ''+cast(@max_used_memory_kb as varchar)+''

Lead Blocker below

''+ SUBSTRING(@Text,1,1800)+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Session_Block where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Session_Block order by SQL_Instance_Name)
END  

', 
		@database_name=N'model', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Killed Session]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Killed Session', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Session_Killed'') is not null
    drop table #SQL_Session_Killed

declare @AlertType varchar(50)
set @AlertType = ''SQL_Session_Killed''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Sessions int
declare @cmd varchar(20)
declare @Text varchar(max)
declare @login_name varchar(50)
declare @host varchar(50)
declare @Last_Wait_Type varchar(20)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,count(ss.SessionID) as [Sessions], ss.cmd, ss.Last_Wait_Type, ss.Text, ss.login_name, ss.Host
---------------------
into #SQL_Session_Killed
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock) 
join [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] ss with (nolock) on si.SQL_Instance_Name = ss.SQL_Instance_Name
where cmd like ''%KILL%'' and si.Department like ''%Prod%'' and ss.login_name not like ''%SQLSERVERAGENT%''
group by si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager], ss.cmd, ss.Last_Wait_Type, ss.Text
,ss.login_name, ss.Host
order by si.SQL_Instance_Name

delete from #SQL_Session_Killed where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Session_Killed)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@cmd = [cmd], @Text  = [Text] ,@login_name = [login_name], @Sessions = [Sessions], @Host = [Host], @Last_Wait_Type = [Last_Wait_Type]
---------------------
from #SQL_Session_Killed order by SQL_Instance_Name


---------------------
set @EmailAddress = @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] ''+ cast(@Sessions as varchar) +'' Sessions in Killed/Rollback''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' ''+ cast(@Sessions as varchar) +'' Sessions in Killed/Rollback.
AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''

Command   ''+@cmd+''
LoginName ''+@login_name+'' 
Wait_Type ''+@Last_Wait_Type+'' 
Host      ''+@host+''

''+ SUBSTRING(@Text,1,1600)+''''


EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Session_Killed where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Session_Killed order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Resource Semaphore]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Resource Semaphore', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_resource_semaphore'') is not null
    drop table #SQL_resource_semaphore

declare @AlertType varchar(50)
set @AlertType = ''SQL_resource_semaphore''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Sessions int
declare @cmd varchar(20)
declare @Text varchar(max)
declare @login_name varchar(50)
declare @host varchar(50)
declare @Last_Wait_Type varchar(20)

select distinct si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,count(ss.SessionID) as [Sessions], ss.cmd, ss.Last_Wait_Type, ss.Text, ss.login_name, ss.Host, ss.[requested_memory_kb]
---------------------
into #SQL_RESOURCE_SEMAPHORE
FROM [DBA_Watcher_DB].[General].[SQL_Sessions_Details_Current] ss with (nolock)
inner join [DBA_Watcher_DB].[dbo].[Inventory_All] si with (nolock)  on ss.SQL_Instance_Name = si.SQL_Instance_Name
where rtrim(Last_Wait_Type)  like ''%RESOURCE_SEMAPHORE'' and (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
group by si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
,ss.cmd, ss.Last_Wait_Type, ss.Text, ss.login_name, ss.Host, ss.requested_memory_kb
order by si.SQL_Instance_Name, ss.[requested_memory_kb] desc

delete from #SQL_RESOURCE_SEMAPHORE where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_resource_semaphore)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@cmd = [cmd], @Text  = [Text] ,@login_name = [login_name], @Sessions = [Sessions], @Host = [Host], @Last_Wait_Type = [Last_Wait_Type]
---------------------
from #SQL_resource_semaphore order by SQL_Instance_Name


---------------------
set @EmailAddress =  @group_email

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +''] ''+ cast(@Sessions as varchar) +'' Sessions in Resource Semaphore''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' ''+ cast(@Sessions as varchar) +'' Sessions in Resource Semaphore.
AppName   = ''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO Email = ''+@Service_Level_Owner_Email+''

Command   ''+@cmd+''
LoginName ''+@login_name+'' 
Wait_Type ''+@Last_Wait_Type+'' 
Host      ''+@host+''

''+ SUBSTRING(@Text,1,1600)+''''


EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_resource_semaphore where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_resource_semaphore order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Run_Blocking_Weekday', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=5, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190409, 
		@active_end_date=99991231, 
		@active_start_time=440, 
		@active_end_time=235959, 
		@schedule_uid=N'fefe07de-a034-453a-88d8-5496954cc8d0'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

/****** Object:  Job [[Alert] SQL Stats]    Script Date: 6/6/2026 9:59:13 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 6/6/2026 9:59:13 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'[Alert] SQL Stats', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL CPU]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL CPU', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_CPU'') is not null
    drop table #SQL_CPU

declare @AlertType varchar(50)
set @AlertType = ''SQL_CPU''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @SQLProcessUtilization int
declare @SystemIdle int
declare @OtherProcessUtilization int
declare @SQLProcessUtilization_Last int
declare @SystemIdle_Last int
declare @OtherProcessUtilization_Last int
declare @Webpage varchar(200)


select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,avg(sscc.SQLProcessUtilization / isnull(si.numa_node_count, 1)) as [SQLProcessUtilization]
,avg(sscc.[SystemIdle] / isnull(si.numa_node_count, 1)) as [SystemIdle],avg(sscc.[OtherProcessUtilization]/ isnull(si.numa_node_count, 1)) as [OtherProcessUtilization]
,avg(sscl.[SQLProcessUtilization]/ isnull(si.numa_node_count, 1)) as [SQLProcessUtilization_Last] ,avg(sscl.[SystemIdle]/ isnull(si.numa_node_count, 1)) as [SystemIdle_Last]
,avg(sscl.[OtherProcessUtilization]/ isnull(si.numa_node_count, 1)) as [OtherProcessUtilization_Last]
---------------------
into #SQL_CPU
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si
inner join [DBA_Watcher_DB].[General].[SQL_Statistics_CPU] sscc on sscc.SQL_Instance_Name = si.SQL_Instance_Name and sscc.Pull_Time>dateadd(minute, -10, GetDate())
inner join [DBA_Watcher_DB].[General].[SQL_Statistics_CPU] sscl on sscl.SQL_Instance_Name = si.SQL_Instance_Name and sscl.Pull_Time>dateadd(minute, -60, GetDate())
where (si.Department like ''%Prod%'' or si.Customer_Email not like '''')
group by si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
having ((avg(sscc.SystemIdle/isnull(si.numa_node_count, 1)) <= 3 and avg(sscc.SystemIdle/isnull(si.numa_node_count, 1)) >= 0) or avg(sscc.SQLProcessUtilization/isnull(si.numa_node_count, 1)) >= 85)

delete from #SQL_CPU where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_CPU)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@SQLProcessUtilization = [SQLProcessUtilization], @SystemIdle = [SystemIdle], @OtherProcessUtilization = [OtherProcessUtilization],
@SQLProcessUtilization_Last = [SQLProcessUtilization_Last], @SystemIdle_Last = [SystemIdle_Last], @OtherProcessUtilization_Last = [OtherProcessUtilization_Last]
---------------------
from #SQL_CPU order by SQL_Instance_Name

---------------------
set @EmailAddress = @Customer_Email_group

set @webpage = ''''

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL CPU ''+ cast(@SQLProcessUtilization as varchar) +'' avg for last 10 mins''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' 

AppName =''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO     =''+@Service_Level_Owner+'' ''+@Service_Level_Owner_Email+''

10 min avg 
SQL  CPU =  ''+cast(@SQLProcessUtilization as varchar)+'' 
Idle CPU =  ''+cast(@SystemIdle as varchar)+'' 
Other CPU = ''+cast(@OtherProcessUtilization as varchar)+'' 

60 min Avg 
SQL  CPU =  ''+cast(@SQLProcessUtilization_Last as varchar)+'' 
Idle CPU =  ''+cast(@SystemIdle_Last as varchar)+'' 
Other CPU = ''+cast(@OtherProcessUtilization_Last as varchar)+''

''+@webpage+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_CPU where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_CPU order by SQL_Instance_Name)
END  
', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [SQL Page Life]    Script Date: 6/6/2026 9:59:13 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'SQL Page Life', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'if object_id(''tempdb..#SQL_Page_Life'') is not null
    drop table #SQL_Page_Life

declare @AlertType varchar(50)
set @AlertType = ''SQL_Page_Life''

declare @EmailAddress varchar(600)
declare @reply_to varchar(200)
declare @header varchar(400)
declare @bodytext varchar(2400) = ''Server''
declare @tab char(2) = CHAR(9) + CHAR(9)

declare @SQL_Instance_Name varchar(200)
declare @team varchar(20)
declare @role_desc varchar(200)
declare @dns_Name varchar(200)
declare @Login_Domain varchar(20)
declare @Department varchar(50)
declare @DepartmentShort varchar(50)
declare @PriorityLevel varchar(10)
declare @Application_Name varchar(400)
declare @Service_Level_Owner varchar(200)
declare @Service_Level_Owner_Email varchar(200)
declare @Patchtime varchar (10)
declare @Patch varchar (100)
declare @Customer_Email varchar(600)
declare @Customer_Email_group varchar(600)
declare @group_email varchar(600)
declare @Pager varchar(600)

declare @Page_Life int
declare @Page_Life_Last int
declare @Webpage varchar(200)


select si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
---------------------
,avg(sscc.[Page_Life]) as [Page_Life],avg(sscl.[Page_Life]) as [Page_Life_Last] 
---------------------
into #SQL_Page_Life
FROM [DBA_Watcher_DB].[dbo].[Inventory_All] si
inner join [DBA_Watcher_DB].[General].[SQL_Statistics_Usage] sscc on sscc.SQL_Instance_Name = si.SQL_Instance_Name and sscc.Pull_Time>dateadd(minute, -10, GetDate())
inner join [DBA_Watcher_DB].[General].[SQL_Statistics_Usage] sscl on sscl.SQL_Instance_Name = si.SQL_Instance_Name and sscl.Pull_Time>dateadd(minute, -60, GetDate())
where si.Department like ''%Prod%''
group by si.SQL_Instance_Name, si.[Team], si.[role_desc], si.dns_Name, si.Login_Domain, si.Department, si.[DepartmentShort], si.PriorityLevel, si.Application_Name, si.[Service_Level_Owner]
,si.Service_Level_Owner_Email, si.[reply_to], si.[Patch], si.[Patchtime], si.Customer_Email, si.[Customer_Email_group], si.[group_email] ,si.[Pager]
having avg(sscc.[Page_Life]) < 20

delete from #SQL_Page_Life where SQL_Instance_Name in (select distinct SQL_Instance_Name from [DBA_Watcher_DB].[Settings].[Alert_Override] with (nolock) where 
[Alert]=@AlertType and getdate() between [Start_Time] and [End_Time])

WHILE EXISTS(SELECT * FROM #SQL_Page_Life)
BEGIN  
select top 1 @SQL_Instance_Name = [SQL_Instance_Name], @team =[Team], @role_desc = [role_desc], @dns_Name = [dns_Name], @Login_Domain = [Login_Domain], @Department = [Department], 
@DepartmentShort = [DepartmentShort], @PriorityLevel = [PriorityLevel], @Application_Name =[Application_Name], @Service_Level_Owner=[Service_Level_Owner], 
@Service_Level_Owner_Email = [Service_Level_Owner_Email], @reply_to = [reply_to], @Patch = [Patch],@Patchtime = [Patchtime], @Customer_Email = [Customer_Email], 
@Customer_Email_group = [Customer_Email_group], @group_email = [group_email], @Pager = [Pager]
---------------------
,@Page_Life = [Page_Life] ,@Page_Life_Last = [Page_Life_Last]
---------------------
from #SQL_Page_Life order by SQL_Instance_Name

---------------------
set @EmailAddress = @group_email

set @webpage = ''''

Set @header = ''['' + @DepartmentShort + ''][''+ cast(@role_desc as varchar) +'']''+@Patchtime+'' SQL Page Life ''+ cast(@Page_Life as varchar) +'' avg for 10 mins''
set @bodytext = ''''+ @Department +'' Server ''+ @SQL_Instance_Name +'' 

AppName =''+@PriorityLevel+'' ''+@Application_Name+'' 
SLO     =''+@Service_Level_Owner_Email+''

10 min avg 
SQL Page Life = ''+cast(@Page_Life as varchar)+'' 

60 min collection 
SQL Page Life = ''+cast(@Page_Life_Last as varchar)+'' 

''+@webpage+''''

EXEC msdb.dbo.sp_send_dbmail
    @recipients = @EmailAddress,
    @reply_to = @reply_to,
    @subject = @header,
    @body_format=''TEXT'',
	@query_result_header=1,
	@body = @bodytext ,
	@query_result_separator= @tab,
    @attach_query_result_as_file= 0

Insert into [DBA_Watcher_DB].[Notifications].[Alert_History] ([Team],[SQL_Instance_Name],[Department],[Alert],[Send_Time],[Method],[Header],[Body],[Target_Email]) VALUES 
(@team, @SQL_Instance_Name, @Department, @AlertType, cast(getdate() as smalldatetime),''Email'',@header,@bodytext,@EmailAddress)

delete from #SQL_Page_Life where [SQL_Instance_Name] in (select top 1 [SQL_Instance_Name] from #SQL_Page_Life order by SQL_Instance_Name)
END  

', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'runcpu', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=4, 
		@freq_subday_interval=10, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20190412, 
		@active_end_date=99991231, 
		@active_start_time=700, 
		@active_end_time=235959, 
		@schedule_uid=N'654e146b-9db7-45a6-a682-543ff785284b'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO


