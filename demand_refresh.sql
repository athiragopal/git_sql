USE [msdb]
GO

/****** Object:  Job [3_buyer_demand_refresh]    Script Date: 11/1/2018 3:05:57 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 11/1/2018 3:05:57 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'3_buyer_demand_refresh', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'etl_user_prod', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [website_offline]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'website_offline', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'  update  [Wide_Orbit].[dbo].[web_server_status]
  set [prod_status] =0', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [forecast_load]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'forecast_load', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'delete from [dbo].[demand_day_fcst] where as_of_date =  cast(getdate() as date);

INSERT INTO [dbo].[demand_day_fcst]			
SELECT  			
      [Inventory_code]			
      ,[air_week]			
      ,[ordered_week]			
      ,[ACTUAL]			
      ,[PREDICT]			
      ,[LOWER]			
      ,[UPPER]			
      ,[air_year]			
	  ,[station]		
      ,[day_part]			
      ,[air_date]			
      ,[order_week_number] AS ORDER_WEEK			
      ,[week_difference] AS WEEKS_TO_AIR			
	 , cast(getdate() as date) as as_of_date
 ,market
	 ,revenue_forecast
	 ,inventory_pacing
	 ,revenue_pacing
	 ,sellout_forecast	
,tangent_line	
		
      FROM [Wide_Orbit].[dbo].[all_stations_demand_predict_new]			
	  where 1=1
	and forecast_date = cast(getdate() as date)
	----  and forecast_date =''09/09/2018''
	  and [Inventory_code] not like ''%*%'';', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [wo_nielsen_batch1]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'wo_nielsen_batch1', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use Wide_Orbit;			
Go		
	
/**Step 2: Get historical cumulative spot and revenue*/			
BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;		
		
declare @cutoffweek datetime			
declare @currweek int			
set @cutoffweek  = (select max(programweek) from PROD_Ratings_Integrated where programyear =2018 and RatingType =''Actual''	)
set @currweek  = datepart(ISOWK, 	@cutoffweek)	


 select a.*,  			
 case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 				
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year			
,cast(program_start_time as time) pgm_start_time			
,cast(program_end_time as time) pgm_end_time			
,Year(full_date) as wo_pgm_year			
,MOnth(full_date) as wo_pgm_month			
, case when a.air_year= Order_Year then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as week_to_air			
,sum([spot_counts]) over (partition by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as cum_spots		
	, sum([gross_revenue]) over (partition by [market]		
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as accum_revenue		
	  ,0 as bd_forecast		
	  ,0 as bd_upper_limit		
	  ,0 as bd_lower_limit		
 into #tempwodata			
		  from [dbo].[buyer_demand_fcst_input_new2]  a	
		 where  program_start_time is not null
 and invcode_name not like ''%*%''  
and  gross_revenue >0 and spot_counts > 0	
and Station_name in  (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
 and air_year >= year(getdate());




/*Step 3: get weekly historicals for buyer demand from using wide orbit data retrieved in Step 2*/			

BEGIN TRY DROP TABLE #temp_cap END TRY BEGIN CATCH END CATCH		;

select market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time,sum([Potential Units]) as cap
into #temp_cap
from 
( Select distinct market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week,[Air Date],program_Start_time,program_end_time,[Potential Units],break_code
FROM fact_inv_capacity_dv 
where 1=1 
and Station  in (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
and air_year >=2018
and break_code in (''CM'')) as aa
group by market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time
order by air_week;


BEGIN TRY DROP TABLE #temp_cap_wo END TRY BEGIN CATCH END CATCH		;

select a.market, a.[Station Affiliation] as affiliation, a.Station as station_name,  a.Daypart as daypart_name, a.[Inventory Code] as invcode_name , null as invcode_External_Id,
program_Start_time as pgm_start_time, program_end_time as pgm_end_time, a.Air_year as wo_pgm_year, 1 as wo_pgm_month,a.Air_year ,a.Air_week , 2017 as order_year , 42 as order_week,
case when a.air_year= 2017 then (a.air_week - 42 ) else ( case when a.air_year <> 2016 then 52 - 42 + a.air_week else 53 - 42 +a.air_week end ) end as week_to_air	,
case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 				
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year	
 ,cap
 into #temp_cap_wo	
 from #temp_cap a
 order by Air_week;


BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

select b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year , month(b.air_week_date) as wo_pgm_month , b.air_year, b.air_week ,  
coalesce(a.order_year , b.order_year) as order_year
, coalesce(a.Order_week ,b.order_week) as order_week, 			
coalesce(a.week_to_air ,b.week_to_air) as week_to_air,  b.air_week_date, b.disp_air_year, coalesce(max(a.accum_revenue),0) as cum_revenue, coalesce(max(a.cum_spots),0) as cum_spots, coalesce(sum(a.gross_revenue),0) as gross_revenue, 
coalesce(sum(a.[spot_counts]) ,0) as booked_spots			
,avg(b.cap) as weekly_capacity , 0 as forecasted_spots ,  0 as revenue_forecast
	 ,0 as inventory_pacing
	 ,0 as revenue_pacing
	 ,0 as sellout_forecast	
	 ,0 as tangent_line
into #temp_wo			
from #temp_cap_wo b left outer join #tempwodata a 	on (b.station_name =a.station_name and b.daypart_name =a.daypart_name and b.invcode_name =a.invcode_Name and b.Air_year =a.air_year and b.air_week =a.air_week)		
group by b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year ,  b.air_year, b.disp_air_year, b.air_week  ,  a.order_year, a.Order_week, 
	 b.order_year, b.Order_week, 			
a.week_to_air, b.week_to_air,  b.air_week_date			
order by a.order_year, a.order_week	;




/* Step 4: Get weekly buyer demand forecast data and append WO program details to it*/			
BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;			
			
select a.*, b.air_year, b.air_week, order_week, YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week)) as Order_year
,round(b.predict,0) as forecast_spots,   revenue_forecast
	 ,inventory_pacing
	 ,revenue_pacing
	 ,sellout_forecast	, tangent_line,
case when air_year=YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week)) then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as weeks_to_air, b.air_date 			
into #temp_wo_data_fcst			
 from [dbo].[demand_day_fcst] b,			
(SELECT  [market]			
      ,[Station]	as station_name		
      ,[daypart]		as daypart_name	
      ,[Inventory Code]		as invcode_name	
      ,cast (NULL as varchar(50))		as invcode_external_id	
      ,cast([program_start_time] as time)  [pgm_start_time]			
      ,cast([program_end_time] as time) [pgm_end_time]			
      ,avg(CAP) as cap			
      ,[Station Affiliation]	as Affiliation
	  ,air_week as airweek
	  ,air_year as airyear		
  FROM #temp_cap
  where  program_start_time is not null	
and  [Inventory Code] is not null
 and Station in (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
 and  air_year >= year(getdate())
  group by [market]			
		,[Station Affiliation]	
      ,Station			
      ,[daypart]			
      ,[Inventory Code]			
        ,air_year
	  ,air_week			
	  ,cast([program_start_time] as time) 		
      ,cast([program_end_time] as time)			
	 ) a		
	 where a.station_name = b.station		
	 and a.daypart_name = b.daypart		
	 and a.invcode_name = b.invcode_name
	 and a.airyear =b.air_year
	 and a.airweek = b.air_week	
			
 	and b.as_of_date = cast(getdate() as date);	
--     and b.as_of_date = ''01/01/2018'';
	

BEGIN TRY DROP TABLE #temp_ext_id END TRY BEGIN CATCH END CATCH;		

select distinct Station_Name, daypart_name, invcode_name , invcode_External_Id
into #temp_ext_id
from buyer_demand_fcst_input_new2
where 1=1
and  Station_Name in  (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
and air_year >=2018;


update #temp_wo_data_fcst
set invcode_external_id =b.invcode_External_Id
from #temp_wo_data_fcst a, #temp_ext_id b
where a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


update #temp_wo
set invcode_external_id =b.invcode_External_Id
from #temp_wo a, #temp_ext_id b
where a.invcode_External_Id is NULL
and a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


/* Step 5: Add forecast weekly into historical weekly table*/	
		
insert into #temp_wo			
select a.market, a.affiliation,a.Station_Name, a.daypart_name, a.invcode_name,   a.invcode_External_Id,  [pgm_start_time] ,			
 [pgm_end_time], air_year as wo_pgm_year, month(air_date) as wo_pgm_month, a.air_year, a.air_week, a.Order_year, a.order_week, 			
weeks_to_air as week_to_air,			
air_date air_week_date, 			
air_year as disp_air_year,			
 0 as cum_revenue, 0 as cum_spots, 0 as gross_revenue, 0 as booked_spots, 			
a.cap, case when a.forecast_spots is NULl then 0 else forecast_spots end as forecast_spots
 ,case when revenue_forecast is NULL then 0 else revenue_forecast end as revenue_forecast
	 ,case when inventory_pacing is NULL then 0 else inventory_pacing end as inventory_pacing
	 ,case when revenue_pacing is NULL then 0 else revenue_pacing end as revenue_pacing
	 ,case when sellout_forecast	is NULL then 0 else sellout_forecast end as sellout_forecast
	 ,case when tangent_line	is NULL then 0 else tangent_line end as tangent_line
from #temp_wo_data_fcst a		;	



		
/*Step 6: Get display air year and week to air date*/			
			
BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;			
			
		
declare @currYear int			
declare @prioryear int			
declare @priorweek int			
declare @currdate date			
			
set @curryear  = 2017			
set @prioryear = 2016		
			
select a.*, ROW_NUMBER()  over (partition by a.market , a.Station_Name , a.daypart_name , a.invcode_name , a.air_year, a.air_week order by booked_spots desc) row_id			
into #tempwodata_r			
from			
 (select a.*,		
case when (week_to_air < a.air_week ) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@curryear AS VARCHAR)) + (a.order_week-1), 7) else			
DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@prioryear AS VARCHAR)) + (a.order_week-1), 7)  end as week_to_air_date			
from #temp_wo a) a 	;		
			
/*Step 7: Get peak sales */				
			
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH	;		
			
select a.* , case when (peak_sales_flag = ''Y'' and a.air_week > b.curr_week) then ((air_week - week_to_air) - curr_week) else 0 end as weeks_to_peak			
into #temp_wo_all			
 from 			
 (select  datepart(Iso_WEEK ,getdate()) curr_week) b ,			
(select b.*, case when b.row_id = 1 then ''Y'' else ''N'' end as peak_sales_flag			
from #tempwodata_r b ) a	;		



BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;	

BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;	

BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;	
			

			
/*Step 8: Get nielsen program details*/	
			
BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	
			
		
select a.* , b.programname, b.Starttime as start_time, b.endtime as end_time, b.DayPart as nielsen_DayPart, b.station as nielsen_station 
into #temp_wo_nielsen_match			
 from #temp_wo_all a left outer join wo_nielsen_pgm_lookup_3 b
on (a.market = b.wo_market  and a.daypart_name =b.daypart_name and a.invcode_name  = b.invcode_name )
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	

select a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time ,
a.wo_pgm_year, a.wo_pgm_month, a.air_year, a.air_week, a.order_year, a.Order_week, a.week_to_air, a.air_week_date, a.disp_air_year, a.cum_revenue, a.cum_spots
, a.gross_revenue, a.booked_spots, a.weekly_capacity, a.forecasted_spots,a.revenue_forecast, a.inventory_pacing, a.revenue_pacing, a.sellout_forecast, a.tangent_line
,a.week_to_air_date, a.row_id, a.peak_sales_flag, a.weeks_to_peak, b.programname, b.start_time, b.end_time, b.DayPart, b.station 		
into #temp_wo_nielsen_match_2		
 from #temp_wo_nielsen_match a left outer join wo_nielsen_pgm b
on (a.market = b.market  and a.daypart_name =b.daypart_name and a.air_week = b.nielsen_air_week 
and a.invcode_name  = b.invcode_name and a.invcode_External_Id = b.invcode_external_id  )
where a.programname is null
and b.air_year >=2016;
	

insert into  #temp_wo_nielsen_match
select * from  #temp_wo_nielsen_match_2	;



BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH		;

/*Step 9: Integrate Ratings Forecast*/			
			
--BEGIN TRY DROP TABLE wo_nielsen  END TRY BEGIN CATCH END CATCH		;		
truncate table    wo_nielsen;

insert into   wo_nielsen		
select a.* , b.Affiliation, a.air_week, b.playbacktype, b.sampletype,			
b.Ratingtype, b.demo , b.Rating, b.Share   , b.hutput
			
	    from #temp_wo_nielsen_match a left outer join  [dbo].[PROD_Ratings_Integrated]  b		
  on ( a.nielsen_station = b.Station	and a.nielsen_daypart = b.daypart			
  and a.air_year = YEAR(DATEADD(day, 26 - DATEPART(isoww, programweek), programweek))			
   and a.air_week = datepart(ISO_WEEK,b.programweek)			
 and (a.programname = b.programname  ))		
     where  b.Affiliation in (''ABC'', ''CBS'', ''NBC'', ''FOX'')	;
 

  

BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	

/* Step 10: Filter out valid records with forecasted data */

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;		

 select a.market as wo_market, a.affiliation as wo_affiliation, a.Station_name as wo_station_name, a.daypart_name as wo_daypart_name, a.invcode_name as wo_invcode_name, a.invcode_external_id			
  ,a.pgm_start_time as wo_pgm_start_time , a.pgm_end_time as wo_pgm_end_time , a.wo_pgm_year, month(a.air_week_date) wo_pgm_month , a.air_year, a.air_week, a.order_year, a.Order_week,			
   cast(a.air_week_date as date) air_week_date, max(cum_revenue) as accum_revenue, max(cum_spots) as cum_spots, sum(gross_revenue) gross_revenue, sum(booked_spots) booked_spots, avg(weekly_capacity) as capacity			
  ,max(a.forecasted_spots) as bookings_fcst, NULL as nielsen_programname , NULL as nielsen_starttime, NULL as nielsen_end_time, daypart as nielsen_Daypart			
  ,a.Station as nielsen_Station, a.nielsen_aff as nielsen_affiliation, nielsen_air_week, playbacktype, sampletype, ratingtype, demo, max(rating) as Avg_Rating, max(Share) as avg_Share			
  ,max(a.hutput) as avg_Hutput, a.week_to_air, cast(a.week_to_air_date as date) week_to_air_date, a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year ,max(revenue_forecast) as revenue_forecast 
  ,max( inventory_pacing) as inventory_pacing, max(revenue_pacing) as revenue_pacing, max(sellout_forecast) as sellout_forecast,max(tangent_line) as tangent_line
 into #temp_wo_nielsen_int 
 from   wo_nielsen a ,(select market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week, Count(*) as cnt
         from  wo_nielsen
         where  affiliation = nielsen_aff
		 and ((air_year = Year(Getdate()) and air_week  >= Datepart(ISO_WEEK, Getdate()) - 11) or (air_year = Year(Getdate())+1 and air_week  < Datepart(ISO_WEEK, Getdate()) -11 ))
		 and rating is not null
   group by  market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week) b
   where a.market = b.market
   and a.station_name = b.station_name
   and a.daypart_name = b.daypart_name
   and a.affiliation = b.affiliation
   and a.invcode_name = b.invcode_name
  -- and a.invcode_external_id = b.invcode_external_id
   and a.air_week = b.air_week
    group by a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time , a.wo_pgm_year, month(a.air_week_date),
 	a.air_year, a.air_week, a.order_year, a.Order_week,	cast(a.air_week_date as date) 	,  a.DayPart , a.station, a.nielsen_aff, a.nielsen_air_week, a.Playbacktype, a.Sampletype,
	a.RatingType, a.demo, a.week_to_air , cast(a.week_to_air_date as date) , a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year 	
 order by Order_week;


/* Step 11: Delete old Actual data */

-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in  (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
and Ratingtype =''Actual'';




/* Step 12: Insert actual (Ratings) data */
insert into wo_nielsen_integration 			
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
     from #temp_wo_nielsen_int a
where  air_year >= year(getdate())  and Ratingtype =''Actual'';


BEGIN TRY DROP TABLE #temp_wo_nielsen_act  END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_nielsen  END TRY BEGIN CATCH END CATCH		;



/* Step 13: Filter out Ratingtype = forecasted */

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst from #temp_wo_nielsen_int where  air_year >= year(getdate()) and RatingTYpe =''Forecasted'';

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;


/* Step 14: Filter out TEGNA aff*/

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst_a from #temp_wo_nielsen_fcst where wo_affiliation = nielsen_affiliation;


/* Step 15: Insert non tegna aff with HUT*/
 insert into #temp_wo_nielsen_fcst_a 
 select  b.wo_market, b.wo_affiliation, b.wo_Station_Name, b.wo_daypart_name, b.wo_invcode_name, b.invcode_External_Id, b.wo_pgm_start_time, b.wo_pgm_end_time, b.wo_pgm_year, b.wo_pgm_month, b.air_year
 , b.air_week, b.order_year, b.Order_week,  b.air_week_date,  b.accum_revenue, b.cum_spots, b.gross_revenue, b.booked_spots, b.capacity, b.bookings_fcst
  , b.nielsen_programname, b.nielsen_starttime, b.nielsen_end_time, b.nielsen_Daypart, b.nielsen_Station, b.nielsen_affiliation, b.nielsen_air_week, b.Playbacktype, b.Sampletype
 ,b.RatingType, b.Demo, b.avg_Rating, case when a.avg_hutput > 0 then ((b.avg_Rating/a.avg_hutput)*100) else 0 end as share, a.avg_hutput
, b.week_to_air,b.week_to_air_date,   b.weeks_to_peak , b.peak_sales_flag, b.disp_air_year, b.revenue_forecast, b.inventory_pacing, b.revenue_pacing, b.sellout_forecast,b.tangent_line
 from
( select *   from #temp_wo_nielsen_fcst
where  wo_affiliation = nielsen_affiliation ) a,
 (select *  from #temp_wo_nielsen_fcst
where wo_affiliation <> nielsen_affiliation) b
where a.wo_market = b.wo_market
and a.air_year = b.air_year
and a.air_week = b.air_week
and a.wo_invcode_name = b.wo_invcode_name
and a.wo_affiliation = b.wo_affiliation
and a.order_year = b.order_year
and a.Order_week = b.Order_week
and a.air_week_date = b.air_week_date
and a.week_to_air_date = b.week_to_air_date;


/* Step 16 a: Delete old Forecasted data */
-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in (''WXIA'',
''WCNC'',
''WKYC'',
''KGW'',
''KVUE''
,''WFAA'')
and Ratingtype =''Forecasted'' ;

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	

/* Step 16: Insert forecasted (Ratings) data */

insert into wo_nielsen_integration 	
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
from #temp_wo_nielsen_fcst_a a
where  air_year >= year(getdate()) and Ratingtype =''Forecasted''
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;

', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [wo_nielsen_batch2]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'wo_nielsen_batch2', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use Wide_Orbit;			
Go		
	
/**Step 2: Get historical cumulative spot and revenue*/			
BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;		
		
declare @cutoffweek datetime			
declare @currweek int			
set @cutoffweek  = (select max(programweek) from PROD_Ratings_Integrated where programyear =2018 and RatingType =''Actual''	)
set @currweek  = datepart(ISOWK, 	@cutoffweek)	


 select a.*,  			
 case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 			
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year			
,cast(program_start_time as time) pgm_start_time			
,cast(program_end_time as time) pgm_end_time			
,Year(full_date) as wo_pgm_year			
,MOnth(full_date) as wo_pgm_month			
, case when a.air_year= Order_Year then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as week_to_air			
,sum([spot_counts]) over (partition by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as cum_spots		
	, sum([gross_revenue]) over (partition by [market]		
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as accum_revenue		
	  ,0 as bd_forecast		
	  ,0 as bd_upper_limit		
	  ,0 as bd_lower_limit		
 into #tempwodata			
		  from [dbo].[buyer_demand_fcst_input_new2]  a	
		 where  program_start_time is not null
 and invcode_name not like ''%*%''  
and  gross_revenue >0	and spot_counts > 0
and Station_name in   (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
and  air_year >= year(getdate());






/*Step 3: get weekly historicals for buyer demand from using wide orbit data retrieved in Step 2*/			

BEGIN TRY DROP TABLE #temp_cap END TRY BEGIN CATCH END CATCH		;

select market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time,sum([Potential Units]) as cap
into #temp_cap
from 
( Select distinct market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week,[Air Date],program_Start_time,program_end_time,[Potential Units],break_code
FROM fact_inv_capacity_dv 
where 1=1 
and Station  in (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
and air_year >=2018
and break_code in (''CM'')) as aa
group by market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time
order by air_week;


BEGIN TRY DROP TABLE #temp_cap_wo END TRY BEGIN CATCH END CATCH		;

select a.market, a.[Station Affiliation] as affiliation, a.Station as station_name,  a.Daypart as daypart_name, a.[Inventory Code] as invcode_name , null as invcode_External_Id,
program_Start_time as pgm_start_time, program_end_time as pgm_end_time, a.Air_year as wo_pgm_year, 1 as wo_pgm_month,a.Air_year ,a.Air_week , 2017 as order_year , 42 as order_week,
case when a.air_year= 2017 then (a.air_week - 42 ) else ( case when a.air_year <> 2016 then 52 - 42 + a.air_week else 53 - 42 +a.air_week end ) end as week_to_air	,
case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 					
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year	
 ,cap
 into #temp_cap_wo	
 from #temp_cap a
 order by Air_week;


BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

select b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year , month(b.air_week_date) as wo_pgm_month , b.air_year, b.air_week ,  
coalesce(a.order_year , b.order_year) as order_year
, coalesce(a.Order_week ,b.order_week) as order_week, 			
coalesce(a.week_to_air ,b.week_to_air) as week_to_air,  b.air_week_date, b.disp_air_year, coalesce(max(a.accum_revenue),0) as cum_revenue, coalesce(max(a.cum_spots),0) as cum_spots, coalesce(sum(a.gross_revenue),0) as gross_revenue, 
coalesce(sum(a.[spot_counts]) ,0) as booked_spots			
,avg(b.cap) as weekly_capacity , 0 as forecasted_spots ,  0 as revenue_forecast
	 ,0 as inventory_pacing
	 ,0 as revenue_pacing
	 ,0 as sellout_forecast	
	 ,0 as tangent_line
into #temp_wo			
from #temp_cap_wo b left outer join #tempwodata a 	on (b.station_name =a.station_name and b.daypart_name =a.daypart_name and b.invcode_name =a.invcode_Name and b.Air_year =a.air_year and b.air_week =a.air_week)		
group by b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year ,  b.air_year, b.disp_air_year, b.air_week  ,  a.order_year, a.Order_week, 
	 b.order_year, b.Order_week, 			
a.week_to_air, b.week_to_air,  b.air_week_date			
order by a.order_year, a.order_week	;



/* Step 4: Get weekly buyer demand forecast data and append WO program details to it*/			
BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;			
			
select a.*, b.air_year, b.air_week, order_week, YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week)) as Order_year
,round(b.predict,0) as forecast_spots,   revenue_forecast
	 ,inventory_pacing
	 ,revenue_pacing
	 ,sellout_forecast	, tangent_line,
case when air_year= YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week))  then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as weeks_to_air, b.air_date 			
into #temp_wo_data_fcst			
 from [dbo].[demand_day_fcst] b,			
(SELECT  [market]			
      ,[Station]	as station_name		
      ,[daypart]		as daypart_name	
      ,[Inventory Code]		as invcode_name	
      ,cast (NULL as varchar(50))		as invcode_external_id	
      ,cast([program_start_time] as time)  [pgm_start_time]			
      ,cast([program_end_time] as time) [pgm_end_time]			
      ,avg(CAP) as cap			
      ,[Station Affiliation]	as Affiliation
	  ,air_week as airweek
	  ,air_year as airyear		
  FROM #temp_cap
  where  program_start_time is not null	
and  [Inventory Code] is not null
 and Station in     (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
 and  air_year >= year(getdate())
  group by [market]			
		,[Station Affiliation]	
      ,Station			
      ,[daypart]			
      ,[Inventory Code]			
        ,air_year
	  ,air_week			
	  ,cast([program_start_time] as time) 		
      ,cast([program_end_time] as time)			
	 ) a		
	 where a.station_name = b.station		
	 and a.daypart_name = b.daypart		
	 and a.invcode_name = b.invcode_name
	 and a.airyear =b.air_year
	 and a.airweek = b.air_week	
 	and b.as_of_date = cast(getdate() as date);	
   -- and b.as_of_date = ''01/01/2018'';

BEGIN TRY DROP TABLE #temp_ext_id END TRY BEGIN CATCH END CATCH;		

select distinct Station_Name, daypart_name, invcode_name , invcode_External_Id
into #temp_ext_id
from buyer_demand_fcst_input_new2
where 1=1
and  Station_Name in      (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
and air_year >=2018;


update #temp_wo_data_fcst
set invcode_external_id =b.invcode_External_Id
from #temp_wo_data_fcst a, #temp_ext_id b
where a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


update #temp_wo
set invcode_external_id =b.invcode_External_Id
from #temp_wo a, #temp_ext_id b
where a.invcode_External_Id is NULL
and a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


/* Step 5: Add forecast weekly into historical weekly table*/	
		
insert into #temp_wo			
select a.market, a.affiliation,a.Station_Name, a.daypart_name, a.invcode_name,   a.invcode_External_Id,  [pgm_start_time] ,			
 [pgm_end_time], air_year as wo_pgm_year, month(air_date) as wo_pgm_month, a.air_year, a.air_week, a.Order_year, a.order_week, 			
weeks_to_air as week_to_air,			
air_date air_week_date, 			
air_year as disp_air_year,			
 0 as cum_revenue, 0 as cum_spots, 0 as gross_revenue, 0 as booked_spots, 			
a.cap, case when a.forecast_spots is NULl then 0 else forecast_spots end as forecast_spots
 ,case when revenue_forecast is NULL then 0 else revenue_forecast end as revenue_forecast
	 ,case when inventory_pacing is NULL then 0 else inventory_pacing end as inventory_pacing
	 ,case when revenue_pacing is NULL then 0 else revenue_pacing end as revenue_pacing
	 ,case when sellout_forecast	is NULL then 0 else sellout_forecast end as sellout_forecast
	 ,case when tangent_line	is NULL then 0 else tangent_line end as tangent_line
from #temp_wo_data_fcst a		;	



		
/*Step 6: Get display air year and week to air date*/			
			
BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;			
			
		
declare @currYear int			
declare @prioryear int			
declare @priorweek int			
declare @currdate date			
			
set @curryear  = 2017			
set @prioryear = 2016		
				
			
select a.*, ROW_NUMBER()  over (partition by a.market , a.Station_Name , a.daypart_name , a.invcode_name , a.air_year, a.air_week order by booked_spots desc) row_id			
into #tempwodata_r			
from			
 (select a.*,		
case when (week_to_air < a.air_week ) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@curryear AS VARCHAR)) + (a.order_week-1), 7) else			
DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@prioryear AS VARCHAR)) + (a.order_week-1), 7)  end as week_to_air_date			
from #temp_wo a) a 	;		
			
/*Step 7: Get peak sales */				
			
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH	;		
			
select a.* , case when (peak_sales_flag = ''Y'' and a.air_week > b.curr_week) then ((air_week - week_to_air) - curr_week) else 0 end as weeks_to_peak			
into #temp_wo_all			
 from 			
 (select  datepart(Iso_WEEK ,getdate()) curr_week) b ,			
(select b.*, case when b.row_id = 1 then ''Y'' else ''N'' end as peak_sales_flag			
from #tempwodata_r b ) a	;		



BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;	

BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;	

BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;	
			

			
/*Step 8: Get nielsen program details*/	
			
BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	
			
			
select a.* , b.programname, b.Starttime as start_time, b.endtime as end_time, b.DayPart as nielsen_DayPart, b.station as nielsen_station 
into #temp_wo_nielsen_match			
 from #temp_wo_all a left outer join wo_nielsen_pgm_lookup_3 b
on (a.market = b.wo_market  and a.daypart_name =b.daypart_name and a.invcode_name  = b.invcode_name )
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	

select a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time ,
a.wo_pgm_year, a.wo_pgm_month, a.air_year, a.air_week, a.order_year, a.Order_week, a.week_to_air, a.air_week_date, a.disp_air_year, a.cum_revenue, a.cum_spots
, a.gross_revenue, a.booked_spots, a.weekly_capacity, a.forecasted_spots,a.revenue_forecast, a.inventory_pacing, a.revenue_pacing, a.sellout_forecast, a.tangent_line
,a.week_to_air_date, a.row_id, a.peak_sales_flag, a.weeks_to_peak, b.programname, b.start_time, b.end_time, b.DayPart, b.station 		
into #temp_wo_nielsen_match_2		
 from #temp_wo_nielsen_match a left outer join wo_nielsen_pgm b
on (a.market = b.market  and a.daypart_name =b.daypart_name and a.air_week = b.nielsen_air_week 
and a.invcode_name  = b.invcode_name and a.invcode_External_Id = b.invcode_external_id  )
where a.programname is null
and b.air_year >=2016;
	

insert into  #temp_wo_nielsen_match
select * from  #temp_wo_nielsen_match_2	;



BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH		;

/*Step 9: Integrate Ratings Forecast*/			
			
--BEGIN TRY DROP TABLE wo_nielsen  END TRY BEGIN CATCH END CATCH		;		
truncate table    wo_nielsen;

insert into   wo_nielsen		
select a.* , b.Affiliation, a.air_week, b.playbacktype, b.sampletype,			
b.Ratingtype, b.demo , b.Rating, b.Share   , b.hutput
			
	    from #temp_wo_nielsen_match a left outer join  [dbo].[PROD_Ratings_Integrated]  b		
  on ( a.nielsen_station = b.Station	and a.nielsen_daypart = b.daypart			
  and a.air_year = YEAR(DATEADD(day, 26 - DATEPART(isoww, programweek), programweek))		
   and a.air_week =datepart(ISO_WEEK,b.programweek)		
 and (a.programname = b.programname  ))		
     where  b.Affiliation in (''ABC'', ''CBS'', ''NBC'', ''FOX'')	;
 

  

BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	

/* Step 10: Filter out valid records with forecasted data */

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;		

 select a.market as wo_market, a.affiliation as wo_affiliation, a.Station_name as wo_station_name, a.daypart_name as wo_daypart_name, a.invcode_name as wo_invcode_name, a.invcode_external_id			
  ,a.pgm_start_time as wo_pgm_start_time , a.pgm_end_time as wo_pgm_end_time , a.wo_pgm_year, month(a.air_week_date) wo_pgm_month , a.air_year, a.air_week, a.order_year, a.Order_week,			
   cast(a.air_week_date as date) air_week_date, max(cum_revenue) as accum_revenue, max(cum_spots) as cum_spots, sum(gross_revenue) gross_revenue, sum(booked_spots) booked_spots, avg(weekly_capacity) as capacity			
  ,max(a.forecasted_spots) as bookings_fcst, NULL as nielsen_programname , NULL as nielsen_starttime, NULL as nielsen_end_time, daypart as nielsen_Daypart			
  ,a.Station as nielsen_Station, a.nielsen_aff as nielsen_affiliation, nielsen_air_week, playbacktype, sampletype, ratingtype, demo, max(rating) as Avg_Rating, max(Share) as avg_Share			
  ,max(a.hutput) as avg_Hutput, a.week_to_air, cast(a.week_to_air_date as date) week_to_air_date, a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year ,max(revenue_forecast) as revenue_forecast 
  ,max( inventory_pacing) as inventory_pacing, max(revenue_pacing) as revenue_pacing, max(sellout_forecast) as sellout_forecast,max(tangent_line) as tangent_line
 into #temp_wo_nielsen_int 
 from   wo_nielsen a ,(select market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week, Count(*) as cnt
         from  wo_nielsen
         where  affiliation = nielsen_aff
		 and ((air_year = Year(Getdate()) and air_week  >= Datepart(ISO_WEEK, Getdate()) - 11) or (air_year = Year(Getdate())+1 and air_week  < Datepart(ISO_WEEK, Getdate()) -11 ))
		 and rating is not null
   group by  market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week) b
   where a.market = b.market
   and a.station_name = b.station_name
   and a.daypart_name = b.daypart_name
   and a.affiliation = b.affiliation
   and a.invcode_name = b.invcode_name
  -- and a.invcode_external_id = b.invcode_external_id
   and a.air_week = b.air_week
    group by a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time , a.wo_pgm_year, month(a.air_week_date),
 	a.air_year, a.air_week, a.order_year, a.Order_week,	cast(a.air_week_date as date) 	,  a.DayPart , a.station, a.nielsen_aff, a.nielsen_air_week, a.Playbacktype, a.Sampletype,
	a.RatingType, a.demo, a.week_to_air , cast(a.week_to_air_date as date) , a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year 	
 order by Order_week;


/* Step 11: Delete old Actual data */

-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in     (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
and Ratingtype =''Actual'';




/* Step 12: Insert actual (Ratings) data */
insert into wo_nielsen_integration 			
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
     from #temp_wo_nielsen_int a
where   air_year >= year(getdate())  and Ratingtype =''Actual'';


BEGIN TRY DROP TABLE #temp_wo_nielsen_act  END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_nielsen  END TRY BEGIN CATCH END CATCH		;



/* Step 13: Filter out Ratingtype = forecasted */

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst from #temp_wo_nielsen_int where  air_year >= year(getdate()) and RatingTYpe =''Forecasted'';

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;


/* Step 14: Filter out TEGNA aff*/

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst_a from #temp_wo_nielsen_fcst where wo_affiliation = nielsen_affiliation;


/* Step 15: Insert non tegna aff with HUT*/
 insert into #temp_wo_nielsen_fcst_a 
 select  b.wo_market, b.wo_affiliation, b.wo_Station_Name, b.wo_daypart_name, b.wo_invcode_name, b.invcode_External_Id, b.wo_pgm_start_time, b.wo_pgm_end_time, b.wo_pgm_year, b.wo_pgm_month, b.air_year
 , b.air_week, b.order_year, b.Order_week,  b.air_week_date,  b.accum_revenue, b.cum_spots, b.gross_revenue, b.booked_spots, b.capacity, b.bookings_fcst
  , b.nielsen_programname, b.nielsen_starttime, b.nielsen_end_time, b.nielsen_Daypart, b.nielsen_Station, b.nielsen_affiliation, b.nielsen_air_week, b.Playbacktype, b.Sampletype
 ,b.RatingType, b.Demo, b.avg_Rating, case when a.avg_hutput > 0 then ((b.avg_Rating/a.avg_hutput)*100) else 0 end as share, a.avg_hutput
, b.week_to_air,b.week_to_air_date,   b.weeks_to_peak , b.peak_sales_flag, b.disp_air_year, b.revenue_forecast, b.inventory_pacing, b.revenue_pacing, b.sellout_forecast,b.tangent_line
 from
( select *   from #temp_wo_nielsen_fcst
where  wo_affiliation = nielsen_affiliation ) a,
 (select *  from #temp_wo_nielsen_fcst
where wo_affiliation <> nielsen_affiliation) b
where a.wo_market = b.wo_market
and a.air_year = b.air_year
and a.air_week = b.air_week
and a.wo_invcode_name = b.wo_invcode_name
and a.wo_affiliation = b.wo_affiliation
and a.order_year = b.order_year
and a.Order_week = b.Order_week
and a.air_week_date = b.air_week_date
and a.week_to_air_date = b.week_to_air_date;


/* Step 16 a: Delete old Forecasted data */
-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in   (''KARE'',
''WWL'',
''KPNX'',
''KUSA'',
''KING'')
and Ratingtype =''Forecasted'' ;

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	

/* Step 16: Insert forecasted (Ratings) data */

insert into wo_nielsen_integration 	
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
from #temp_wo_nielsen_fcst_a a
where   air_year >= year(getdate()) and Ratingtype =''Forecasted''
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;




', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [wo_nielsen_batch3]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'wo_nielsen_batch3', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use Wide_Orbit;			
Go		
	
/**Step 2: Get historical cumulative spot and revenue*/			
BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;		
		
declare @cutoffweek datetime			
declare @currweek int			
set @cutoffweek  = (select max(programweek) from PROD_Ratings_Integrated where programyear =2018 and RatingType =''Actual''	)
set @currweek  = datepart(ISOWK, 	@cutoffweek)	


 select a.*,  			
 case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 			
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year			
,cast(program_start_time as time) pgm_start_time			
,cast(program_end_time as time) pgm_end_time			
,Year(full_date) as wo_pgm_year			
,MOnth(full_date) as wo_pgm_month			
, case when a.air_year= Order_Year then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as week_to_air			
,sum([spot_counts]) over (partition by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as cum_spots		
	, sum([gross_revenue]) over (partition by [market]		
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as accum_revenue		
	  ,0 as bd_forecast		
	  ,0 as bd_upper_limit		
	  ,0 as bd_lower_limit		
 into #tempwodata			
		  from [dbo].[buyer_demand_fcst_input_new2]  a	
		 where  program_start_time is not null
 and invcode_name not like ''%*%''  
and  gross_revenue >0 and spot_counts > 0	
and Station_name in  (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
and air_year >= year(getdate());




/*Step 3: get weekly historicals for buyer demand from using wide orbit data retrieved in Step 2*/			

BEGIN TRY DROP TABLE #temp_cap END TRY BEGIN CATCH END CATCH		;

select market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time,sum([Potential Units]) as cap
into #temp_cap
from 
( Select distinct market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week,[Air Date],program_Start_time,program_end_time,[Potential Units],break_code
FROM fact_inv_capacity_dv 
where 1=1 
and Station  in    (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
and air_year >=2018
and break_code in (''CM'')) as aa
group by market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time
order by air_week;


BEGIN TRY DROP TABLE #temp_cap_wo END TRY BEGIN CATCH END CATCH		;
select a.market, a.[Station Affiliation] as affiliation, a.Station as station_name,  a.Daypart as daypart_name, a.[Inventory Code] as invcode_name , null as invcode_External_Id,
program_Start_time as pgm_start_time, program_end_time as pgm_end_time, a.Air_year as wo_pgm_year, 1 as wo_pgm_month,a.Air_year ,a.Air_week , 2017 as order_year , 42 as order_week,
case when a.air_year= 2017 then (a.air_week - 42 ) else ( case when a.air_year <> 2016 then 52 - 42 + a.air_week else 53 - 42 +a.air_week end ) end as week_to_air	,
case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 						
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year	
 ,cap
 into #temp_cap_wo	
 from #temp_cap a
 order by Air_week;


BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

select b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year , month(b.air_week_date) as wo_pgm_month , b.air_year, b.air_week ,  
coalesce(a.order_year , b.order_year) as order_year
, coalesce(a.Order_week ,b.order_week) as order_week, 			
coalesce(a.week_to_air ,b.week_to_air) as week_to_air,  b.air_week_date, b.disp_air_year, coalesce(max(a.accum_revenue),0) as cum_revenue, coalesce(max(a.cum_spots),0) as cum_spots, coalesce(sum(a.gross_revenue),0) as gross_revenue, 
coalesce(sum(a.[spot_counts]) ,0) as booked_spots			
,avg(b.cap) as weekly_capacity , 0 as forecasted_spots ,  0 as revenue_forecast
	 ,0 as inventory_pacing
	 ,0 as revenue_pacing
	 ,0 as sellout_forecast	
	 ,0 as tangent_line
into #temp_wo			
from #temp_cap_wo b left outer join #tempwodata a 	on (b.station_name =a.station_name and b.daypart_name =a.daypart_name and b.invcode_name =a.invcode_Name and b.Air_year =a.air_year and b.air_week =a.air_week)		
group by b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year ,  b.air_year, b.disp_air_year, b.air_week  ,  a.order_year, a.Order_week, 
	 b.order_year, b.Order_week, 			
a.week_to_air, b.week_to_air,  b.air_week_date			
order by a.order_year, a.order_week	;



/* Step 4: Get weekly buyer demand forecast data and append WO program details to it*/			
BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;			
			
select a.*, b.air_year, b.air_week, order_week, YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week)) as Order_year 
, round(b.predict,0)  as forecast_spots,   revenue_forecast
	 ,inventory_pacing
	 ,revenue_pacing
	 ,sellout_forecast	, tangent_line,
case when air_year= YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week))  then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as weeks_to_air, b.air_date 			
into #temp_wo_data_fcst			
 from [dbo].[demand_day_fcst] b,			
(SELECT  [market]			
      ,[Station]	as station_name		
      ,[daypart]		as daypart_name	
      ,[Inventory Code]		as invcode_name	
      ,cast (NULL as varchar(50))		as invcode_external_id	
      ,cast([program_start_time] as time)  [pgm_start_time]			
      ,cast([program_end_time] as time) [pgm_end_time]			
      ,avg(CAP) as cap			
      ,[Station Affiliation]	as Affiliation
	  ,air_week as airweek
	  ,air_year as airyear		
  FROM #temp_cap
  where  program_start_time is not null	
and  [Inventory Code] is not null
 and Station in    (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
 and  air_year >= year(getdate())
  group by [market]			
		,[Station Affiliation]	
      ,Station			
      ,[daypart]			
      ,[Inventory Code]			
        ,air_year
	  ,air_week			
	  ,cast([program_start_time] as time) 		
      ,cast([program_end_time] as time)			
	 ) a		
	 where a.station_name = b.station		
	 and a.daypart_name = b.daypart		
	 and a.invcode_name = b.invcode_name
	 and a.airyear =b.air_year
	 and a.airweek = b.air_week	
 	 and b.as_of_date = cast(getdate() as date);	
  --   and b.as_of_date = ''01/01/2018'';
	

BEGIN TRY DROP TABLE #temp_ext_id END TRY BEGIN CATCH END CATCH;		

select distinct Station_Name, daypart_name, invcode_name , invcode_External_Id
into #temp_ext_id
from buyer_demand_fcst_input_new2
where 1=1
and  Station_Name in     (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
and air_year >=2018;


update #temp_wo_data_fcst
set invcode_external_id =b.invcode_External_Id
from #temp_wo_data_fcst a, #temp_ext_id b
where a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


update #temp_wo
set invcode_external_id =b.invcode_External_Id
from #temp_wo a, #temp_ext_id b
where a.invcode_External_Id is NULL
and a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


/* Step 5: Add forecast weekly into historical weekly table*/	
		
insert into #temp_wo			
select a.market, a.affiliation,a.Station_Name, a.daypart_name, a.invcode_name,   a.invcode_External_Id,  [pgm_start_time] ,			
 [pgm_end_time], air_year as wo_pgm_year, month(air_date) as wo_pgm_month, a.air_year, a.air_week, a.Order_year, a.order_week, 			
weeks_to_air as week_to_air,			
air_date air_week_date, 			
air_year as disp_air_year,			
 0 as cum_revenue, 0 as cum_spots, 0 as gross_revenue, 0 as booked_spots, 			
a.cap, case when a.forecast_spots is NULl then 0 else forecast_spots end as forecast_spots
 ,case when revenue_forecast is NULL then 0 else revenue_forecast end as revenue_forecast
	 ,case when inventory_pacing is NULL then 0 else inventory_pacing end as inventory_pacing
	 ,case when revenue_pacing is NULL then 0 else revenue_pacing end as revenue_pacing
	 ,case when sellout_forecast	is NULL then 0 else sellout_forecast end as sellout_forecast
	 ,case when tangent_line	is NULL then 0 else tangent_line end as tangent_line
from #temp_wo_data_fcst a		;	



		
/*Step 6: Get display air year and week to air date*/			
			
BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;			
			
		
declare @currYear int			
declare @prioryear int			
declare @priorweek int			
declare @currdate date			
			
set @curryear  = 2017			
set @prioryear = 2016		
						
			
select a.*, ROW_NUMBER()  over (partition by a.market , a.Station_Name , a.daypart_name , a.invcode_name , a.air_year, a.air_week order by booked_spots desc) row_id			
into #tempwodata_r			
from			
 (select a.*,		
case when (week_to_air < a.air_week ) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@curryear AS VARCHAR)) + (a.order_week-1), 7) else			
DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@prioryear AS VARCHAR)) + (a.order_week-1), 7)  end as week_to_air_date			
from #temp_wo a) a 	;		
			
/*Step 7: Get peak sales */				
			
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH	;		
			
select a.* , case when (peak_sales_flag = ''Y'' and a.air_week > b.curr_week) then ((air_week - week_to_air) - curr_week) else 0 end as weeks_to_peak			
into #temp_wo_all			
 from 			
 (select  datepart(Iso_WEEK ,getdate()) curr_week) b ,			
(select b.*, case when b.row_id = 1 then ''Y'' else ''N'' end as peak_sales_flag			
from #tempwodata_r b ) a	;		



BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;	

BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;	

BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;	
			

			
/*Step 8: Get nielsen program details*/	
			
BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	
				
select a.* , b.programname, b.Starttime as start_time, b.endtime as end_time, b.DayPart as nielsen_DayPart, b.station as nielsen_station 
into #temp_wo_nielsen_match			
 from #temp_wo_all a left outer join wo_nielsen_pgm_lookup_3 b
on (a.market = b.wo_market  and a.daypart_name =b.daypart_name and a.invcode_name  = b.invcode_name )
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	

select a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time ,
a.wo_pgm_year, a.wo_pgm_month, a.air_year, a.air_week, a.order_year, a.Order_week, a.week_to_air, a.air_week_date, a.disp_air_year, a.cum_revenue, a.cum_spots
, a.gross_revenue, a.booked_spots, a.weekly_capacity, a.forecasted_spots,a.revenue_forecast, a.inventory_pacing, a.revenue_pacing, a.sellout_forecast, a.tangent_line
,a.week_to_air_date, a.row_id, a.peak_sales_flag, a.weeks_to_peak, b.programname, b.start_time, b.end_time, b.DayPart, b.station 		
into #temp_wo_nielsen_match_2		
 from #temp_wo_nielsen_match a left outer join wo_nielsen_pgm b
on (a.market = b.market  and a.daypart_name =b.daypart_name and a.air_week = b.nielsen_air_week 
and a.invcode_name  = b.invcode_name and a.invcode_External_Id = b.invcode_external_id  )
where a.programname is null
and b.air_year >=2016;
	

insert into  #temp_wo_nielsen_match
select * from  #temp_wo_nielsen_match_2	;



BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH		;

/*Step 9: Integrate Ratings Forecast*/			
			
--BEGIN TRY DROP TABLE wo_nielsen  END TRY BEGIN CATCH END CATCH		;		
truncate table    wo_nielsen;

insert into   wo_nielsen		
select a.* , b.Affiliation, a.air_week, b.playbacktype, b.sampletype,			
b.Ratingtype, b.demo , b.Rating, b.Share   , b.hutput
			
	    from #temp_wo_nielsen_match a left outer join  [dbo].[PROD_Ratings_Integrated]  b		
  on ( a.nielsen_station = b.Station	and a.nielsen_daypart = b.daypart			
  and a.air_year = YEAR(DATEADD(day, 26 - DATEPART(isoww, programweek), programweek))			
   and a.air_week = datepart(ISO_WEEK,b.programweek)			
 and (a.programname = b.programname  ))		
     where  b.Affiliation in (''ABC'', ''CBS'', ''NBC'', ''FOX'')	;
 

  

BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	

/* Step 10: Filter out valid records with forecasted data */

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;		

 select a.market as wo_market, a.affiliation as wo_affiliation, a.Station_name as wo_station_name, a.daypart_name as wo_daypart_name, a.invcode_name as wo_invcode_name, a.invcode_external_id			
  ,a.pgm_start_time as wo_pgm_start_time , a.pgm_end_time as wo_pgm_end_time , a.wo_pgm_year, month(a.air_week_date) wo_pgm_month , a.air_year, a.air_week, a.order_year, a.Order_week,			
   cast(a.air_week_date as date) air_week_date, max(cum_revenue) as accum_revenue, max(cum_spots) as cum_spots, sum(gross_revenue) gross_revenue, sum(booked_spots) booked_spots, avg(weekly_capacity) as capacity			
  ,max(a.forecasted_spots) as bookings_fcst, NULL as nielsen_programname , NULL as nielsen_starttime, NULL as nielsen_end_time, daypart as nielsen_Daypart			
  ,a.Station as nielsen_Station, a.nielsen_aff as nielsen_affiliation, nielsen_air_week, playbacktype, sampletype, ratingtype, demo, max(rating) as Avg_Rating, max(Share) as avg_Share			
  ,max(a.hutput) as avg_Hutput, a.week_to_air, cast(a.week_to_air_date as date) week_to_air_date, a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year ,max(revenue_forecast) as revenue_forecast 
  ,max( inventory_pacing) as inventory_pacing, max(revenue_pacing) as revenue_pacing, max(sellout_forecast) as sellout_forecast,max(tangent_line) as tangent_line
 into #temp_wo_nielsen_int 
 from   wo_nielsen a ,(select market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week, Count(*) as cnt
         from  wo_nielsen
         where  affiliation = nielsen_aff
		 and ((air_year = Year(Getdate()) and air_week  >= Datepart(ISO_WEEK, Getdate()) - 11) or (air_year = Year(Getdate())+1 and air_week  < Datepart(ISO_WEEK, Getdate()) -11 ))
		 and rating is not null
   group by  market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week) b
   where a.market = b.market
   and a.station_name = b.station_name
   and a.daypart_name = b.daypart_name
   and a.affiliation = b.affiliation
   and a.invcode_name = b.invcode_name
  -- and a.invcode_external_id = b.invcode_external_id
   and a.air_week = b.air_week
    group by a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time , a.wo_pgm_year, month(a.air_week_date),
 	a.air_year, a.air_week, a.order_year, a.Order_week,	cast(a.air_week_date as date) 	,  a.DayPart , a.station, a.nielsen_aff, a.nielsen_air_week, a.Playbacktype, a.Sampletype,
	a.RatingType, a.demo, a.week_to_air , cast(a.week_to_air_date as date) , a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year 	
 order by Order_week;


/* Step 11: Delete old Actual data */

-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in     (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
and Ratingtype =''Actual'';




/* Step 12: Insert actual (Ratings) data */
insert into wo_nielsen_integration 			
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
     from #temp_wo_nielsen_int a
where  air_year >= year(getdate())  and Ratingtype =''Actual'';


BEGIN TRY DROP TABLE #temp_wo_nielsen_act  END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_nielsen  END TRY BEGIN CATCH END CATCH		;



/* Step 13: Filter out Ratingtype = forecasted */

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst from #temp_wo_nielsen_int where  air_year >= year(getdate()) and RatingTYpe =''Forecasted'';

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;


/* Step 14: Filter out TEGNA aff*/

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst_a from #temp_wo_nielsen_fcst where wo_affiliation = nielsen_affiliation;


/* Step 15: Insert non tegna aff with HUT*/
 insert into #temp_wo_nielsen_fcst_a 
 select  b.wo_market, b.wo_affiliation, b.wo_Station_Name, b.wo_daypart_name, b.wo_invcode_name, b.invcode_External_Id, b.wo_pgm_start_time, b.wo_pgm_end_time, b.wo_pgm_year, b.wo_pgm_month, b.air_year
 , b.air_week, b.order_year, b.Order_week,  b.air_week_date,  b.accum_revenue, b.cum_spots, b.gross_revenue, b.booked_spots, b.capacity, b.bookings_fcst
  , b.nielsen_programname, b.nielsen_starttime, b.nielsen_end_time, b.nielsen_Daypart, b.nielsen_Station, b.nielsen_affiliation, b.nielsen_air_week, b.Playbacktype, b.Sampletype
 ,b.RatingType, b.Demo, b.avg_Rating, case when a.avg_hutput > 0 then ((b.avg_Rating/a.avg_hutput)*100) else 0 end as share, a.avg_hutput
, b.week_to_air,b.week_to_air_date,   b.weeks_to_peak , b.peak_sales_flag, b.disp_air_year, b.revenue_forecast, b.inventory_pacing, b.revenue_pacing, b.sellout_forecast,b.tangent_line
 from
( select *   from #temp_wo_nielsen_fcst
where  wo_affiliation = nielsen_affiliation ) a,
 (select *  from #temp_wo_nielsen_fcst
where wo_affiliation <> nielsen_affiliation) b
where a.wo_market = b.wo_market
and a.air_year = b.air_year
and a.air_week = b.air_week
and a.wo_invcode_name = b.wo_invcode_name
and a.wo_affiliation = b.wo_affiliation
and a.order_year = b.order_year
and a.Order_week = b.Order_week
and a.air_week_date = b.air_week_date
and a.week_to_air_date = b.week_to_air_date;


/* Step 16 a: Delete old Forecasted data */
-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in    (''KENS'',
''WFMY'',
''KXTV'',
''WTSP'',
''WUSA''
,''WBIR'')
and Ratingtype =''Forecasted'' ;

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	

/* Step 16: Insert forecasted (Ratings) data */

insert into wo_nielsen_integration 	
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
from #temp_wo_nielsen_fcst_a a
where  air_year >= year(getdate()) and Ratingtype =''Forecasted''
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;




', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [wo_nielsen_batch4]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'wo_nielsen_batch4', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use Wide_Orbit;			
Go		
	
/**Step 2: Get historical cumulative spot and revenue*/			
BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;		
		
declare @cutoffweek datetime			
declare @currweek int			
set @cutoffweek  = (select max(programweek) from PROD_Ratings_Integrated where programyear =2018 and RatingType =''Actual''	)
set @currweek  = datepart(ISOWK, 	@cutoffweek)	


 select a.*,  			
 case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 			
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year			
,cast(program_start_time as time) pgm_start_time			
,cast(program_end_time as time) pgm_end_time			
,Year(full_date) as wo_pgm_year			
,MOnth(full_date) as wo_pgm_month			
, case when a.air_year= Order_Year then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as week_to_air			
,sum([spot_counts]) over (partition by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as cum_spots		
	, sum([gross_revenue]) over (partition by [market]		
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
     order by [market]			
	  , [affiliation]		
      ,[Station_Name]			
      ,[daypart_name]			
      ,[invcode_name]			
      ,[invcode_External_Id]			
	  ,[air_year]		
      ,[air_week]			
      ,[order_year]			
      ,[Order_week] 			
	  rows unbounded preceding) as accum_revenue		
	  ,0 as bd_forecast		
	  ,0 as bd_upper_limit		
	  ,0 as bd_lower_limit		
 into #tempwodata			
		  from [dbo].[buyer_demand_fcst_input_new2]  a	
		 where  program_start_time is not null
 and invcode_name not like ''%*%''  
and  gross_revenue >0	 and spot_counts > 0
and Station_name in (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
and air_year >= year(getdate());



/*Step 3: get weekly historicals for buyer demand from using wide orbit data retrieved in Step 2*/			

BEGIN TRY DROP TABLE #temp_cap END TRY BEGIN CATCH END CATCH		;

select market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time,sum([Potential Units]) as cap
into #temp_cap
from 
( Select distinct market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week,[Air Date],program_Start_time,program_end_time,[Potential Units],break_code
FROM fact_inv_capacity_dv 
where 1=1 
and Station  in   (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
and air_year >=2018
and break_code in (''CM'')) as aa
group by market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week, program_Start_time, program_end_time
order by air_week;

BEGIN TRY DROP TABLE #temp_cap_wo END TRY BEGIN CATCH END CATCH		;

select a.market, a.[Station Affiliation] as affiliation, a.Station as station_name,  a.Daypart as daypart_name, a.[Inventory Code] as invcode_name , null as invcode_External_Id,
program_Start_time as pgm_start_time, program_end_time as pgm_end_time, a.Air_year as wo_pgm_year, 1 as wo_pgm_month,a.Air_year ,a.Air_week , 2017 as order_year , 42 as order_week,
case when a.air_year= 2017 then (a.air_week - 42 ) else ( case when a.air_year <> 2016 then 52 - 42 + a.air_week else 53 - 42 +a.air_week end ) end as week_to_air	,
case when (air_week <= @currweek) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate())+1 AS VARCHAR)) + (a.air_week-1), 7) 			
 else  DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(Year(getdate()) AS VARCHAR)) + (a.air_week-1), 7) end as air_week_date 					
 , case when (air_week <= @currweek) then air_year - 1  else  air_year end as disp_air_year	
 ,cap
 into #temp_cap_wo	
 from #temp_cap a
 order by Air_week;


BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

select b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year , month(b.air_week_date) as wo_pgm_month , b.air_year, b.air_week ,  
coalesce(a.order_year , b.order_year) as order_year
, coalesce(a.Order_week ,b.order_week) as order_week, 			
coalesce(a.week_to_air ,b.week_to_air) as week_to_air,  b.air_week_date, b.disp_air_year, coalesce(max(a.accum_revenue),0) as cum_revenue, coalesce(max(a.cum_spots),0) as cum_spots, coalesce(sum(a.gross_revenue),0) as gross_revenue, 
coalesce(sum(a.[spot_counts]) ,0) as booked_spots			
,avg(b.cap) as weekly_capacity , 0 as forecasted_spots ,  0 as revenue_forecast
	 ,0 as inventory_pacing
	 ,0 as revenue_pacing
	 ,0 as sellout_forecast	
	 ,0 as tangent_line
into #temp_wo			
from #temp_cap_wo b left outer join #tempwodata a 	on (b.station_name =a.station_name and b.daypart_name =a.daypart_name and b.invcode_name =a.invcode_Name and b.Air_year =a.air_year and b.air_week =a.air_week)		
group by b.market, b.affiliation , b.Station_Name, b.daypart_name,b.invcode_name, a.invcode_External_Id  ,b.pgm_start_time, b.pgm_end_time, b.wo_pgm_year ,  b.air_year, b.disp_air_year, b.air_week  ,  a.order_year, a.Order_week, 
	 b.order_year, b.Order_week, 			
a.week_to_air, b.week_to_air,  b.air_week_date			
order by a.order_year, a.order_week	;



/* Step 4: Get weekly buyer demand forecast data and append WO program details to it*/			
BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;			
			
select a.*, b.air_year, b.air_week, order_week, YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week)) as Order_year 
, round(b.predict,0) as forecast_spots,   revenue_forecast
	 ,inventory_pacing
	 ,revenue_pacing
	 ,sellout_forecast	, tangent_line,
case when air_year=YEAR(DATEADD(day, 26 - DATEPART(isoww, ordered_week), ordered_week))  then (air_week - order_week ) else ( case when air_year <> 2016 then 52 - order_week + air_week else 53 - order_week +air_week end ) end as weeks_to_air, b.air_date 			
into #temp_wo_data_fcst			
 from [dbo].[demand_day_fcst] b,			
(SELECT  [market]			
      ,[Station]	as station_name		
      ,[daypart]		as daypart_name	
      ,[Inventory Code]		as invcode_name	
      ,cast (NULL as varchar(50))		as invcode_external_id	
      ,cast([program_start_time] as time)  [pgm_start_time]			
      ,cast([program_end_time] as time) [pgm_end_time]			
      ,avg(CAP) as cap			
      ,[Station Affiliation]	as Affiliation
	  ,air_week as airweek
	  ,air_year as airyear		
  FROM #temp_cap
  where  program_start_time is not null	
and  [Inventory Code] is not null
 and Station in   (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
 and  air_year >= year(getdate())
  group by [market]			
		,[Station Affiliation]	
      ,Station			
      ,[daypart]			
      ,[Inventory Code]			
        ,air_year
	  ,air_week			
	  ,cast([program_start_time] as time) 		
      ,cast([program_end_time] as time)			
	 ) a		
	 where a.station_name = b.station		
	 and a.daypart_name = b.daypart		
	 and a.invcode_name = b.invcode_name
	 and a.airyear =b.air_year
	 and a.airweek = b.air_week	
 	   and b.as_of_date = cast(getdate() as date);	
---    and b.as_of_date = ''01/01/2018'';
	

BEGIN TRY DROP TABLE #temp_ext_id END TRY BEGIN CATCH END CATCH;		

select distinct Station_Name, daypart_name, invcode_name , invcode_External_Id
into #temp_ext_id
from buyer_demand_fcst_input_new2
where 1=1
and  Station_Name in    (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
and air_year >=2018;


update #temp_wo_data_fcst
set invcode_external_id =b.invcode_External_Id
from #temp_wo_data_fcst a, #temp_ext_id b
where a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


update #temp_wo
set invcode_external_id =b.invcode_External_Id
from #temp_wo a, #temp_ext_id b
where a.invcode_External_Id is NULL
and a.station_name = b.Station_Name
and a.daypart_name =b.daypart_name
and a.invcode_name =b.invcode_name;


/* Step 5: Add forecast weekly into historical weekly table*/	
		
insert into #temp_wo			
select a.market, a.affiliation,a.Station_Name, a.daypart_name, a.invcode_name,   a.invcode_External_Id,  [pgm_start_time] ,			
 [pgm_end_time], air_year as wo_pgm_year, month(air_date) as wo_pgm_month, a.air_year, a.air_week, a.Order_year, a.order_week, 			
weeks_to_air as week_to_air,			
air_date air_week_date, 			
air_year as disp_air_year,			
 0 as cum_revenue, 0 as cum_spots, 0 as gross_revenue, 0 as booked_spots, 			
a.cap, case when a.forecast_spots is NULl then 0 else forecast_spots end as forecast_spots
 ,case when revenue_forecast is NULL then 0 else revenue_forecast end as revenue_forecast
	 ,case when inventory_pacing is NULL then 0 else inventory_pacing end as inventory_pacing
	 ,case when revenue_pacing is NULL then 0 else revenue_pacing end as revenue_pacing
	 ,case when sellout_forecast	is NULL then 0 else sellout_forecast end as sellout_forecast
	 ,case when tangent_line	is NULL then 0 else tangent_line end as tangent_line
from #temp_wo_data_fcst a		;	



		
/*Step 6: Get display air year and week to air date*/			
			
BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;			
			
		
declare @currYear int			
declare @prioryear int			
declare @priorweek int			
declare @currdate date			
			
set @curryear  = 2017			
set @prioryear = 2016		
					
			
select a.*, ROW_NUMBER()  over (partition by a.market , a.Station_Name , a.daypart_name , a.invcode_name , a.air_year, a.air_week order by booked_spots desc) row_id			
into #tempwodata_r			
from			
 (select a.*,		
case when (week_to_air < a.air_week ) then DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@curryear AS VARCHAR)) + (a.order_week-1), 7) else			
DATEADD(wk, DATEDIFF(WK, 7, ''1/1/'' + CASt(@prioryear AS VARCHAR)) + (a.order_week-1), 7)  end as week_to_air_date			
from #temp_wo a) a 	;		
			
/*Step 7: Get peak sales */				
			
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH	;		
			
select a.* , case when (peak_sales_flag = ''Y'' and a.air_week > b.curr_week) then ((air_week - week_to_air) - curr_week) else 0 end as weeks_to_peak			
into #temp_wo_all			
 from 			
 (select  datepart(Iso_WEEK ,getdate()) curr_week) b ,			
(select b.*, case when b.row_id = 1 then ''Y'' else ''N'' end as peak_sales_flag			
from #tempwodata_r b ) a	;		



BEGIN TRY DROP TABLE #tempwodata END TRY BEGIN CATCH END CATCH	;	

BEGIN TRY DROP TABLE #temp_wo END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_data_fcst END TRY BEGIN CATCH END CATCH;	

BEGIN TRY DROP TABLE #tempwodata_r END TRY BEGIN CATCH END CATCH;	
			

			
/*Step 8: Get nielsen program details*/	
			
BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	
			
			
select a.* , b.programname, b.Starttime as start_time, b.endtime as end_time, b.DayPart as nielsen_DayPart, b.station as nielsen_station 
into #temp_wo_nielsen_match			
 from #temp_wo_all a left outer join wo_nielsen_pgm_lookup_3 b
on (a.market = b.wo_market  and a.daypart_name =b.daypart_name and a.invcode_name  = b.invcode_name )
;

BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	

select a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time ,
a.wo_pgm_year, a.wo_pgm_month, a.air_year, a.air_week, a.order_year, a.Order_week, a.week_to_air, a.air_week_date, a.disp_air_year, a.cum_revenue, a.cum_spots
, a.gross_revenue, a.booked_spots, a.weekly_capacity, a.forecasted_spots,a.revenue_forecast, a.inventory_pacing, a.revenue_pacing, a.sellout_forecast, a.tangent_line
,a.week_to_air_date, a.row_id, a.peak_sales_flag, a.weeks_to_peak, b.programname, b.start_time, b.end_time, b.DayPart, b.station 		
into #temp_wo_nielsen_match_2		
 from #temp_wo_nielsen_match a left outer join wo_nielsen_pgm b
on (a.market = b.market  and a.daypart_name =b.daypart_name and a.air_week = b.nielsen_air_week 
and a.invcode_name  = b.invcode_name and a.invcode_External_Id = b.invcode_external_id  )
where a.programname is null
and b.air_year >=2016;
	

insert into  #temp_wo_nielsen_match
select * from  #temp_wo_nielsen_match_2	;



BEGIN TRY DROP TABLE #temp_wo_nielsen_match_2 END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_all END TRY BEGIN CATCH END CATCH		;

/*Step 9: Integrate Ratings Forecast*/			
			
--BEGIN TRY DROP TABLE wo_nielsen  END TRY BEGIN CATCH END CATCH		;		
truncate table    wo_nielsen;

insert into   wo_nielsen		
select a.* , b.Affiliation, a.air_week, b.playbacktype, b.sampletype,			
b.Ratingtype, b.demo , b.Rating, b.Share   , b.hutput
			
	    from #temp_wo_nielsen_match a left outer join  [dbo].[PROD_Ratings_Integrated]  b		
  on ( a.nielsen_station = b.Station	and a.nielsen_daypart = b.daypart			
  and a.air_year = YEAR(DATEADD(day, 26 - DATEPART(isoww, programweek), programweek))	 		
   and a.air_week = datepart(ISO_WEEK,b.programweek)			
 and (a.programname = b.programname  ))		
     where  b.Affiliation in (''ABC'', ''CBS'', ''NBC'', ''FOX'')	;
 

  

BEGIN TRY DROP TABLE #temp_wo_nielsen_match END TRY BEGIN CATCH END CATCH		;	

/* Step 10: Filter out valid records with forecasted data */

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;		

 select a.market as wo_market, a.affiliation as wo_affiliation, a.Station_name as wo_station_name, a.daypart_name as wo_daypart_name, a.invcode_name as wo_invcode_name, a.invcode_external_id			
  ,a.pgm_start_time as wo_pgm_start_time , a.pgm_end_time as wo_pgm_end_time , a.wo_pgm_year, month(a.air_week_date) wo_pgm_month , a.air_year, a.air_week, a.order_year, a.Order_week,			
   cast(a.air_week_date as date) air_week_date, max(cum_revenue) as accum_revenue, max(cum_spots) as cum_spots, sum(gross_revenue) gross_revenue, sum(booked_spots) booked_spots, avg(weekly_capacity) as capacity			
  ,max(a.forecasted_spots) as bookings_fcst, NULL as nielsen_programname , NULL as nielsen_starttime, NULL as nielsen_end_time, daypart as nielsen_Daypart			
  ,a.Station as nielsen_Station, a.nielsen_aff as nielsen_affiliation, nielsen_air_week, playbacktype, sampletype, ratingtype, demo, max(rating) as Avg_Rating, max(Share) as avg_Share			
  ,max(a.hutput) as avg_Hutput, a.week_to_air, cast(a.week_to_air_date as date) week_to_air_date, a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year ,max(revenue_forecast) as revenue_forecast 
  ,max( inventory_pacing) as inventory_pacing, max(revenue_pacing) as revenue_pacing, max(sellout_forecast) as sellout_forecast,max(tangent_line) as tangent_line
 into #temp_wo_nielsen_int 
 from   wo_nielsen a ,(select market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week, Count(*) as cnt
         from  wo_nielsen
         where  affiliation = nielsen_aff
		 and ((air_year = Year(Getdate()) and air_week  >= Datepart(ISO_WEEK, Getdate()) - 11) or (air_year = Year(Getdate())+1 and air_week  < Datepart(ISO_WEEK, Getdate()) -11 ))
		 and rating is not null
   group by  market, station_name, affiliation, daypart_name, invcode_name , invcode_external_id,air_week) b
   where a.market = b.market
   and a.station_name = b.station_name
   and a.daypart_name = b.daypart_name
   and a.affiliation = b.affiliation
   and a.invcode_name = b.invcode_name
  -- and a.invcode_external_id = b.invcode_external_id
   and a.air_week = b.air_week
    group by a.market, a.affiliation, a.Station_Name, a.daypart_name, a.invcode_name, a.invcode_External_Id, a.pgm_start_time, a.pgm_end_time , a.wo_pgm_year, month(a.air_week_date),
 	a.air_year, a.air_week, a.order_year, a.Order_week,	cast(a.air_week_date as date) 	,  a.DayPart , a.station, a.nielsen_aff, a.nielsen_air_week, a.Playbacktype, a.Sampletype,
	a.RatingType, a.demo, a.week_to_air , cast(a.week_to_air_date as date) , a.weeks_to_peak, a.peak_sales_flag, a.disp_air_year 	
 order by Order_week;


/* Step 11: Delete old Actual data */

-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in    (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
and Ratingtype =''Actual'';




/* Step 12: Insert actual (Ratings) data */
insert into wo_nielsen_integration 			
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
     from #temp_wo_nielsen_int a
where air_year >= year(getdate())  and Ratingtype =''Actual'';


BEGIN TRY DROP TABLE #temp_wo_nielsen_act  END TRY BEGIN CATCH END CATCH		;	
BEGIN TRY DROP TABLE #temp_wo_nielsen  END TRY BEGIN CATCH END CATCH		;



/* Step 13: Filter out Ratingtype = forecasted */

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst from #temp_wo_nielsen_int where  air_year >= year(getdate()) and RatingTYpe =''Forecasted'';

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;


/* Step 14: Filter out TEGNA aff*/

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	
select * into #temp_wo_nielsen_fcst_a from #temp_wo_nielsen_fcst where wo_affiliation = nielsen_affiliation;


/* Step 15: Insert non tegna aff with HUT*/
 insert into #temp_wo_nielsen_fcst_a 
 select  b.wo_market, b.wo_affiliation, b.wo_Station_Name, b.wo_daypart_name, b.wo_invcode_name, b.invcode_External_Id, b.wo_pgm_start_time, b.wo_pgm_end_time, b.wo_pgm_year, b.wo_pgm_month, b.air_year
 , b.air_week, b.order_year, b.Order_week,  b.air_week_date,  b.accum_revenue, b.cum_spots, b.gross_revenue, b.booked_spots, b.capacity, b.bookings_fcst
  , b.nielsen_programname, b.nielsen_starttime, b.nielsen_end_time, b.nielsen_Daypart, b.nielsen_Station, b.nielsen_affiliation, b.nielsen_air_week, b.Playbacktype, b.Sampletype
 ,b.RatingType, b.Demo, b.avg_Rating, case when a.avg_hutput > 0 then ((b.avg_Rating/a.avg_hutput)*100) else 0 end as share, a.avg_hutput
, b.week_to_air,b.week_to_air_date,   b.weeks_to_peak , b.peak_sales_flag, b.disp_air_year, b.revenue_forecast, b.inventory_pacing, b.revenue_pacing, b.sellout_forecast,b.tangent_line
 from
( select *   from #temp_wo_nielsen_fcst
where  wo_affiliation = nielsen_affiliation ) a,
 (select *  from #temp_wo_nielsen_fcst
where wo_affiliation <> nielsen_affiliation) b
where a.wo_market = b.wo_market
and a.air_year = b.air_year
and a.air_week = b.air_week
and a.wo_invcode_name = b.wo_invcode_name
and a.wo_affiliation = b.wo_affiliation
and a.order_year = b.order_year
and a.Order_week = b.Order_week
and a.air_week_date = b.air_week_date
and a.week_to_air_date = b.week_to_air_date;


/* Step 16 a: Delete old Forecasted data */
-- Delete data 

delete from wo_nielsen_integration where  air_year >= year(getdate())  and wo_station_name  in   (''KHOU'',
''WTLV'',
''KSDK'',
''WVEC'',
''WGRZ'',
''WCSH'')
and Ratingtype =''Forecasted'' ;

BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst  END TRY BEGIN CATCH END CATCH		;	

/* Step 16: Insert forecasted (Ratings) data */

insert into wo_nielsen_integration 	
select [wo_market]
      ,[wo_affiliation]
      ,[wo_station_name]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[invcode_external_id]
      ,cast([wo_pgm_start_time] as time)
      ,cast([wo_pgm_end_time] as time)
      ,[wo_pgm_year]
      ,[wo_pgm_month]
      ,[air_year]
      ,[air_week]
      ,[order_year]
      ,[Order_week]
      ,[air_week_date]
      ,[accum_revenue]
      ,[cum_spots]
      ,[gross_revenue]
      ,[booked_spots]
      ,[capacity]
      ,[bookings_fcst]
      ,[nielsen_programname]
      ,NULL 
      ,NULL 
      ,[nielsen_Daypart]
      ,[nielsen_Station]
      ,[nielsen_affiliation]
      ,[nielsen_air_week]
      ,[playbacktype]
      ,[sampletype]
      ,[ratingtype]
      ,[demo]
      ,[Avg_Rating]
      ,[avg_Share]
      ,[avg_Hutput]
      ,[week_to_air]
      ,[week_to_air_date]
      ,[weeks_to_peak]
      ,[peak_sales_flag]
      ,[disp_air_year]
      ,[revenue_forecast]
      ,[inventory_pacing]
      ,[revenue_pacing]
      ,[sellout_forecast]
	  ,tangent_line
from #temp_wo_nielsen_fcst_a a
where  air_year >= year(getdate()) and Ratingtype =''Forecasted''
;


BEGIN TRY DROP TABLE #temp_wo_nielsen_fcst_a  END TRY BEGIN CATCH END CATCH		;	

BEGIN TRY DROP TABLE #temp_wo_nielsen_int  END TRY BEGIN CATCH END CATCH		;




', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [price_calc_new]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'price_calc_new', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'use kantar;			
   go			
			
BEGIN TRY DROP TABLE #temp_kantar_nielsen END TRY BEGIN CATCH END CATCH		;	

   select a.aff_c , a.master_n , a.year, a.month, b.nielsen_dp , sum(a.revenue) as rev , sum(a.units) as units ,case when sum(a.units) > 0 then sum(a.revenue)/sum(a.units) else 0 end as spot_rate
   into #temp_kantar_nielsen
   from  [dbo].[kantar_actuals_fcst] a, [dbo].[dim_daypart_final]  b
   where   a.year >= Year(getdate())
	and  a.aff_c = b.aff_c
   and a.daypart_n = b.daypart_n
  group by a.aff_c , a.master_n ,  a.year, a.month, b.nielsen_dp
  order by a.aff_c , a.master_n ,  a.year, a.month, b.nielsen_dp;
 
  
  use Wide_Orbit;
  go



 BEGIN TRY DROP TABLE #temp_kantar_CPP END TRY BEGIN CATCH END CATCH		;			

 
select a.*, case when monthly_rating >= 0.2  then spot_rate/monthly_rating else spot_rate end as kantar_Cpp
 into #temp_kantar_CPP
from #temp_kantar_nielsen a,
   (select a.nielsen_affiliation, a.nielsen_station, a.wo_Station_name, a.wo_daypart_name, a.nielsen_daypart, wo_pgm_year, wo_pgm_month, Round(avg(avg_Rating),2) as monthly_rating from wo_nielsen_integration a
    where ((a.air_year = Year(getdate()) and a.air_week >= datepart(ISO_WEEK, getdate())) or ((a.air_year = Year(getdate())+1) and a.air_week < datepart(ISO_WEEK, getdate())))
	  group by a.nielsen_affiliation, a.nielsen_station,a.wo_Station_name, a.wo_daypart_name, a.nielsen_daypart,  wo_pgm_year, wo_pgm_month ) b
  where b.nielsen_affiliation = a.aff_c
  and b.nielsen_station=a.master_n
  and b.nielsen_daypart = a.nielsen_dp
  and b.wo_pgm_year = a.year
  and b.wo_pgm_month  =a.month;


 BEGIN TRY DROP TABLE #temp_integrated_ds END TRY BEGIN CATCH END CATCH		;	


  select a.*, b.aff_c, b.master_n, b.year, b.month, b.nielsen_dp, b.rev as revenue, b.units, b.spot_rate, b.kantar_Cpp			
 into #temp_integrated_ds 			
  from wo_nielsen_integration a left outer join #temp_kantar_CPP b			
  on( a.nielsen_affiliation = b.aff_c			
  and a.nielsen_station = b.master_n			
  and ((a.nielsen_daypart = b.nielsen_dp)		or ( a.nielsen_Daypart =''LN'' and b.nielsen_dp =''PR''))
  and ((a.air_year = Year(getdate()) and a.air_week >= datepart(ISO_WEEK, getdate())) or ((a.air_year = Year(getdate())+1) and a.air_week < datepart(ISO_WEEK, getdate())))
   and a.wo_pgm_year = b.year			
  and a.wo_pgm_month =b.month);	



 BEGIN TRY DROP TABLE #temp_tegna_quarter_ratings END TRY BEGIN CATCH END CATCH		;	

select wo_market, wo_affiliation,wo_Station_name, nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date) as quarter_num, round(avg(avg_Rating),2) avg_rating_q_tegna 
into #temp_tegna_quarter_ratings
 from  wo_nielsen_integration
where 1=1 
 and air_year >= Year(getdate())
and wo_affiliation = nielsen_affiliation
group by wo_market, wo_affiliation,wo_Station_name,nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date)
order by air_year, datepart(quarter, air_week_date);

/*BEGIN TRY DROP TABLE #temp_quarter_ratings_rank END TRY BEGIN CATCH END CATCH		;	
select * into #temp_quarter_ratings_rank
from
(select * , row_number() over(partition by wo_station_name, nielsen_daypart, wo_invcode_name ,air_year ,quarter_num order by  avg_rating_q_tegna desc) as rownum
from (
select wo_market, wo_affiliation,wo_Station_name, nielsen_affiliation, nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date) as quarter_num, round(avg(avg_Rating),2) avg_rating_q_tegna 
 from  wo_nielsen_integration
where 1=1 
 and air_year >= Year(getdate())
group by wo_market, wo_affiliation,wo_Station_name, nielsen_affiliation, nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date)
) a )b
where b.wo_affiliation =b.nielsen_affiliation
order by air_year ,quarter_num; */



--select *  from #temp_quarter_ratings_rank where wo_station_name =''WXIA''  and air_year =2018 and quarter_num =2


BEGIN TRY DROP TABLE #temp_all_quarter_ratings END TRY BEGIN CATCH END CATCH		;	

select wo_market, wo_affiliation,wo_Station_name, nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date) as quarter_num, round(avg(avg_Rating),2)  avg_rating_q_all
into #temp_all_quarter_ratings
 from  wo_nielsen_integration
where 1=1 
 
and air_year >= Year(getdate())
group by wo_market, wo_affiliation,wo_Station_name,nielsen_daypart, wo_invcode_name,air_year, datepart(quarter, air_week_date)
order by air_year, datepart(quarter, air_week_date)			
 				
use staging;			
go			
			

delete from price_guide_staging  where air_year = Year(air_week_date)  ;


insert into price_guide_staging			
select a.wo_market, a.wo_affiliation, a.wo_daypart_name,  a.wo_invcode_name, a.air_week_date , sum(a.weeks_to_peak) as weeks_to_peak,	
max(case when nielsen_affiliation =''NBC''   then (a.kantar_cpp) end) as NBC_CPP,			
max(case when nielsen_affiliation =''ABC''  then (a.kantar_cpp) end) as  ABC_CPP,			
max(case when nielsen_affiliation =''CBS''  then (a.kantar_cpp) end)  as CBS_CPP,			
max(case when nielsen_affiliation =''FOX''  then (a.kantar_cpp) end)  as FOX_CPP,			
avg(case when nielsen_affiliation =''NBC'' then a.avg_rating end ) as NBC_Rating,			
avg(case when nielsen_affiliation =''ABC'' then a.avg_rating end ) as ABC_Rating,			
avg(case when nielsen_affiliation =''CBS'' then a.avg_rating end ) as CBS_Rating,			
avg(case when nielsen_affiliation =''FOX'' then a.avg_rating end ) as FOX_Rating,			
case when wo_affiliation = nielsen_affiliation and avg(a.rating) >= 0.2 and max(cum_spots) > 0  then ((max(accum_revenue)/max(cum_spots))/avg(a.rating)) 			
     else 0 end as avg_CPP,		
case when wo_affiliation = nielsen_affiliation then max(accum_revenue) else 0 end as booked_revenue,			
case when wo_affiliation = nielsen_affiliation then max(cum_spots) else 0 end as sold,			
avg(capacity) as capacity,			
case when wo_affiliation = nielsen_affiliation then (avg(capacity) - sum(booked_spots)) else 0 end as Rem_Cap,			
case when wo_affiliation = nielsen_affiliation then max(bookings_fcst) else 0 end as Tot_Dem,			
case when wo_affiliation = nielsen_affiliation then (max(bookings_fcst) - sum(booked_spots)) else 0 end as Rem_Dem,			
case when wo_affiliation = nielsen_affiliation then (avg(capacity) - max(bookings_fcst)) else 0 end as Net_Cap,			
a.nielsen_Daypart,			
a.nielsen_Station,			
case when wo_affiliation = nielsen_affiliation and max(cum_spots) > 0  then (max(accum_revenue)/max(cum_spots)) else 0  end as wo_spot_rate,			
a.air_year,			
is_tegna_station,			
max(a.rating) as rating,			
  max(kantar_cpp) as kantar_cpp,			
  getdate() as peak_week_date,			
a.wo_station_name, 			
max(case when nielsen_affiliation =''NBC''   then (a.revenue) end) as NBC_rev,			
max(case when nielsen_affiliation =''ABC''  then (a.revenue) end) as  ABC_rev,			
max(case when nielsen_affiliation =''CBS''  then (a.revenue) end)  as CBS_rev,			
max(case when nielsen_affiliation =''FOX''  then (a.revenue) end)  as FOX_rev,			
max(case when nielsen_affiliation =''NBC''   then (a.units) end) as NBC_units,			
max(case when nielsen_affiliation =''ABC''  then (a.units) end) as  ABC_units,			
max(case when nielsen_affiliation =''CBS''  then (a.units) end)  as CBS_units,			
max(case when nielsen_affiliation =''FOX''  then (a.units) end)  as FOX_units	,
a.nielsen_affiliation
,a.invcode_external_id
,case when wo_affiliation = nielsen_affiliation then max(revenue_forecast) else 0 end as  rev_fcst
,0 as budget
from			
	(	select a.*,	round(avg_rating, 2) rating,
		case when (a.wo_affiliation =a.nielsen_affiliation) then 1 else 0 end as is_tegna_station	
		from #temp_integrated_ds a	
		
				
	 ) a	
where air_year = Year(air_week_date)	--and  wo_station_name =''WCSH''	
group by a.wo_market, a.wo_affiliation,a.wo_station_name, a.wo_daypart_name, nielsen_affiliation,a.nielsen_Station,a.nielsen_Daypart, a.wo_invcode_name, a.invcode_external_id, a.air_year, a.air_week_date,  is_tegna_station			
order by wo_invcode_name, air_week_date;			
			

			
BEGIN TRY DROP TABLE #temp_pricing_rules_base END TRY BEGIN CATCH END CATCH		;				
	
			
select a.wo_market, a.wo_affiliation,  a.wo_station_name, a.wo_daypart_name,  a.invcode_external_id, a.wo_invcode_name, a.air_week_date , max(a.weeks_to_peak) as weeks_to_peak, max(peak_week_date) as peak_weak_date,			
 a.nielsen_Daypart,	a.air_year,		
  max(case when is_tegna_station = 1  then avg_rating else 0 end ) as max_teg_rating,			
  max(case when is_tegna_station = 0  then avg_rating else 0 end ) as max_non_teg_rating,			
  max(case when is_tegna_station = 1  then kantar_cpp else 0 end ) as max_teg_k_cpp,			
  max(case when is_tegna_station = 0  then kantar_cpp else 0 end ) as max_non_teg_k_cpp,			
  max(case when air_year = Year(air_week_date) - 1  then sold 		    else 0 end) as LY_dem,
		   			
  max(case when  is_tegna_station = 1 and air_year = Year(air_week_date)-1  then wo_spot_rate else 0 end) as LY_spot_rate,		

  max(case when  is_tegna_station = 1  and air_year = Year(air_week_date) then wo_spot_rate   else 0 end) as CY_spot_rate,	

  max(case when  is_tegna_station = 1 and  air_year = Year(air_week_date)-1  then avg_rating  else 0 end) as LY_rating,

 max(case when  is_tegna_station = 1 and air_year = Year(air_week_date) then avg_rating else 0 end) as CY_rating,				
  
  max(case when  is_tegna_station = 1 and air_year = Year(air_week_date)-1  then avg_cpp  else 0 end) as LY_CPP,		

max(case when  is_tegna_station = 1 and air_year = Year(air_week_date) then avg_cpp else 0 end) as CY_CPP,				

max( tot_dem) CY_dem,
	
 max(capacity) as CY_cap,			
  max(capacity) - max(tot_dem) as CY_Net_cap,			
 case when ((wo_invcode_name like ''%News%'' )  or (invcode_external_id in (''LDT'', ''LEM'', ''LEN'', ''LLN'', ''LRE''))) then 1 else 0 end as is_news
into #temp_pricing_rules_base			
 from price_guide_staging a
 where air_year = Year(air_week_date)
 
 group by a.wo_market, a.wo_affiliation, a.wo_station_name, a.wo_daypart_name,  a.invcode_external_id, a.wo_invcode_name, 			
 a.air_week_date , a.nielsen_Daypart , a.air_year	
 order by air_year			;

BEGIN TRY DROP TABLE #temp_pricing_rules_base_ly END TRY BEGIN CATCH END CATCH		;	


 select a.wo_market, a.wo_affiliation,  a.wo_station_name, a.wo_daypart_name, a.invcode_external_id,  a.wo_invcode_name, a.air_week_date , max(a.weeks_to_peak) as weeks_to_peak, max(peak_week_date) as peak_weak_date,			
 a.nielsen_Daypart,	a.air_year,		
  max(case when is_tegna_station = 1  then avg_rating else 0 end ) as max_teg_rating,			
  max(case when is_tegna_station = 0  then avg_rating else 0 end ) as max_non_teg_rating,			
  max(case when is_tegna_station = 1  then kantar_cpp else 0 end ) as max_teg_k_cpp,			
  max(case when is_tegna_station = 0  then kantar_cpp else 0 end ) as max_non_teg_k_cpp,			
 -- max(case when air_year = Year(air_week_date) - 1  then sold 		    else 0 end) as LY_dem,
 max(sold) as LY_dem,
		   			
  max(case when  is_tegna_station = 1 and air_year = Year(air_week_date)-1  then wo_spot_rate else 0 end) as LY_spot_rate,		

  max(case when  is_tegna_station = 1  and air_year = Year(air_week_date) then wo_spot_rate   else 0 end) as CY_spot_rate,	

  max(case when  is_tegna_station = 1 and  air_year = Year(air_week_date)-1  then avg_rating  else 0 end) as LY_rating,

 max(case when  is_tegna_station = 1 and air_year = Year(air_week_date) then avg_rating else 0 end) as CY_rating,				
  
  max(case when  is_tegna_station = 1 and air_year = Year(air_week_date)-1  then avg_cpp  else 0 end) as LY_CPP,		

max(case when  is_tegna_station = 1 and air_year = Year(air_week_date) then avg_cpp else 0 end) as CY_CPP,				

max( tot_dem) CY_dem,
	
 max(capacity) as CY_cap,			
  max(capacity) - max(tot_dem) as CY_Net_cap,			
 case when ((wo_invcode_name like ''%News%'' )  or (invcode_external_id in (''LDT'', ''LEM'', ''LEN'', ''LLN'', ''LRE''))) then 1 else 0 end as is_news
into #temp_pricing_rules_base_ly		
 from price_guide_staging a
 where air_year = Year(air_week_date) -1  

 group by a.wo_market, a.wo_affiliation, a.wo_station_name, a.wo_daypart_name, a.invcode_external_id, a.wo_invcode_name, 			
 a.air_week_date , a.nielsen_Daypart , a.air_year	
 order by air_year		;

BEGIN TRY DROP TABLE #temp_pricing_rules END TRY BEGIN CATCH END CATCH		;

select a.* , b.ly_dem  as last_year_dem , b.LY_spot_rate as last_year_spotrate, b.LY_rating as last_year_rating, b.LY_CPP as last_year_CPP into #temp_pricing_rules
 from #temp_pricing_rules_base a left outer join #temp_pricing_rules_base_ly b
on(  a.wo_station_name = b.wo_station_name
and a.air_week_date =b.air_week_date
and a.wo_invcode_name = b.wo_invcode_name
and a.air_year = Year(a.air_week_date) 
and b.air_year = Year(a.air_week_date)  -1 );


BEGIN TRY DROP TABLE #temp_price_rule3 END TRY BEGIN CATCH END CATCH		;


select  wo_market, wo_Station_name, nielsen_daypart,wo_invcode_name,  avg( case when cy_spot_rate > 0 then cy_spot_rate else last_year_spotrate end )  as rule_3_price  
into #temp_price_rule3
from #temp_pricing_rules
where air_year = Year(Getdate()) 
group by wo_market, wo_Station_name, nielsen_daypart,wo_invcode_name;	

/*
BEGIN TRY DROP TABLE #temp_price_rule4 END TRY BEGIN CATCH END CATCH		;


select  wo_market, wo_Station_name, nielsen_daypart,wo_invcode_name,air_year,  air_week_date,
 avg(nullif(rev,0)) over (partition by wo_market, wo_Station_name,  nielsen_daypart,wo_invcode_name
 order by  wo_market, wo_Station_name,  nielsen_daypart,wo_invcode_name, air_year,air_week_date   rows between 13 preceding and current row)  as rule_4_price
 into #temp_price_rule4
from 
(select  wo_market, wo_Station_name, nielsen_daypart,wo_invcode_name, air_year, air_week_date,case when sum(sold) > 0 then sum(booked_revenue)/sum(sold) else 0 end as rev
--into #temp_price_rule4
from price_guide_staging
where 1=1
and air_year = year(air_week_date)
and wo_affiliation =nielsen_affiliation
group by wo_market, wo_Station_name, nielsen_daypart,wo_invcode_name, air_year, air_week_date

 ) x 

 */

 /* Base Price Revision - As of Jan 2018 -- Start */

 
use Wide_Orbit;
Go	

BEGIN TRY DROP TABLE #temp_cap END TRY BEGIN CATCH END CATCH		;

 Select distinct market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week , DATEADD(wk, DATEDIFF(wk,0,[Air Date]), 0) as air_week_date
  into #temp_cap
FROM fact_inv_capacity_dv 
where 1=1 
---and Station =''WFAA''
and air_year =2018
and break_code in (''CM'')
and [Potential Units] > 0
and Daypart not in (''SP'', ''MISC'', ''WK'')
and  [Inventory Code] not like ''%BB%''
and [Inventory Code] not like ''%CM%''
and [Inventory Code] not like ''%Spon%''
and [Inventory Code] not like ''%Spn%''
and [Inventory Code] not like ''%Fixed%''
and [Inventory Code] not like ''%ROS%''
and [Inventory Code] not like ''%*%''
order by market, Station, [Station Affiliation], Daypart, [Inventory Code], Air_year, Air_week 
;



/* Fetch  all the spot orders from fact spot rating table using the new invcode for air_year < 2018 and then  calculate EUR*/

BEGIN TRY DROP TABLE #temp_fact_spot END TRY BEGIN CATCH END CATCH		;	

select market,[Station Call Letters], new_daypart, new_invcode,air_year,  air_week, DATEADD(wk, DATEDIFF(wk,0,full_date), 0) as air_week_date,  full_date, case when sum(booked_spots) >0  then coalesce(sum([Gross Revenue])/sum(booked_spots),0) else 0 end as eur
into #temp_fact_spot
 from fact_spot_rating_dv where 1=1 
 --and market =''Dallas'' and  [Station Call Letters] =''WFAA'' 
and [Daypart (placed)] not in (''SP'', ''MISC'', ''WK'')
and new_invcode not like ''%BB%''
and new_invcode not like ''%CM%''
and new_invcode not like ''%Spon%''
and new_invcode not like ''%Spn%''
and new_invcode not like ''%Fixed%''
and new_invcode not like ''%ROS%''
and new_invcode not like ''%*%''
and  agency  not like ''%Carat - General Motors%'' 
and  agency  not like ''%Carat USA - General Motors%'' 
and  advertiser  not like ''%Nebraska Funiture Mart%'' 
 and  agency  not  like ''%Rooms to go%'' 
 -- and Air_year < 2018
group by market,[Station Call Letters], new_daypart, new_invcode,air_year,  air_week,DATEADD(wk, DATEDIFF(wk,0,full_date), 0) ,full_date ;



/* Get the Mean EUR and 1.5*STD_DEV (EUR)  and ignore any data which has eur < mean_eur -1.5*stddev */ 
/*Then aggregate the data to air_week and then calculate 5_year_weekly_avg_eur*/

BEGIN TRY DROP TABLE #temp_historic_avg_w END TRY BEGIN CATCH END CATCH		;	

select c.market,c.[Station Call Letters] as station_call_letters, c.new_daypart as daypart_name, c.new_invcode as invcode_name,c.air_week ,  avg(mean_eur) as mean_eur, avg(c.avg_eur_2) as five_year_avg_eur_2, avg(eur_perc_increase)  as eur_perc_increase
into #temp_historic_avg_w
from
(
select  b.market,b.[Station Call Letters], b.new_daypart, b.new_invcode,b.air_year, b.air_week, b.air_week_date,  avg(a.avg_eur)  as mean_eur, avg(b.eur) as avg_eur_2 , (avg(b.eur) - avg(a.avg_eur))/ avg(b.eur) as eur_perc_increase 
from #temp_fact_spot b,
(select market,[Station Call Letters], new_daypart, new_invcode,air_year,  air_week, air_week_date , coalesce(avg(z.eur),0) as avg_eur,  coalesce(1.5*STDEV(eur), 0) as stdeur_lower , coalesce(2*STDEV(eur), 0) as stdeur_upper
from (
select * from #temp_fact_spot
 where  eur > 0 ) z
group by  market,[Station Call Letters], new_daypart, new_invcode,air_year,  air_week , air_week_date) a
where b.market = a.market and  b.[Station Call Letters] = a.[Station Call Letters] and b.new_daypart =a.new_daypart
and b.new_invcode =a.new_invcode and b.air_year =a.air_year and b.air_week = a.air_week 
and ((b.eur >= (avg_eur - stdeur_lower)) or (b.eur < avg_eur + stdeur_upper))
and b.eur > 0
group by b.market,b.[Station Call Letters], b.new_daypart, b.new_invcode,b.air_year, b.air_week , b.air_week_date) c
group by c.market,c.[Station Call Letters], c.new_daypart, c.new_invcode,c.air_week 
order by c.market,c.[Station Call Letters], c.new_daypart, c.new_invcode,c.air_week ;



BEGIN TRY DROP TABLE #temp_historic_avg_wk END TRY BEGIN CATCH END CATCH		;	


select a.market, a.Station as station_call_letters, a.Daypart as daypart_name , a.[Inventory Code] as invcode_name , a.Air_week , a.air_week_date , b.mean_eur, b.five_year_avg_eur_2 , b.eur_perc_increase 
into #temp_historic_avg_wk
from #temp_cap a left outer join #temp_historic_avg_w  b
on (a.Station = b.station_call_letters and a.Daypart =b.daypart_name and a.[Inventory Code] =b.invcode_name  and a.Air_week =b.Air_week );



BEGIN TRY DROP TABLE #missing_codes END TRY BEGIN CATCH END CATCH		;	

select * into #missing_codes 
from  #temp_historic_avg_wk  a
where a.five_year_avg_eur_2 is null; 


BEGIN TRY DROP TABLE #temp_week_diff END TRY BEGIN CATCH END CATCH		;	


select a.*, b.Air_week as missing_wk, (b.Air_week - a.air_week) as wk_diff 
into #temp_week_diff
from  #temp_historic_avg_wk  a , #missing_codes b 
where  1=1
and a.station_call_letters =b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name 
and datepart(qq, a.air_week_date) =  datepart(qq, b.air_week_date) 
and  a.five_year_avg_eur_2 is not null;

BEGIN TRY DROP TABLE #min_week END TRY BEGIN CATCH END CATCH		;

select a.market  ,a.station_call_letters, a.daypart_name, a.invcode_name,   missing_wk,  min(wk_diff) as min_wk_diff
into #min_week
from 
#temp_week_diff a
 where wk_diff > 0
group by a.market  ,a.station_call_letters, a.daypart_name, a.invcode_name,  missing_wk;

BEGIN TRY DROP TABLE #max_week END TRY BEGIN CATCH END CATCH		;

select a.market  ,a.station_call_letters, a.daypart_name, a.invcode_name,   missing_wk,  max(wk_diff) as max_wk_diff
into #max_week
from 
#temp_week_diff a
 where wk_diff < 0
group by a.market  ,a.station_call_letters, a.daypart_name, a.invcode_name,  missing_wk;


BEGIN TRY DROP TABLE #temp_min_week_eur END TRY BEGIN CATCH END CATCH		;

select a.* , coalesce(b.mean_eur,0) mean_eur , coalesce(b.five_year_avg_eur_2,0) five_year_avg_eur_2 , coalesce(b.eur_perc_increase,0) eur_perc_increase  into #temp_min_week_eur from    #min_week a left outer join #temp_week_diff b 
 on (a.market =b.market and a.station_call_letters = b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name and  a.min_wk_diff =b.wk_diff and a.missing_wk =b.missing_wk);


 update #temp_historic_avg_wk
 set mean_eur =b.mean_eur
 , five_year_avg_eur_2 =b.five_year_avg_eur_2
 ,eur_perc_increase =b.eur_perc_increase
 from #temp_historic_avg_wk a , #temp_min_week_eur b
 where a.market =b.market and a.station_call_letters = b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name and a.Air_week =b.missing_wk and b.five_year_avg_eur_2 > 0 and a.five_year_avg_eur_2 is null


BEGIN TRY DROP TABLE #temp_max_week_eur END TRY BEGIN CATCH END CATCH		;


select a.* , coalesce(b.mean_eur,0) mean_eur , coalesce(b.five_year_avg_eur_2,0) five_year_avg_eur_2 , coalesce(b.eur_perc_increase,0) eur_perc_increase  into #temp_max_week_eur from    #max_week a left outer join #temp_week_diff b 
 on (a.market =b.market and a.station_call_letters = b.station_call_letters and a.invcode_name =b.invcode_name and  a.max_wk_diff =b.wk_diff and a.missing_wk =b.missing_wk);


 update #temp_historic_avg_wk
 set mean_eur =b.mean_eur
 , five_year_avg_eur_2 =b.five_year_avg_eur_2
 ,eur_perc_increase =b.eur_perc_increase
 from #temp_historic_avg_wk a , #temp_max_week_eur b
 where a.market =b.market and a.station_call_letters = b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name and a.Air_week =b.missing_wk and b.five_year_avg_eur_2 > 0 and a.five_year_avg_eur_2 is null;

 BEGIN TRY DROP TABLE #temp_historic_avg_byname END TRY BEGIN CATCH END CATCH		;	

select c.market,c.station_call_letters, c.daypart_name, c.invcode_name , coalesce(avg(five_year_avg_eur_2),0) as byname_avg_eur ,  coalesce(avg(mean_eur),0) as byname_mean_eur ,  coalesce(avg(eur_perc_increase),0) as byname_eur_perc_increase
into #temp_historic_avg_byname
from #temp_historic_avg_wk c
group by c.market,c.station_call_letters, c.daypart_name, c.invcode_name;


 update #temp_historic_avg_wk
 set mean_eur =b.byname_mean_eur
 , five_year_avg_eur_2 =b.byname_avg_eur
 ,eur_perc_increase =b.byname_eur_perc_increase
 from #temp_historic_avg_wk a , #temp_historic_avg_byname b
 where a.market =b.market and a.station_call_letters = b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name  and a.five_year_avg_eur_2 is null;


/* Calculate the quarterly avg_eur for each inventory code from the 5_year_weekly_avg_eur from above table*/

BEGIN TRY DROP TABLE #temp_historic_avg_q END TRY BEGIN CATCH END CATCH		;	

select c.market,c.station_call_letters, c.daypart_name, c.invcode_name , datepart(qq, air_week_date) as air_q, coalesce(avg(five_year_avg_eur_2),0) as quarter_avg_eur
into #temp_historic_avg_q
from #temp_historic_avg_wk c
group by c.market,c.station_call_letters, c.daypart_name, c.invcode_name,datepart(qq, air_week_date);

/*Calculate the coefficient by comparing the weekly_avg_eur to quarterly EUR for each inventory code */

BEGIN TRY DROP TABLE #temp_coeff END TRY BEGIN CATCH END CATCH		;	

select a.market, a.station_call_letters, a.daypart_name, a.invcode_name, a.air_week,  coalesce(avg(a. five_year_avg_eur_2),0) as five_year_avg_eur_2 , avg(b.quarter_avg_eur) as quarter_avg_eur, 
avg(case when (quarter_avg_eur) > 0 then (a.five_year_avg_eur_2/b.quarter_avg_eur) else 0  end) as coeff
into #temp_coeff
 from #temp_historic_avg_wk a left outer join #temp_historic_avg_q b
on (a.station_call_letters =b.station_call_letters and a.daypart_name =b.daypart_name and a.invcode_name =b.invcode_name and datepart(QQ, a.air_week_date) =b.air_q )
group by a.market, a.station_call_letters, a.daypart_name, a.invcode_name, a.air_week;


/* Get the 2018 Forecast/Actual data and store the current EUR only if sellthrough >= 15% */

BEGIN TRY DROP TABLE #temp_2018_avg_w END TRY BEGIN CATCH END CATCH		;	

select a.* , case when a.sell_through  >= 0.15 then coalesce(EUR,0) else 0 end as EUR_2018 , coalesce(EUR,0) EUR_all
into #temp_2018_avg_w
from
(select wo_market,wo_Station_name, wo_daypart_name, wo_invcode_name as Inventory_code ,air_week_date as air_week ,air_week as air_week_num, case when max(cum_spots) > 0 then  Round((max(Accum_revenue)/max(cum_spots)) /5, 0)*5 else 0 end as eur , 
case when avg(capacity) > 0 then max(cum_spots)/avg(capacity) else 0  end as sell_through
 from wo_nielsen_integration where 1=1 
--and   wo_Station_name =''WWL'' 
and wo_invcode_name not like ''% BB%''
and wo_invcode_name not like ''% CM%''
and wo_invcode_name not like ''% Spon%''
and wo_invcode_name not like ''% Spn%''
and wo_invcode_name not like ''% Fixed%''
and wo_invcode_name not like ''%*%''
and air_year =2018
and wo_daypart_name not in (''SP'', ''MISC'' ,''WK'')
and wo_affiliation = nielsen_affiliation
 group by  wo_market,wo_Station_name, wo_daypart_name, wo_invcode_name ,air_week_date,air_week
 ) a
order by wo_market,wo_Station_name, wo_daypart_name, Inventory_code,air_week;


/* Get the quarterly avg_EUR for inventory code in  2018 and also calculate the number of weeks this program has made 25% sellthrough in that quarter */

BEGIN TRY DROP TABLE #temp_2018_avg_q END TRY BEGIN CATCH END CATCH		;	

select a.wo_Station_name, a.wo_daypart_name, a.Inventory_code, datepart(qq,a.Air_week) as air_q, coalesce(avg(a.EUR_2018),0) as avg_eur_2018 , count(air_week_num) as week_counter , STDEV(EUR_2018) as stddev_eur
into #temp_2018_avg_q
from #temp_2018_avg_w a where a.EUR_2018 > 0 
group by  a.wo_Station_name, a.wo_daypart_name, a.Inventory_code, datepart(qq,a.Air_week) 
order by a.wo_Station_name, a.wo_daypart_name, a.Inventory_code, datepart(qq,a.Air_week);





BEGIN TRY DROP TABLE #temp_2018_avg_byname END TRY BEGIN CATCH END CATCH		;	

select a.wo_Station_name, a.wo_daypart_name, a.Inventory_code,  coalesce(avg(a.EUR_2018),0) as byname_eur_2018 
into #temp_2018_avg_byname
from #temp_2018_avg_w a where a.EUR_2018 > 0
group by  a.wo_Station_name, a.wo_daypart_name, a.Inventory_code
order by a.wo_Station_name, a.wo_daypart_name, a.Inventory_code;


/* join the above two tables for 2018*/

BEGIN TRY DROP TABLE #temp_2018_avg END TRY BEGIN CATCH END CATCH		;	

select a.*, datepart(qq, a.air_week) air_q, coalesce(b.avg_eur_2018 ,0) avg_eur_2018 ,coalesce(b.week_counter,0) week_counter , coalesce(c.byname_eur_2018,0) as byname_eur_2018 , coalesce(b.stddev_eur,0) as stddev_eur
into #temp_2018_avg
from #temp_2018_avg_w  a left outer join #temp_2018_avg_q b 
on (a.wo_Station_name =b.wo_Station_name and a.wo_daypart_name =b.wo_daypart_name and a.Inventory_code =b.Inventory_code and datepart(QQ,air_week) =b.air_q )
left outer join #temp_2018_avg_byname c on (
 a.wo_Station_name =c.wo_Station_name and a.wo_daypart_name =c.wo_daypart_name and a.Inventory_code =c.Inventory_code );

/* Derive the base price using below criteria*/
/* Step 1: if current_week_EUR for inventory code is > 0  and this program has made 15% sellthrough for more than 7 times a quarter then use the current EUR*/
/*Step 2: if current_week_eur  is 0 but has made 15% sellthrough for more than 7 times a quarter then do this calc : historic_coefficient_for the_week * current _quarterly_eur*/
/* Step 3: For the remaining, use historic_5_year_avg_eur*/

BEGIN TRY DROP TABLE #temp_base_price_revised END TRY BEGIN CATCH END CATCH		;	

 select a.* , b.five_year_avg_eur_2 , b.quarter_avg_eur, b.coeff
, case when coalesce(a.EUR_2018,0) > 0 then coalesce(a.eur_2018,0)
	   when (a.EUR_2018 = 0 and a.week_counter >= 6 and a.avg_eur_2018 > 0 and b.coeff is not null   ) then coalesce (b.coeff*a.avg_eur_2018  ,0)
       when (a.EUR_2018 = 0  and a.avg_eur_2018 = 0 and coalesce(five_year_avg_eur_2,0) > 0 ) then coalesce(b.five_year_avg_eur_2,0)
	   else coalesce(a.byname_eur_2018,0) end as base_price_final 
, case when coalesce(a.EUR_2018,0) > 0 then ''C''
	   when (a.EUR_2018 = 0 and a.week_counter >= 6 and a.avg_eur_2018 > 0 and b.coeff is not null ) then ''C''
       when (a.EUR_2018 = 0  and a.avg_eur_2018 = 0 and coalesce(five_year_avg_eur_2,0) > 0 ) then ''H''
	   else ''C'' end as base_price_ind 
into #temp_base_price_revised
  from #temp_2018_avg a left outer join #temp_coeff b
 on (a.wo_Station_name =b.station_call_letters and a.Air_week_num =b.air_week and a.Inventory_code =b.invcode_name)
     order by a.wo_market, a.wo_Station_name, a.wo_daypart_name, a.Inventory_code,a.air_week;
	 
-- added as part of Alek''s comments

update #temp_base_price_revised
set base_price_final = EUR_all
where( sell_through < 0.15 and  EUR_all > base_price_final and EUR_all <=  (avg_eur_2018+stddev_eur))


--delete from base_price_revised;

--insert into base_price_revised
--select * , cast(getdate() as date)   from  #temp_base_price_revised ;
 
 
/* Base Price Revision - As of Jan 2018 End */





use Wide_Orbit;
Go

delete from price_grid_staging where air_year >= Year(getdate())   ;
insert into  price_grid_staging 			
 select d.wo_market, d.wo_affiliation, d.wo_daypart_name, wo_invcode_name, d.nielsen_daypart, d.air_week_date, d.booked_revenue, d.avg_CPP, d.capcity, 			
  d.sold,   d.Rem_cap,  d.Tot_dem, d.Rem_dem, d.Net_Cap, d.peak_sales_flag , d.weeks_to_Peak,d.peak_week_date, d.Expected_Weekly, d.NBC_Rating ,			
  d.FOX_Rating , d.ABC_Rating , d.CBS_Rating ,			
  d.NBC_CPP_RATIO , d.FOX_CPP_RATIO , d.ABC_CPP_RATIO , d.CBS_CPP_RATIO ,			
  d.cy_dem, d.LY_dem, d.CY_Net_Cap, d.ly_spot_rate, d.ly_cpp,	
  case when d.max_teg_rating >= max_non_teg_rating then 1 else 0 end as Rule_1,			
  -- Rule 2 change -- 08/09/2017-- 
  --case when( d.max_teg_rating > max_non_teg_rating and d.is_news = 1) or ((d.max_teg_rating <= d.max_non_teg_rating) and ((d.max_non_teg_rating - d.max_teg_rating) < (0.1*d.max_teg_rating)) )
  -- then 1 else 0 end as Rule_2,
  case when d.is_news = 1   then 1 else 0 end as Rule_2,		
  -- Rule 3 change -- 08/09/2017-- 	
  --case when (d.max_teg_rating <= d.max_non_teg_rating) and ((d.max_non_teg_rating - d.max_teg_rating) < (0.1*d.max_teg_rating)) then 1 else 0 end as Rule_3,	
  case when (d.max_teg_rating < max_non_teg_rating) and (d.avg_rating_q_tegna > d.avg_rating_q_all - (0.20*d.avg_rating_q_all)) then 1 else 0 end as Rule_3,
  case when d.max_teg_k_cpp < d.max_non_teg_k_cpp then 1 else 0 end as Rule_4,			
  case when (d.max_teg_k_cpp >= d.max_non_teg_k_cpp) and (d.max_teg_k_cpp - d.max_non_teg_k_cpp) < (0.15 * d.max_teg_k_cpp ) then 1 else 0 end as Rule_5,	
  -- Rule 6 change -- 08/09/2017-- 		
  --case when (d.CY_Net_cap < 0) and (abs(d.CY_Net_cap) <= (0.15* d.CY_cap)) then 1 else 0 end as Rule_6,		
--Back to normal -- 04/06/2018 --	
  case when (d.Tot_dem > d.capcity) and (abs(d.CY_Net_cap) <= (0.15* d.CY_cap)) then 1 else 0 end as Rule_6,			
  case when (d.CY_Net_cap < 0) and (abs(d.CY_Net_cap) > (0.15* d.CY_cap)) then 1 else 0 end as Rule_7,			
  case when (d.CY_Net_cap >= 0) and d.CY_dem > d.LY_dem then 1 else 0 end as Rule_8,			
  case when d.weeks_to_Peak > 1 then 1 else 0 end as Rule_9,			
    case when (d.avg_cpp = 0 and delta_fcst > 1 )then delta_fcst * LY_CPP 			
		when (d.avg_cpp = 0 and delta_fcst <= 1 )then  LY_CPP 	
  else d.avg_cpp end as avg_cpp_new	

  , d.wo_station_name			
  ,d.NBC_rev			
  ,d.ABC_rev			
  ,d.CBS_rev			
  ,d.FOX_rev			
  ,d.NBC_units			
  ,d.ABC_units			
  ,d.CBS_units			
  ,d.FOX_units		
  ,d.base_price_final as cy_spot_rate
  ,d.air_year
, d.invcode_external_id
--New Rule 10 -- 08/09/2017 
 /*, case when (d.max_teg_rating < max_non_teg_rating) and (d.avg_rating_q_tegna < d.avg_rating_q_all - (0.20*d.avg_rating_q_all)) 
  and (d.avg_rating_q_tegna >= d.avg_rating_q_all - (0.40*d.avg_rating_q_all)) or rownum <=3)     then 1 else 0 end as Rule_10 */

  , case when (d.max_teg_rating < max_non_teg_rating) and (d.avg_rating_q_tegna < d.avg_rating_q_all - (0.20*d.avg_rating_q_all)) 
  and (d.avg_rating_q_tegna >= d.avg_rating_q_all - (0.40*d.avg_rating_q_all))  then 1 else 0 end as Rule_10

  ,d.rule_3_price
 -- ,d.rule_4_price
,d.revenue_forecast as rev_fcst
,budget_inv_weekly as budget


   from			
 (select c.*,  r.CY_cap, r.CY_dem, r.CY_Net_cap, r.is_news, r.last_year_dem as ly_dem, r.max_non_teg_k_cpp, r.max_non_teg_rating, r.max_teg_k_cpp, r.max_teg_rating,r.last_year_spotrate as LY_spot_rate , r.last_year_cpp as LY_CPP 			
 , case when c.avg_CPP = 0 and CY_rating > 0  then  (Last_year_rating / CY_rating)  			
		when c.avg_CPP = 0 and CY_rating = 0 then  0 end as delta_fcst	
	, r.Last_year_rating as ly_rating, CY_rating	, r.cy_spot_rate	, e.avg_rating_q_tegna,e.quarter_num , f.avg_rating_q_all, g.rule_3_price , z.base_price_final , z.base_price_ind,  y.budget_inv_weekly 
	--,x.rownum
			
 from			
 (			
 select b.*  , case when weeks_remaining > 0 then (Rem_dem/weeks_remaining) else 0 end as Expected_Weekly			
, case	 when b.wo_affiliation = ''NBC'' and NBC_CPP >0  then (NBC_CPP/NBC_CPP)*100 		
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (NBC_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (NBC_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (NBC_CPP/FOX_CPP)*100 END AS NBC_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (ABC_CPP/NBC_CPP)*100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (ABC_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (ABC_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (ABC_CPP/FOX_CPP)*100 END AS ABC_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (CBS_CPP/NBC_CPP) * 100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (CBS_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (CBS_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (CBS_CPP/FOX_CPP)*100 END AS CBS_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (FOX_CPP/NBC_CPP) *100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (FOX_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (FOX_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (FOX_CPP/FOX_CPP)*100 END AS FOX_CPP_RATIO	
,sum([sold]) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
  -- ,	[air_week_date]		
     order by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
   ,	[air_week_date]		
	  rows unbounded preceding) as cum_spots_sold		
,sum([booked_revenue]) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
 --  ,	[air_week_date]		
     order by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
   ,	[air_week_date]		
	  rows unbounded preceding) as cum_rev,		
PERCENTILE_CONT(0.55) within group (order by sold) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]) as sold_zero,			
PERCENTILE_CONT(0.55) within group (order by booked_revenue) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]) rev_zero			
			
from			
(select a.* , case when weeks_to_Peak > 0 then ''Y'' else ''N'' end as peak_sales_flag,			
datediff(WK, Getdate() , air_week_date) as weeks_remaining			
			
from			
(SELECT [wo_market]			
      ,[wo_affiliation]			
	  ,[wo_station_name]		
      ,[wo_daypart_name]			
	  ,[nielsen_daypart]		
	-- ,[nielsen_station]		
      ,[wo_invcode_name]	
,invcode_external_id		
      ,[air_week_date]			
	  ,[air_year]		
	  ,max([peak_week_date]) as peak_week_date		
      ,max([weeks_to_peak]) as weeks_to_Peak			
	  ,avg(NBC_CPP) as NBC_CPP		
	  ,avg(ABC_CPP) as ABC_CPP		
	  ,avg(CBS_CPP) as CBS_CPP		
	  ,avg(FOX_CPP) as FOX_CPP		
	  ,avg(NBC_rev) as NBC_rev		
	  ,avg(ABC_rev) as ABC_rev		
	  ,avg(CBS_rev) as CBS_rev		
	  ,avg(FOX_rev) as FOX_rev		
	  ,avg(NBC_units) as NBC_units		
	  ,avg(ABC_units) as ABC_units		
	  ,avg(CBS_units) as CBS_units		
	  ,avg(FOX_units) as FOX_units		
	  ,AVG(NBC_Rating) as NBC_Rating		
	  ,AVG(ABC_Rating) as ABC_Rating		
	  ,AVG(CBS_Rating) as CBS_Rating		
	  ,AVG(FOX_Rating) as FOX_Rating		
	  ,max(avg_cpp) as avg_CPP		
	  ,max(booked_revenue) as booked_revenue		
	  ,max(sold) as sold		
	  ,avg(capacity) as capcity		
	  ,avg(capacity) - max(sold) as Rem_cap		
	  ,Max(Tot_Dem) as Tot_dem		
	  ,Max(Tot_Dem) - Max(sold) as Rem_dem		
	  ,avg(capacity)  - Max(Tot_Dem) as Net_Cap		
	  ,max(revenue_forecast) as revenue_forecast
	   FROM [Staging].[dbo].[price_guide_staging]		
	where air_year >= Year(getdate()) --and wo_station_name =''WFAA'' -- and wo_invcode_name =''Today Show 3'' and air_week_date =''06/18/2018''
   group by [wo_market]			
      ,[wo_affiliation]			
	  ,wo_station_name		
      ,[wo_daypart_name]			

	  ,[nielsen_daypart]		
      ,[wo_invcode_name]	
,invcode_external_id		
      ,[air_week_date]			
	  ,[air_year]		
	
 ) a			
	   ) b  ) c left outer join #temp_pricing_rules r		
	   on (c.wo_market = r.wo_market	
	   and c.wo_affiliation = r.wo_affiliation    		
	   and c.wo_daypart_name = r.wo_daypart_name		
	   and c.wo_invcode_name = r.wo_invcode_name		
	   and c.air_week_date = r.air_week_date
	   and c.air_year =  r.air_year) 
	   left outer join #temp_price_rule3 g
	   on (c.wo_market =g.wo_market 
	   and c.wo_station_name = g.wo_station_name
	   and c.nielsen_daypart = g.nielsen_daypart
	   and c.wo_invcode_name = g.wo_invcode_name)
	left outer join #temp_tegna_quarter_ratings e on (c.wo_market = e.wo_market	
	  	   and c.wo_affiliation = e.wo_affiliation    		
	   and c.nielsen_daypart = e.nielsen_daypart		
	   and c.wo_invcode_name = e.wo_invcode_name		
	   and c.air_year =  e.air_year
	   and datepart(quarter, c.air_week_date)= e.quarter_num)
	   left outer join #temp_all_quarter_ratings f on (c.wo_market = f.wo_market	
		   and c.wo_affiliation = f.wo_affiliation    		
	   and c.nielsen_daypart = f.nielsen_daypart		
	   and c.wo_invcode_name = f.wo_invcode_name		
	   and c.air_year =  f.air_year
	   and datepart(quarter, c.air_week_date)= f.quarter_num)
	    left outer join #temp_base_price_revised z on (c.wo_market = z.wo_market	
		--   and c.wo_affiliation = z.wo_affiliation    		
	--   and c.nielsen_daypart = z.nielsen_daypart		
	   and c.wo_invcode_name = z.Inventory_code		
	   and c.air_year =  year(z.air_week)
	   and c.air_week_date= z.air_week)
	left outer join budget_invcode_weekly y on (c.wo_station_name = y.station 
	and c.nielsen_daypart = y.nielsen_Daypart
	and c.wo_invcode_name =y.wo_invcode_name
	and c.air_year = y.air_year
	and c.air_week_date = y.air_week_date)
	/*left outer join #temp_quarter_ratings_rank x on (c.wo_market = x.wo_market 
	and   c.nielsen_daypart = x.nielsen_Daypart
	and c.wo_invcode_name =x.wo_invcode_name
	and c.air_year = x.air_year
	and datepart(quarter, c.air_week_date)= x.quarter_num) */
	 ) d 	;





	insert into [Wide_Orbit].[dbo].[price_analysis_backup] 
	select d.wo_market, d.wo_affiliation, d.wo_daypart_name, wo_invcode_name, d.nielsen_daypart, d.air_week_date, d.booked_revenue, d.avg_CPP, d.capcity, 			
  d.sold,   d.Rem_cap,  d.Tot_dem, d.Rem_dem, d.Net_Cap, d.peak_sales_flag , d.weeks_to_Peak,d.peak_week_date, d.Expected_Weekly, d.NBC_Rating ,			
  d.FOX_Rating , d.ABC_Rating , d.CBS_Rating ,			
  d.NBC_CPP_RATIO , d.FOX_CPP_RATIO , d.ABC_CPP_RATIO , d.CBS_CPP_RATIO ,			
  d.cy_dem, d.LY_dem, d.CY_Net_Cap, d.ly_spot_rate, d.ly_cpp
	  ,d.max_teg_rating as [rule1_tegna_max_rating]
      ,max_non_teg_rating as [rule1_non_tegna_max_rating]
	  ,case when d.max_teg_rating >= max_non_teg_rating then 1 else 0 end as Rule_1
	  ,[wo_invcode_name] as [rule2_is_news_check1]
	  ,d.invcode_external_id as [rule2_is_news_check2]
	  		
  -- Rule 2 change -- 08/09/2017-- 
  --case when( d.max_teg_rating > max_non_teg_rating and d.is_news = 1) or ((d.max_teg_rating <= d.max_non_teg_rating) and ((d.max_non_teg_rating - d.max_teg_rating) < (0.1*d.max_teg_rating)) )
  -- then 1 else 0 end as Rule_2,
	 ,case when d.is_news = 1   then 1 else 0 end as Rule_2	
  -- Rule 3 change -- 08/09/2017-- 	
     ,d.avg_rating_q_tegna as [rule3_quarterly_avg_tegna_rating]
     ,d.avg_rating_q_all as [rule3_quarterly_avg_all_rating]
     ,d.avg_rating_q_all - (0.20*d.avg_rating_q_all) as [rule3_within20perc_toprating]

     ,case when (d.max_teg_rating < max_non_teg_rating) and (d.avg_rating_q_tegna > d.avg_rating_q_all - (0.20*d.avg_rating_q_all)) then 1 else 0 end as Rule_3
	 ,d.max_teg_k_cpp as [rule4_tegna_max_cpp_kantar]
     ,d.max_non_teg_k_cpp as [rule4_nontegna_max_cpp_kantar]
     ,case when d.max_teg_k_cpp < d.max_non_teg_k_cpp then 1 else 0 end as Rule_4
	 ,(0.15 * d.max_teg_k_cpp ) as [rule5_within15perc_topKantar]
     ,case when (d.max_teg_k_cpp >= d.max_non_teg_k_cpp) and (d.max_teg_k_cpp - d.max_non_teg_k_cpp) < (0.15 * d.max_teg_k_cpp ) then 1 else 0 end as Rule_5
	 -- Rule 6 change -- 08/09/2017-- 		
	--case when (d.CY_Net_cap < 0) and (abs(d.CY_Net_cap) <= (0.15* d.CY_cap)) then 1 else 0 end as Rule_6,		
	--Back to normal -- 04/06/2018 --

     ,d.Tot_dem as [rule6_demand]
     ,d.capcity as [rule6_capacity]
     ,d.CY_Net_cap as [rule6_net_capacity]
     ,(0.15* d.CY_cap) as [rule6_15perc_capacity]	
	 ,case when (d.Tot_dem > d.capcity) and (abs(d.CY_Net_cap) <= (0.15* d.CY_cap)) then 1 else 0 end as Rule_6			
     ,case when (d.CY_Net_cap < 0) and (abs(d.CY_Net_cap) > (0.15* d.CY_cap)) then 1 else 0 end as Rule_7
	 ,d.CY_dem as [rule8_curryear_demand]
     ,d.LY_dem as [rule8_lastyear_demand]	
     ,case when (d.CY_Net_cap >= 0) and d.CY_dem > d.LY_dem then 1 else 0 end as Rule_8	
	 ,d.weeks_to_Peak as [rule9_weeks_to_Peak] 
	 ,case when d.weeks_to_Peak > 1 then 1 else 0 end as Rule_9	
	 ,d.avg_rating_q_all - (0.40*d.avg_rating_q_all) as [rule10_within_40perc_avg_q_rating]		
	 ,case when (d.max_teg_rating < max_non_teg_rating) and (d.avg_rating_q_tegna < d.avg_rating_q_all - (0.20*d.avg_rating_q_all)) 
		and (d.avg_rating_q_tegna >= d.avg_rating_q_all - (0.40*d.avg_rating_q_all))  then 1 else 0 end as Rule_10

		,case when (d.avg_cpp = 0 and delta_fcst > 1 )then delta_fcst * LY_CPP 			
		when (d.avg_cpp = 0 and delta_fcst <= 1 )then  LY_CPP 	
  else d.avg_cpp end as avg_cpp_new	

  , d.wo_station_name			
  ,d.NBC_rev			
  ,d.ABC_rev			
  ,d.CBS_rev			
  ,d.FOX_rev			
  ,d.NBC_units			
  ,d.ABC_units			
  ,d.CBS_units			
  ,d.FOX_units		
  ,d.base_price_final as cy_spot_rate
  ,d.air_year
, d.invcode_external_id
 ,d.rule_3_price
	,d.revenue_forecast as rev_fcst
	,budget_inv_weekly as budget
	,cast(getdate() as date ) [backup_date]

   from			
 (select c.*,  r.CY_cap, r.CY_dem, r.CY_Net_cap, r.is_news, r.last_year_dem as ly_dem, r.max_non_teg_k_cpp, r.max_non_teg_rating, r.max_teg_k_cpp, r.max_teg_rating,r.last_year_spotrate as LY_spot_rate , r.last_year_cpp as LY_CPP 			
 , case when c.avg_CPP = 0 and CY_rating > 0  then  (Last_year_rating / CY_rating)  			
		when c.avg_CPP = 0 and CY_rating = 0 then  0 end as delta_fcst	
	, r.Last_year_rating as ly_rating, CY_rating	, r.cy_spot_rate	, e.avg_rating_q_tegna,e.quarter_num , f.avg_rating_q_all, g.rule_3_price , z.base_price_final , z.base_price_ind,  y.budget_inv_weekly 
	--,x.rownum
			
 from			
 (			
 select b.*  , case when weeks_remaining > 0 then (Rem_dem/weeks_remaining) else 0 end as Expected_Weekly			
, case	 when b.wo_affiliation = ''NBC'' and NBC_CPP >0  then (NBC_CPP/NBC_CPP)*100 		
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (NBC_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (NBC_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (NBC_CPP/FOX_CPP)*100 END AS NBC_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (ABC_CPP/NBC_CPP)*100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (ABC_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (ABC_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (ABC_CPP/FOX_CPP)*100 END AS ABC_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (CBS_CPP/NBC_CPP) * 100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (CBS_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (CBS_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (CBS_CPP/FOX_CPP)*100 END AS CBS_CPP_RATIO	
, case   when b.wo_affiliation = ''NBC'' and NBC_CPP >0 then (FOX_CPP/NBC_CPP) *100 			
		 WHEN b.wo_affiliation = ''ABC'' and ABC_CPP >0 then (FOX_CPP/ABC_CPP)*100 	
		 WHEN b.wo_affiliation = ''CBS'' and CBS_CPP >0 then (FOX_CPP/CBS_CPP)*100 	
		 WHEN b.wo_affiliation = ''FOX'' and FOX_CPP >0 then (FOX_CPP/FOX_CPP)*100 END AS FOX_CPP_RATIO	
,sum([sold]) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
  -- ,	[air_week_date]		
     order by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
   ,	[air_week_date]		
	  rows unbounded preceding) as cum_spots_sold		
,sum([booked_revenue]) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
 --  ,	[air_week_date]		
     order by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]			
   ,	[air_week_date]		
	  rows unbounded preceding) as cum_rev,		
PERCENTILE_CONT(0.55) within group (order by sold) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]) as sold_zero,			
PERCENTILE_CONT(0.55) within group (order by booked_revenue) over (partition by [wo_market]			
	  , [wo_affiliation]		
       ,[wo_daypart_name]			
      ,[wo_invcode_name]) rev_zero			
			
from			
(select a.* , case when weeks_to_Peak > 0 then ''Y'' else ''N'' end as peak_sales_flag,			
datediff(WK, Getdate() , air_week_date) as weeks_remaining			
			
from			
(SELECT [wo_market]			
      ,[wo_affiliation]			
	  ,[wo_station_name]		
      ,[wo_daypart_name]			
	  ,[nielsen_daypart]		
	-- ,[nielsen_station]		
      ,[wo_invcode_name]	
,invcode_external_id		
      ,[air_week_date]			
	  ,[air_year]		
	  ,max([peak_week_date]) as peak_week_date		
      ,max([weeks_to_peak]) as weeks_to_Peak			
	  ,avg(NBC_CPP) as NBC_CPP		
	  ,avg(ABC_CPP) as ABC_CPP		
	  ,avg(CBS_CPP) as CBS_CPP		
	  ,avg(FOX_CPP) as FOX_CPP		
	  ,avg(NBC_rev) as NBC_rev		
	  ,avg(ABC_rev) as ABC_rev		
	  ,avg(CBS_rev) as CBS_rev		
	  ,avg(FOX_rev) as FOX_rev		
	  ,avg(NBC_units) as NBC_units		
	  ,avg(ABC_units) as ABC_units		
	  ,avg(CBS_units) as CBS_units		
	  ,avg(FOX_units) as FOX_units		
	  ,AVG(NBC_Rating) as NBC_Rating		
	  ,AVG(ABC_Rating) as ABC_Rating		
	  ,AVG(CBS_Rating) as CBS_Rating		
	  ,AVG(FOX_Rating) as FOX_Rating		
	  ,max(avg_cpp) as avg_CPP		
	  ,max(booked_revenue) as booked_revenue		
	  ,max(sold) as sold		
	  ,avg(capacity) as capcity		
	  ,avg(capacity) - max(sold) as Rem_cap		
	  ,Max(Tot_Dem) as Tot_dem		
	  ,Max(Tot_Dem) - Max(sold) as Rem_dem		
	  ,avg(capacity)  - Max(Tot_Dem) as Net_Cap		
	  ,max(revenue_forecast) as revenue_forecast
	   FROM [Staging].[dbo].[price_guide_staging]		
	where air_year >= Year(getdate())  --- and wo_station_name =''WFAA'' --and wo_invcode_name =''Local News @ 6a M-F'' and air_week_date =''06/18/2018''
   group by [wo_market]			
      ,[wo_affiliation]			
	  ,wo_station_name		
      ,[wo_daypart_name]			

	  ,[nielsen_daypart]		
      ,[wo_invcode_name]	
,invcode_external_id		
      ,[air_week_date]			
	  ,[air_year]		
	
 ) a			
	   ) b  ) c left outer join #temp_pricing_rules r		
	   on (c.wo_market = r.wo_market	
	   and c.wo_affiliation = r.wo_affiliation    		
	   and c.wo_daypart_name = r.wo_daypart_name		
	   and c.wo_invcode_name = r.wo_invcode_name		
	   and c.air_week_date = r.air_week_date
	   and c.air_year =  r.air_year) 
	   left outer join #temp_price_rule3 g
	   on (c.wo_market =g.wo_market 
	   and c.wo_station_name = g.wo_station_name
	   and c.nielsen_daypart = g.nielsen_daypart
	   and c.wo_invcode_name = g.wo_invcode_name)
	left outer join #temp_tegna_quarter_ratings e on (c.wo_market = e.wo_market	
	  	   and c.wo_affiliation = e.wo_affiliation    		
	   and c.nielsen_daypart = e.nielsen_daypart		
	   and c.wo_invcode_name = e.wo_invcode_name		
	   and c.air_year =  e.air_year
	   and datepart(quarter, c.air_week_date)= e.quarter_num)
	   left outer join #temp_all_quarter_ratings f on (c.wo_market = f.wo_market	
		   and c.wo_affiliation = f.wo_affiliation    		
	   and c.nielsen_daypart = f.nielsen_daypart		
	   and c.wo_invcode_name = f.wo_invcode_name		
	   and c.air_year =  f.air_year
	   and datepart(quarter, c.air_week_date)= f.quarter_num)
	    left outer join #temp_base_price_revised z on (c.wo_market = z.wo_market	
		--   and c.wo_affiliation = z.wo_affiliation    		
	--   and c.nielsen_daypart = z.nielsen_daypart		
	   and c.wo_invcode_name = z.Inventory_code		
	   and c.air_year =  year(z.air_week)
	   and c.air_week_date= z.air_week)
	left outer join budget_invcode_weekly y on (c.wo_station_name = y.station 
	and c.nielsen_daypart = y.nielsen_Daypart
	and c.wo_invcode_name =y.wo_invcode_name
	and c.air_year = y.air_year
	and c.air_week_date = y.air_week_date)
	/*left outer join #temp_quarter_ratings_rank x on (c.wo_market = x.wo_market 
	and   c.nielsen_daypart = x.nielsen_Daypart
	and c.wo_invcode_name =x.wo_invcode_name
	and c.air_year = x.air_year
	and datepart(quarter, c.air_week_date)= x.quarter_num) */
	 ) d 	;
', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [rules_calc]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'rules_calc', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
delete from price_grid_batch1 where ((air_year = Year(Getdate()) and month(air_week) >=  month(getdate()) ) or air_year = Year(Getdate())+1) ;

insert into price_grid_batch1
SELECT [wo_market]
      ,[wo_affiliation]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
      ,[nielsen_daypart]
      ,[air_week_date]
      ,Round([booked_Revenue]/5, 0)*5 [booked_Revenue]
      ,Round([avg_CPP]/5, 0)*5 avg_CPP
      ,Round([Capcity],0 ) [Capacity]
      ,Round([Sold], 0) [Sold]
	  ,(Round([Capcity],0 ) - Round([Sold], 0))  [Rem_Cap]
     -- ,Round([Rem_Cap],0) [Rem_Cap]
      ,Round([tot_dem],0) [tot_dem]
	  ,(Round([tot_dem],0) - Round([Sold], 0))  [Rem_dem]
    --  ,Round([Rem_dem],0) [Rem_dem]
	  ,(Round([Capcity],0 ) - Round([tot_dem],0)) [Net_cap]
   --   ,Round([Net_cap],0) [Net_cap]
      ,[peak_sales_flag]
      ,[weeks_to_Peak]
      ,[peak_week_date]
      ,[Expected_Weekly]
      ,Round([NBC_rating],2) [NBC_rating]
      ,Round([ABC_rating] ,2) [ABC_rating]
      ,Round([CBS_rating],2) [CBS_rating]
      ,Round([FOX_rating],2) [FOX_rating]
      ,[NBC_CPP_ratio]
      ,[ABC_CPP_Ratio]
      ,[CBS_CPP_Ratio]
      ,[FOX_CPP_Ratio]
      ,[cy_dem]
      ,[LY_dem]
      ,[CY_net_Cap]
      ,[ly_spot_rate]
      ,[ly_cpp]
      ,[Rule_1]
      ,[Rule_2]
      ,[Rule_3]
      ,[Rule_4]
      ,[Rule_5]
      ,[Rule_6]
      ,[Rule_7]
      ,[Rule_8]
      ,[Rule_9]
      ,[avg_Cpp_new]
      ,[wo_Station_name]
      ,[NBC_rev]
      ,[ABC_rev]
      ,[CBS_rev]
      ,[FOX_rev]
      ,[NBC_units]
      ,[ABC_units]
      ,[CBS_units]
      ,[FOX_units]
      ,[cy_spot_rate]
      ,[perc_increase_cap]
      ,Round([Base_CPP]/5, 0)*5  [Base_CPP]
      ,Round([Base_price]/5, 0)*5 [Base_price]
      ,[Existing_rate]
      ,[Recommended_CPP]
      ,Round([Recommended_CPP]/5, 0)*5 [Recommended_CPP]
      ,Round([base_revenue]/5, 0)*5 [base_revenue]
      ,Round([Recommended_Price]/5, 0)*5 [Recommended_Price]
      ,Round([Expected_Revenue_rd]/5, 0)*5 [Expected_Revenue_rd]
      ,[pricing_benefit]
      ,[pricing_benefit_perc]
      ,Round([ABC_Price_N]/5, 0)*5 [ABC_Price_N]
      ,Round([CBS_Price_N]/5, 0)*5 [CBS_Price_N]
      ,Round([FOX_Price_N]/5, 0)*5 [FOX_Price_N]
      ,Round([NBC_Price_N]/5, 0)*5 [NBC_Price_N]
      ,Round([ABC_Price_KM]/5, 0)*5 [ABC_Price_KM]
      ,Round([CBS_Price_KM]/5, 0)*5 [CBS_Price_KM]
      ,Round([FOX_Price_KM]/5, 0)*5 [FOX_Price_KM]
      ,Round([NBC_Price_KM]/5, 0)*5 [NBC_Price_KM]
      ,cast(getdate() as date)
	  , air_year
	  ,Round(case when sold > 0 then [booked_revenue] / sold else 0 end /5 ,0)*5 as EUR	
	  ,invcode_external_id
	  ,[Rule_10]
	  ,Round([Expected_Revenue_rc]/5, 0)*5 [Expected_Revenue_rc]
,revenue_forecast  as rev_fcst
,budget as budget
,0 as bridge_price_rc
,0 as bridge_price_rd
--,base_price_ind

from
(
select f.*, (expected_revenue_rd - base_revenue ) as pricing_benefit, 
case when base_revenue >0  then ((expected_revenue_rd - base_revenue )/base_revenue)*100 else 0 end as pricing_benefit_perc,
(base_CPP * ABC_rating)  as ABC_Price_N,
 (base_CPP * CBS_rating) as CBS_Price_N
,  (base_CPP * FOX_rating) as FOX_Price_N
, (base_CPP *NBC_rating ) as NBC_Price_N
, (case when ABC_units > 0 then ABC_rev/ABC_units else 0 end )as ABC_Price_KM
, (case when   CBS_units > 0 then CBS_rev/CBS_units else 0 end) as CBS_Price_KM
, (case when   FOX_units > 0 then FOX_rev/FOX_units else 0 end) as FOX_Price_KM
, (case when   NBC_units > 0 then NBC_rev/NBC_units else 0 end) as NBC_Price_KM

from
(select e.*,case when Rem_Dem > 0  then (Round([Recommended_Price]/5, 0)*5 * (Round([tot_dem],0) - Round([Sold], 0))  + Round([booked_Revenue]/5, 0)*5 ) ELSE booked_revenue end as expected_revenue_rd,
case when Rem_Cap > 0  then ((Round([Recommended_Price]/5, 0)*5 * (Round([Capcity],0 ) - Round([Sold], 0))) + Round([booked_Revenue]/5, 0)*5 ) ELSE booked_revenue end as expected_revenue_rc
from
(select d.*
from
(
select c.*, 
case when Rem_Dem > 0  then (booked_revenue + ( Rem_Dem * base_price ) ) ELSE booked_revenue end as base_revenue
from
 (select a.*, 
	   base_price as Existing_rate,
        case	  when wo_affiliation = ''NBC''  and NBC_rating >= 0.2 then recommended_price/NBC_rating  
	              when wo_affiliation = ''ABC''  and ABC_rating >= 0.2 then recommended_price/ABC_rating  
			      when wo_affiliation = ''CBS''  and CBS_rating >= 0.2 then recommended_price/CBS_rating  
			      when wo_affiliation = ''FOX''  and FOX_rating >= 0.2 then recommended_price/FOX_rating  
			 end as Recommended_CPP
	     from 
	   (select b.*, (base_price + (base_price * (perc_increase_cap/100)  ))  as recommended_price,
	   -- case when wo_affiliation = ''NBC'' and avg_cpp_new > 0   then avg_cpp_new  
		  --   when wo_affiliation = ''NBC'' and avg_cpp_new = 0  and NBC_rating >= 0.2 then base_price/NBC_rating  
	   --      when wo_affiliation = ''ABC'' and avg_cpp_new > 0  then avg_cpp_new
			 --when wo_affiliation = ''ABC'' and avg_cpp_new = 0 and ABC_rating >= 0.2 then base_price/ABC_rating  
			 --when wo_affiliation = ''CBS'' and avg_cpp_new > 0    then avg_cpp_new
			 --when wo_affiliation = ''CBS'' and avg_cpp_new =0  and CBS_rating >= 0.2 then base_price/CBS_rating  
			 --when wo_affiliation = ''FOX'' and avg_cpp_new > 0  then avg_cpp_new
			 --when wo_affiliation = ''FOX'' and avg_cpp_new =0  and FOX_rating >= 0.2 then base_price/FOX_rating  
			 --end as base_CPP
			 case when wo_affiliation = ''NBC''  and NBC_rating >= 0.2 then base_price/NBC_rating  
	              when wo_affiliation = ''ABC''  and ABC_rating >= 0.2 then base_price/ABC_rating  
			      when wo_affiliation = ''CBS''  and CBS_rating >= 0.2 then base_price/CBS_rating  
			      when wo_affiliation = ''FOX''  and FOX_rating >= 0.2 then base_price/FOX_rating  
			 end as base_CPP

			 from (
			 select h.* ,  case when perc_increase > =18 then 18 else perc_increase end as perc_increase_cap,
			 	 --  case when ( cy_spot_rate >= ly_spot_rate  and cy_spot_rate >= rule_3_price ) then cy_spot_rate
						--when ( ly_spot_rate > cy_spot_rate  and ly_spot_rate > rule_3_price ) then ly_spot_rate
						--else  rule_3_price end as base_price
/* New Base Price Revsion - As of Jan 2018 -- Start */
						  case when ( cy_spot_rate > 0) then   cy_spot_rate
						--	  when ly_spot_rate > 0 then ly_spot_rate
					--		  else rule_3_price 
							  else 0
							end as base_price

		/* New Base Price Revsion - As of Jan 2018 -- End */
	  	  from
(select [wo_market]
      ,[wo_affiliation]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
,invcode_external_id
      ,[nielsen_daypart]
	  ,air_year
      ,[air_week_date]
	--  ,base_price_ind
      ,max([booked_revenue]) as [booked_revenue]
      ,max([avg_CPP]) [avg_CPP]
      ,max([capcity]) [capcity]
	  ,max(sold) as sold,
	   max(Rem_Cap) as Rem_Cap
,max(tot_dem) as tot_dem
, max(Rem_dem) as Rem_dem
, max(Net_cap) as Net_cap
      ,[peak_sales_flag]
      ,[weeks_to_Peak]
      ,[peak_week_date]
      ,max([Expected_Weekly]) as [Expected_Weekly]
      ,max(NBC_rating) NBC_rating,
max(ABC_rating) ABC_rating,
max(CBS_rating) CBS_rating,
max(FOX_rating) FOX_rating,
max(NBC_CPP_ratio) NBC_CPP_ratio,
max(ABC_CPP_Ratio) ABC_CPP_Ratio,
max(CBS_CPP_Ratio) CBS_CPP_Ratio,
max(FOX_CPP_Ratio) FOX_CPP_Ratio,
max(cy_dem) cy_dem,
max(LY_dem) LY_dem,
max(CY_net_Cap) CY_net_Cap,
max(ly_spot_rate) as ly_spot_rate,
max(ly_cpp) ly_cpp,
max(Rule_1) Rule_1 ,
max(Rule_2) Rule_2,
max(Rule_3) Rule_3,
max(Rule_4) Rule_4,
max(Rule_5) Rule_5,
max(Rule_6) Rule_6,
max(Rule_7) Rule_7,
max(Rule_8) Rule_8,
max(Rule_9) Rule_9,
max(Rule_10) Rule_10,
max(avg_cpp_new) as avg_Cpp_new,
[wo_Station_name],
max(NBC_rev) as NBC_rev,
max(ABC_rev) as ABC_rev,
max(CBS_rev) as CBS_rev,
max(FOX_rev) as FOX_rev,
max(NBC_units) as NBC_units,
max(ABC_units) ABC_units,
max(CBS_units) CBS_units,
max(FOX_units) FOX_units,
max(cy_spot_rate) as cy_spot_rate,
max(rule_3_price)  rule_3_price
,sum  (case when Rule_1 = 1 and r.Rule_ID = 1  then r.starting_weight 
	            when Rule_2 = 1 and r.Rule_ID = 2 then r.starting_weight
	            when Rule_3 = 1 and r.Rule_ID = 3 then r.starting_weight
				when Rule_4 = 1 and r.Rule_ID = 4 then  r.starting_weight
				when Rule_5 = 1 and r.Rule_ID = 5 then  r.starting_weight
				when Rule_6 = 1 and r.Rule_ID = 6 then r.starting_weight
				when Rule_7 = 1 and r.Rule_ID = 7 then  r.starting_weight
				when Rule_8 = 1 and r.Rule_ID = 8 then  r.starting_weight
				when Rule_9 = 1 and r.Rule_ID = 9 then  r.starting_weight
				when Rule_10 = 1 and r.Rule_ID = 10 then  r.starting_weight
		else 0 end )as perc_increase 
	,max(revenue_forecast) as revenue_forecast
	,max(budget) as budget
 from  [Wide_Orbit].[dbo].[pricing_rules] R,
(SELECT  [wo_market]
      ,[wo_affiliation]
      ,[wo_daypart_name]
      ,a.[wo_invcode_name]
,invcode_external_id
      ,a.[nielsen_daypart]
	   ,air_year
      ,[air_week_date]
	--  ,base_price_ind
      ,max([booked_revenue]) as [booked_revenue]
      ,max([avg_CPP]) [avg_CPP]
      ,max([capcity]) [capcity]
	  ,max(sold) as sold,
	   max(Rem_Cap) as Rem_Cap
,max(tot_dem) as tot_dem
, max(Rem_dem) as Rem_dem
, max(Net_cap) as Net_cap
      ,[peak_sales_flag]
      ,[weeks_to_Peak]
      ,[peak_week_date]
      ,max([Expected_Weekly]) as [Expected_Weekly]
      ,max(NBC_rating) NBC_rating,
max(ABC_rating) ABC_rating,
max(CBS_rating) CBS_rating,
max(FOX_rating) FOX_rating,
max(NBC_CPP_ratio) NBC_CPP_ratio,
max(ABC_CPP_Ratio) ABC_CPP_Ratio,
max(CBS_CPP_Ratio) CBS_CPP_Ratio,
max(FOX_CPP_Ratio) FOX_CPP_Ratio,
max(cy_dem) cy_dem,
max(LY_dem) LY_dem,
max(CY_net_Cap) CY_net_Cap,
max(ly_spot_rate) as ly_spot_rate,
max(ly_cpp) ly_cpp,
max(Rule_1) Rule_1 ,
max(Rule_2) Rule_2,
max(Rule_3) Rule_3,
max(Rule_4) Rule_4,
max(Rule_5) Rule_5,
max(Rule_6) Rule_6,
max(Rule_7) Rule_7,
max(Rule_8) Rule_8,
max(Rule_9) Rule_9,
max(Rule_10) Rule_10,
max(avg_cpp_new) as avg_Cpp_new,
a.[wo_Station_name],
max(NBC_rev) as NBC_rev,
max(ABC_rev) as ABC_rev,
max(CBS_rev) as CBS_rev,
max(FOX_rev) as FOX_rev,
max(NBC_units) as NBC_units,
max(ABC_units) ABC_units,
max(CBS_units) CBS_units,
max(FOX_units) FOX_units,
max(cy_spot_rate) as cy_spot_rate,
max(rule_3_price) as rule_3_price,
max(revenue_forecast) as revenue_forecast,
MAX(budget) as budget
  FROM [Wide_Orbit].[dbo].[price_grid_staging] a 
 --where  air_year = Year(Getdate()) and month(air_week_date) <= 6

where  ((air_year = Year(Getdate()) and month(air_week_date) >= month(getdate()) ) or (air_year = Year(Getdate())+1))

group by [wo_market]
      ,[wo_affiliation]
      ,[wo_daypart_name]
      ,a.[wo_invcode_name]
,invcode_external_id
      ,a.[nielsen_daypart]
	   ,air_year
      ,[air_week_date]
      ,a.[wo_Station_name]
	     ,[peak_sales_flag]
      ,[weeks_to_Peak]
      ,[peak_week_date]
	 -- ,base_price_ind
    --  ,[Expected_Weekly]
     ) n 
	 where  n.wo_market = r.market
	and n.nielsen_daypart = r.daypart
	 group by [wo_market]
      ,[wo_affiliation]
      ,[wo_daypart_name]
      ,[wo_invcode_name]
,invcode_external_id
      ,[nielsen_daypart]
	   ,air_year
      ,[air_week_date]
      ,[wo_Station_name]
	     ,[peak_sales_flag]
      ,[weeks_to_Peak]
      ,[peak_week_date]
	 -- ,base_price_ind
    --  ,[Expected_Weekly]
	)h
	 )b
	  ) a ) c ) d ) e ) f )g; 

', 
		@database_name=N'Wide_Orbit', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [email_task]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'email_task', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=10, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\awe-biutil01-dv\D$\MSSQL\WO\email_task.dtsx\"" /CHECKPOINTING OFF /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [datachecks_Process]    Script Date: 11/1/2018 3:05:57 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'datachecks_Process', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"\\awe-biutil01-dv\D$\MSSQL\WO\Data_Checks_Process.dtsx\"" /CHECKPOINTING OFF /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Adhoc', 
		@enabled=0, 
		@freq_type=1, 
		@freq_interval=0, 
		@freq_subday_type=0, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20180508, 
		@active_end_date=99991231, 
		@active_start_time=200000, 
		@active_end_time=235959, 
		@schedule_uid=N'dfc6c156-41aa-4994-b568-44835a955317'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'weekly_run', 
		@enabled=1, 
		@freq_type=8, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=1, 
		@active_start_date=20180506, 
		@active_end_date=99991231, 
		@active_start_time=203000, 
		@active_end_time=235959, 
		@schedule_uid=N'0f0d0bd9-3260-4447-b6e0-6d72ad05e3b4'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO


