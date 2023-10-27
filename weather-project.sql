

        -- SILVER LAYER
SELECT CAST(TIME AS TIME) AS extracted_time
FROM WEATHERDATA;

CREATE VIEW  HourlyTemprature as

SELECT CAST(TIME AS TIME) AS Record_time
        ,CAST(TIME AS DATE) AS Record_date
        , temperature_2m as HourlyTemprature
        ,'Stockholm' as City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA;

DROP VIEW HourlyTemprature;

CREATE VIEW HourlyRainFall as
SELECT CAST(TIME AS TIME) AS Record_time
        ,CAST(TIME AS DATE) AS Record_date
        , RAIN as HourlyRainFall
        ,'Stockholm' As City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA;

CREATE VIEW  HourlySnowFall As
SELECT CAST(TIME AS TIME) AS Record_time
        ,CAST(TIME AS DATE) AS Record_date
        , SNOWFALL As HourlySnowFall
        ,'Stockholm' As City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA;


CREATE VIEW  HourlyWindSpeed As
SELECT CAST(TIME AS TIME) AS Record_time
        ,CAST(TIME AS DATE) AS Record_date
        , WINDSPEED_10M As HourlyWindSpeed
        ,'Stockholm' As City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA;


        -- GOLD LAYER

CREATE VIEW DailyTemprature AS
SELECT    CAST(TIME AS DATE) AS Record_date 
        , round(avg(temperature_2m),2) AS DailyTemprature
        ,'Stockholm' AS City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA
GROUP BY Record_date
;

CREATE VIEW  DailyRainFall AS
SELECT    CAST(TIME AS DATE) AS Record_date 
        , SUM(RAIN) AS DailyRainFall
        ,'Stockholm' AS City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA
GROUP BY Record_date
;

CREATE VIEW  DailySnowFall AS
SELECT   CAST(TIME AS DATE) AS Record_date 
        ,SUM(SNOWFALL) AS DailySnowFall
        ,'Stockholm' AS City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA
GROUP BY Record_date
;

CREATE VIEW  DailyWindSpeed As
SELECT   CAST(TIME AS DATE) AS Record_date
        ,ROUND (AVG (WINDSPEED_10M),2) As DailyWindSpeed
        ,'Stockholm' As City
FROM SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA
GROUP BY Record_date;




------------------------------------------------------------------
--- FIRST QUESTION WHICH IS THE HOTTEST SUMMER MONTH, each year 

------USING RANK AND SUBQUERY

select  
           RECORD_YEAR 
         , RECORD_MONTH
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
from (
    SELECT  avg(DAILYTEMPRATURE) AS AVG_TEMP
            , MONTHNAME(RECORD_DATE) AS RECORD_MONTH
            ,YEAR(RECORD_DATE) AS RECORD_YEAR
            , ROW_NUMBER () over ( partition by year(record_date) order by avg(DAILYTEMPRATURE) desc ) as rnk
    FROM GOLD.DAILYTEMPRATURE
        WHERE MONTH(RECORD_DATE)  BETWEEN 6 AND 8
    GROUP BY RECORD_MONTH, RECORD_YEAR
    ORDER BY RECORD_YEAR 
     ) 
where rnk = 1
;


-----  2 Q - WHICH WAS THE HOTTEST DAY IN SUMMER MONTH 
----- using rnk and subqueris 
select  
           RECORD_YEAR
         , RECORD_DATE
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
from (
SELECT   DAILYTEMPRATURE AS AVG_TEMP
        ,RECORD_DATE                         
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by DAILYTEMPRATURE desc ) as rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE)  BETWEEN 6 AND 8
ORDER BY RECORD_YEAR 
     ) 
where rnk = 1
;






SELECT * FROM SRC_WEATHER_DATA.GOLD.DAILYTEMPRATURE WHERE DAILYTEMPRATURE= 'NaN';

----------------------------------
-- Q3  WHICH WAS THE COLDEST MONTH
select  
           RECORD_YEAR::varchar 
         , RECORD_MONTH
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
from (
SELECT  avg(DAILYTEMPRATURE) AS AVG_TEMP
        , MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by avg(DAILYTEMPRATURE) ) as rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE)  IN (9,10,11,12,1,2)
GROUP BY RECORD_MONTH, RECORD_YEAR
ORDER BY RECORD_YEAR 
     ) 
where rnk = 1
;


-------------------------------
---- Q 4 COLDEST DAY IN WINTER 

select  
           RECORD_YEAR::varchar 
         , RECORD_DATE
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
from (
SELECT  avg(DAILYTEMPRATURE) AS AVG_TEMP
        ,RECORD_DATE                          --MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by avg(DAILYTEMPRATURE) ) as rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE) IN (9,10,11,12,1,2)
GROUP BY RECORD_DATE
ORDER BY  RECORD_YEAR
     ) 
where rnk = 1
;








-------------------------------------------------------------
------ Q5 THE WINDIEST MONTH EACH YEAR 
select  
           WIND_YEAR
          ,WIND_MONTH
         , ROUND(AVG(WIND_SPEED),2) AS WIND_EACH_MONTH 
from (
SELECT   
         MONTHNAME(RECORD_DATE) AS WIND_MONTH
        , YEAR(RECORD_DATE) AS WIND_YEAR
        , ROUND(AVG(DAILYWINDSPEED),2) AS WIND_SPEED
        , ROW_NUMBER () over ( partition by year(record_date) order by AVG(DAILYWINDSPEED) DESC ) as rnk
FROM GOLD.DAILYWINDSPEED
GROUP BY 1,2
     ) 
where rnk = 1
GROUP BY 1,2
ORDER BY 1
;





-------------------------- 
----- Q6 THE WINDIEST DAY OF WINTER AND AVG SPEED WIND OF THE YEAR
with avg_year as (

SELECT
YEAR(RECORD_DATE) AS Yearwind,
ROUND(AVG(dailywindspeed), 2) AS avg_wind_year
FROM SRC_WEATHER_DATA.GOLD.DAILYWINDSPEED
GROUP BY YEAR(RECORD_DATE)
),
 windiest_day as(
SELECT  MAX(DAILYWINDSPEED) AS WIND_SPEED
        ,RECORD_DATE                          --MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by MAX(DAILYWINDSPEED) DESC ) as rnk
FROM GOLD.DAILYWINDSPEED 

group by 2,3
     )
select  
          RECORD_DATE as the_windiest_Day
          , DAYNAME(RECORD_DATE) AS WINDIEST_DAY_NAME
         , WIND_SPEED AS MAX_WINDSPEED_DAY
         
         , RECORD_YEAR
         ,avg_year.avg_wind_year
from avg_year
inner join windiest_day
on  avg_year.yearwind =windiest_day.record_year

where windiest_day.rnk = 1
         
order by RECORD_YEAR
;



---------------------------------------------------------------------
---- Q7 WHICH MONTH HAD THE MOST RAINFALL EACH YEAR
select  
           RECORD_YEAR
         , RECORD_MONTH
         , ROUND(rain_fall,2) AS rain_EACH_YEAR 
from (
SELECT  sum(DAILYRAINFALL) AS rain_fall
        , MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by sum(DAILYRAINFALL) DESC ) as rnk
FROM GOLD.DAILYRAINFALL
GROUP BY 3,2
--ORDER BY YEAR(RECORD_DATE) 
     )    
where rnk = 1
--GROUP BY 1,2
--order by RECORD_YEAR

;
select rain from SRC_WEATHER_DATA.WEATHER_DATA.WEATHERDATA where rain >0;
select dailyrainfall from SRC_WEATHER_DATA.gold.daiLYRAINFALL where dailyrainfall>0;

select hourlyrainfall from SRC_WEATHER_DATA.SILVER.HOURLYRAINFALL where hourlyrainfall>0;
---------------------------------------------------------------------
--- Q8 WHICH MONTH HAD THE MOST SNOWFALL AND EACH YEAR
select  
           RECORD_YEAR
         , month
         , SNOW_FALL AS SNOW_EACH_YEAR 
from (
SELECT  sum(DAILYSNOWFALL) AS SNOW_FALL
        ,month(RECORD_DATE)  as month                        --MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by sum(DAILYSNOWFALL) DESC ) as rnk
FROM GOLD.DAILYSNOWFALL
       
GROUP BY 3,2
ORDER BY 3
     ) 
where rnk = 1
;


----------------------------------------------------------------------------
----- Q9 Which date marked the start of spring each year
select * 
from SRC_WEATHER_DATA.GOLD.DAILYTEMPRATURE order by record_date ;



with SPRING_DATES as (
    select  
             
              RECORD_DATE
              , ROW_NUMBER () OVER ( PARTITION BY YEAR(RECORD_DATE) ORDER BY RECORD_DATE ) AS spring_start_date
              , temp_spring
             
    from (
            SELECT  
                    RECORD_DATE ,DAILYTEMPRATURE  ,                       
                    
                     min(DAILYTEMPRATURE) over ( order by RECORD_DATE  ROWS BETWEEN 7 preceding and CURRENT ROW) as temp_spring
            FROM GOLD.DAILYTEMPRATURE
            order by record_date
    
         ) 
         where  to_char(record_date, 'MM-DD' ) between '02-15' and '07-31' 
 and temp_spring > 0.0
)

select record_date AS SPRING_START_DATE
from SPRING_DATES
where spring_start_date=1 ;





-------------------------------------------------------------
------------------------------------------------------------
--- Q10  
with rainn as (
select year(record_date) as rain_year, count(*) as rain_count
from SRC_WEATHER_DATA.GOLD.DAILYRAINFALL
where dailyrainfall >10
group by rain_year order by rain_count desc ----2012= 15, 2011 = 8
),

 temperatue as (
select year(record_date) as temp_year, count(*) as temp_count
from SRC_WEATHER_DATA.GOLD.DAILYSNOWFALL
where dailysnowfall >30
group by snow_year order by snow_count desc -- 2012, 2011=1, 2010, 2003
),

 snoww as (
select year(record_date) as snow_year, count(*) as snow_count
from SRC_WEATHER_DATA.GOLD.DAILYSNOWFALL
where dailysnowfall >30
group by snow_year order by snow_count desc -- 2012, 2011=1, 2010, 2003
),

 windd as (
select year(record_date) wind_year, count(*) as wind_count
from SRC_WEATHER_DATA.SILVER.HOURLYWINDSPEED where hourlywindspeed >60
group by wind_year order by wind_count desc ), ---- 2007,2011= 5

 years as (
 select distinct year (record_date) as year_anomaly  from SRC_WEATHER_DATA.GOLD.DAILYRAINFALL
 )

select   year_anomaly
        ,coalesce (rainn.rain_count,0) as rain
        ,coalesce(windd.wind_count,0) as wind
        ,coalesce(snoww.snow_count,0) as snow
       , sum(coalesce (rainn.rain_count,0) + coalesce(windd.wind_count,0) + coalesce(snoww.snow_count,0)) as summm
         
from years
left join rainn
on year_anomaly=rainn.rain_year
left join snoww
on year_anomaly = snoww.snow_year
left join windd 
on  year_anomaly=windd.wind_year
group by 1,2,3,4
order by summm desc
limit 1
 ;

