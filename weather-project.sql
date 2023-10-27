
        -- SILVER LAYER
/*Creating a divided layer of the raw data to devide wind rain snow and temp on hourly basis*/

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
/*Creating a divided layer of the raw data to devide wind rain snow and temp on daily basis with aggregation for the day*/

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
/*to solve this one i used a window function to give a ranking for each month in each year using the avg temperature of  per month asc t0 get the hottest temp,adding a filter for the summer month only, afterwards getting the rank number 1 in the original query to present the max temp per month in each year in one result*/
SELECT 
           RECORD_YEAR 
         , RECORD_MONTH
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
FROM (
    SELECT  avg(DAILYTEMPRATURE) AS AVG_TEMP
            , MONTHNAME(RECORD_DATE) AS RECORD_MONTH
            ,YEAR(RECORD_DATE) AS RECORD_YEAR
            , ROW_NUMBER () over ( partition by year(record_date) order by avg(DAILYTEMPRATURE) desc ) as rnk
    FROM GOLD.DAILYTEMPRATURE
        WHERE MONTH(RECORD_DATE)  BETWEEN 6 AND 8
    GROUP BY RECORD_MONTH, RECORD_YEAR
    ORDER BY RECORD_YEAR 
     ) 
WHERE rnk = 1
;


-----  2 Q - WHICH WAS THE HOTTEST DAY IN SUMMER MONTH 
/*to solve this one i used a window function to give a ranking for each month in each year using the max temperature  per day desc to get the hottest temp,adding a filter for the summer month only, afterwards getting the rank number 1 in the original query to present the max temp per day in each year in one result*/
SELECT 
           RECORD_YEAR
         , RECORD_DATE
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
FROM (
SELECT   DAILYTEMPRATURE AS AVG_TEMP
        ,RECORD_DATE                         
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( PARTITION BY year(record_date) ORDER BY DAILYTEMPRATURE DESC ) AS rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE)  BETWEEN 6 AND 8
ORDER BY RECORD_YEAR 
     ) 
WHERE rnk = 1
;
----------------------------------
-- Q3  WHICH WAS THE COLDEST MONTH
/*to solve this one i used a window function to give a ranking for each month in each year using the avg temperature of  per month asc tp get the coldest temp,adding a filter for the winter month only, afterwards getting the rank number 1 in the original query to present the min temp per month in each year in one result*/
SELECT 
           RECORD_YEAR
         , RECORD_MONTH
         , ROUND(AVG_TEMP,2) AS TEMP_EACH_YEAR 
FROM (
SELECT  AVG(DAILYTEMPRATURE) AS AVG_TEMP
        ,MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        ,ROW_NUMBER () over ( PARTITION BY year(record_date) order by AVG(DAILYTEMPRATURE) ) as rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE)  IN (9,10,11,12,1,2)
GROUP BY RECORD_MONTH, RECORD_YEAR
ORDER BY RECORD_YEAR 
     ) 
WHERE rnk = 1
;
-------------------------------
---- Q 4 COLDEST DAY IN WINTER 
/*to solve this one i used a window function to give a ranking for each month in each year using the min temperature  per day asc, afterwards getting the rank number 1 in the original query to present the most cold day in each year in one result*/

SELECT  
           RECORD_YEAR
         , RECORD_DATE
         , ROUND(MIN_TEMP,2) AS TEMP_EACH_YEAR 
FROM(
SELECT  min(DAILYTEMPRATURE) AS MIN_TEMP
        ,RECORD_DATE                          
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        ,ROW_NUMBER () over ( partition by year(record_date) order by min(DAILYTEMPRATURE) ) as rnk
FROM GOLD.DAILYTEMPRATURE
    WHERE MONTH(RECORD_DATE) IN (9,10,11,12,1,2)
GROUP BY RECORD_DATE
ORDER BY  RECORD_YEAR
     ) 
WHERE rnk = 1
;
-------------------------------------------------------------
------ Q5 THE WINDIEST MONTH EACH YEAR 
/* to solve this one i used a window function to give a ranking for each month in each year using the avg amount of wind per month desc, afterwards getting the rank number 1 in the original query to present the maximum wind in each year in one result*/
SELECT 
           WIND_YEAR
          ,WIND_MONTH
          ,ROUND(AVG(WIND_SPEED),2) AS WIND_EACH_MONTH 
FROM(
SELECT   
         MONTHNAME(RECORD_DATE) AS WIND_MONTH
        , YEAR(RECORD_DATE) AS WIND_YEAR
        , ROUND(AVG(DAILYWINDSPEED),2) AS WIND_SPEED
        , ROW_NUMBER () over ( PARTITION BY year(record_date) ORDER BY AVG(DAILYWINDSPEED) DESC ) as rnk
FROM GOLD.DAILYWINDSPEED
GROUP BY 1,2
     ) 
WHERE rnk = 1
GROUP BY 1,2
ORDER BY 1
;
-------------------------- 
----- Q6 THE WINDIEST DAY OF WINTER AND AVG SPEED WIND OF THE YEAR
/* to solve this one i used a window function to give a ranking for each day in each year using the maximum of wind per day desc, afterwards getting the rank number 1 in the original query to present the day in each year and also getting the avg of wind for each year in one result*/
WITH avg_year AS (
SELECT
YEAR(RECORD_DATE) AS Yearwind,
ROUND(AVG(dailywindspeed), 2) AS avg_wind_year
FROM SRC_WEATHER_DATA.GOLD.DAILYWINDSPEED
GROUP BY YEAR(RECORD_DATE)
),
 windiest_day AS(
SELECT  MAX(DAILYWINDSPEED) AS WIND_SPEED
        ,RECORD_DATE                         
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( PARTITION year(record_date) ORDER BY MAX(DAILYWINDSPEED) DESC ) as rnk
FROM GOLD.DAILYWINDSPEED 
GROUP BY 2,3   )
SELECT  
          RECORD_DATE AS the_windiest_Day
         ,DAYNAME(RECORD_DATE) AS WINDIEST_DAY_NAME
         ,WIND_SPEED AS MAX_WINDSPEED_DAY
         ,RECORD_YEAR
         ,avg_year.avg_wind_year
FROM avg_year
INNER JOIN windiest_day
ON  avg_year.yearwind =windiest_day.record_year
WHERE windiest_day.rnk = 1  
ORDER BY RECORD_YEAR
;
---------------------------------------------------------------------
---- Q7 WHICH MONTH HAD THE MOST RAINFALL EACH YEAR
/*to solve this one i used a window function to give a ranking for each month in each year using the sum amount of rain per month desc, afterwards getting the rank number 1 in the original query to present the maximum rainfall in each year in one result*/
SELECT  
           RECORD_YEAR
         , RECORD_MONTH
         , ROUND(rain_fall,2) AS rain_EACH_YEAR 
FROM(
SELECT  sum(DAILYRAINFALL) AS rain_fall
        , MONTHNAME(RECORD_DATE) AS RECORD_MONTH
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by sum(DAILYRAINFALL) DESC ) as rnk
FROM GOLD.DAILYRAINFALL
GROUP BY 3,2
    )    
where rnk = 1
;

---------------------------------------------------------------------
--- Q8 8. Which month had the mostsnowfall each year?
/* to solve this one i used a window function to give a ranking for each month in each year using the sum amount of snow per month desc, afterwards getting the rank number 1 in the original query to present the maximum snowfall in each year in one result  */
SELECT 
           RECORD_YEAR
         , month
         , SNOW_FALL AS SNOW_EACH_YEAR 
FROM (
SELECT  sum(DAILYSNOWFALL) AS SNOW_FALL
        ,month(RECORD_DATE)  as month                        
        ,YEAR(RECORD_DATE) AS RECORD_YEAR
        , ROW_NUMBER () over ( partition by year(record_date) order by sum(DAILYSNOWFALL) DESC ) as rnk
FROM GOLD.DAILYSNOWFALL
       
GROUP BY 3,2
ORDER BY 3
     ) 
WHERE rnk = 1
;
----------------------------------------------------------------------------
----- Q9 Which date marked the start of spring each year?
/*to solve this question */
/*Spring will arrive after 7 consecutive days with spring temperatures. Spring temperature is daily mean temperature above 0.0°C, but not yet for 7 consecutive days. 15 February is set as the earliest allowed date for spring arrival. The latest date for spring arrival is 31 July
-- to calculate the spring start based on that definition, i used window funtion that selects the min temperature of 8 consecutive days in a subquery, then on that result i wanted to apply the filer of the days range and if that temp is >0 afterwards applying another window function to give a rank for each 8th day that found within the filter and for each year, then in the original query i retrived only the days that got a rank of 1 */
WITH SPRING_DATES AS (
SELECT  
             
        RECORD_DATE
        ,ROW_NUMBER () OVER ( PARTITION BY YEAR(RECORD_DATE) ORDER BY RECORD_DATE ) AS spring_start_date
        ,temp_spring  
FROM (
        SELECT  
            RECORD_DATE ,DAILYTEMPRATURE  ,                       
            min(DAILYTEMPRATURE) over( order by RECORD_DATE  ROWS BETWEEN 7 preceding and CURRENT ROW) as temp_spring
        FROM GOLD.DAILYTEMPRATURE
        ORDER BY record_date
      ) 
WHERE  to_char(record_date, 'MM-DD' ) BETWEEN '02-15' AND '07-31' 
AND temp_spring > 0.0)

SELECT record_date AS SPRING_START_DATE
FROM SPRING_DATES
WHERE spring_start_date=1 ;

-------------------------------------------------------------
--- Q10 - Which year saw the most weather anomalies? 
/* to solve this question i created a cte for each anomaly that calculates the count days of anomalies per year, i also created a cte that gets the list of the years only so i can left join on that cte the rest of the anomalies and compare the results, in the original query i presented the sum of all the anomalies for each year, ordered by that sum desc and limiting 1 to get the most anomaly year*/

WITH rainn AS (
SELECT year(record_date) AS rain_year, count(*) AS rain_count
FROM SRC_WEATHER_DATA.GOLD.DAILYRAINFALL
WHERE dailyrainfall >10
GROUP BY rain_year ORDER BY rain_count DESC
),

 temperatue AS (
SELECT year(record_date) AS temp_year, count(*) AS temp_count
FROM SRC_WEATHER_DATA.GOLD.DAILYTEMPRATURE
WHERE dailytemprature >28
GROUP BY temp_year ORDER BY temp_count DESC
),

 snoww AS (
SELECT year(record_date) AS snow_year, count(*) AS snow_count
FROM SRC_WEATHER_DATA.GOLD.DAILYSNOWFALL
WHERE dailysnowfall >30
GROUP BY snow_year ORDER BY snow_count DESC 
),

 windd AS (
SELECT year(record_date) wind_year, count(*) AS wind_count
FROM SRC_WEATHER_DATA.SILVER.HOURLYWINDSPEED WHERE hourlywindspeed >60
GROUP BY wind_year ORDER BY wind_count DESC ), 

 years AS (
 SELECT DISTINCT year (record_date) AS year_anomaly  FROM SRC_WEATHER_DATA.GOLD.DAILYRAINFALL
 )

SELECT   year_anomaly
        ,coalesce (rainn.rain_count,0) AS rain
        ,coalesce(windd.wind_count,0) AS wind
        ,coalesce(snoww.snow_count,0) AS snow
        ,coalesce (temperatue.temp_count,0) AS temp
       , sum(coalesce (rainn.rain_count,0) + coalesce(windd.wind_count,0) + coalesce(snoww.snow_count,0) +coalesce (temperatue.temp_count,0)) AS summm
         
FROM years
LEFT JOIN rainn
ON year_anomaly=rainn.rain_year
LEFT JOIN snoww
ON year_anomaly = snoww.snow_year
LEFT JOIN windd 
ON  year_anomaly=windd.wind_year
LEFT JOIN temperatue 
ON year_anomaly = temperatue.temp_year
GROUP BY 1,2,3,4,5
ORDER BY summm DESC
LIMIT 1
 ;

