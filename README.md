# Stockholm Weather Data Analysis

This repository contains SQL code for analyzing weather data of Stockholm, using a structured approach in Snowflake.

## Overview

The project involves extracting, transforming, and loading (ETL) weather data from a weather website into Snowflake. The data is organized into three layers (Bronze, Silver, Gold) to streamline analysis. The SQL code addresses key weather-related questions.

## Data Processing

### Layers

1. **Bronze Layer:** Raw weather data.
2. **Silver Layer:** Hourly aggregated data for temperature, rainfall, snowfall, and wind speed.
3. **Gold Layer:** Daily aggregated data for comprehensive analysis.

### Techniques Used

- **Common Table Expressions (CTEs):** For managing intermediate query results.
- **Window Functions:** For efficient data aggregation and ranking.

## SQL Analysis

The `weather_analysis.sql` file includes SQL code for the following questions:

1. **Hottest Summer Month Each Year:** Identify the hottest month each summer.
2. **Hottest Day in Summer:** Find the hottest day during the summer months.
3. **Coldest Month Each Year:** Identify the coldest month each winter.
4. **Coldest Day in Winter:** Find the coldest day during the winter months.
5. **Windiest Month Each Year:** Determine the month with the highest average wind speed.
6. **Windiest Day in Winter:** Identify the windiest day during winter and calculate the average wind speed for each year.
7. **Rainiest Month Each Year:** Find the month with the highest rainfall.
8. **Snowiest Month Each Year:** Determine the month with the highest snowfall.
9. **Spring Start Date Each Year:** Calculate the earliest date of spring based on consecutive days of temperatures above 0°C.
10. **Year with Most Weather Anomalies:** Identify the year with the most extreme weather events.

## How to Use

1. **Set Up Snowflake:** Ensure you have access to a Snowflake instance.
2. **Upload the CSV:** Upload the weather.csv from your local machine into Snowflake.
3. **Run SQL Code:** Execute the SQL statements in `weather_analysis.sql` in Snowflake to create views and perform analysis.

## Files

- `weather_analysis.sql`: Contains all the SQL code for data processing and analysis.
- `README.md`: This file.

## Conclusion

This project provides a structured approach to analyzing weather data in Snowflake, utilizing CTEs and window functions to efficiently answer key weather-related questions.

## License

Licensed under the MIT License.
