# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 21:29:49 2021

@author: adeel
"""
import psycopg2

conn = psycopg2.connect(dbname="test", user="postgres", password="postgres")
cur = conn.cursor()

# Query VIEW
cur.execute("""
DROP VIEW exercise3_view;
CREATE VIEW exercise3_view AS
SELECT
	t2.extraction_date AS date_cases,
	t2.year_week, 
	t2.cumulative_count, 
	t2.rate_14_day,
	cast(t2.cumulative_count as numeric)/t2.population*100000 AS cases_per_100k,
	t1.* 
FROM countries AS t1
JOIN cases AS t2 ON t1.country = t2.country
WHERE t2.year_week = (
    SELECT MAX(year_week)
    FROM cases
)
AND t2.indicator_col = 'cases';
""")


# Query # 1 - Most cases per 100k habitants
cur.execute("""
SELECT 
	country, 
	cumulative_count,
	population,
	cast(cumulative_count as numeric)/population*100000 AS cases_per_100k
FROM cases
WHERE year_week = '2020-31'
ORDER by cases_per_100k DESC
LIMIT 1
""")

# Query # 2 - Least cases per 100k habitants
cur.execute("""
SELECT 
	country, 
	cumulative_count,
	population,
	cast(cumulative_count as numeric)/population*100000 AS cases_per_100k
FROM cases
WHERE year_week = '2020-31'
ORDER by cases_per_100k ASC
LIMIT 10
""")

# Query #3 - Most cases among the 20 richest countries
cur.execute("""
SELECT *
FROM
	(SELECT 
		t1.country,
		t1.gdp,
		MAX(t2.cumulative_count) AS cases
	FROM countries AS t1
	JOIN cases AS t2 ON t1.country = t2.country
	GROUP BY t1.country
	ORDER BY gdp DESC
	LIMIT 20) AS richest_20
ORDER BY cases DESC
LIMIT 10
""")

# Query #5 - Cases per million in each region and population density
cur.execute("""
SELECT 
		t1.region,
		SUM(cast(t2.cumulative_count AS numeric)/t2.population*1000000) AS cases_per_million,
		SUM(t1.population) AS region_population,
		SUM(t1.area) region_area,
		SUM(t1.population)/SUM(t1.area) AS region_density
FROM countries AS t1
JOIN cases AS t2 ON t1.country = t2.country
WHERE t2.year_week = '2020-31'
GROUP BY t1.region
ORDER BY cases_per_million DESC
""")

# Query #5 - Duplicates
cur.execute("""
SELECT 
	country,
	country_code,
	continent,
	population,
	indicator_col,
	weekly_count,
	year_week,
	cumulative_count,
	source_col,
	rate_14_day,
	count(*)
FROM cases
GROUP BY 
	country,
	country_code,
	continent,
	population,
	indicator_col,
	weekly_count,
	year_week,
	cumulative_count,
	source_col,
	rate_14_day
HAVING count(*) > 1;

SELECT 
	country,
	year_week,
	indicator_col,
	count(*)
FROM cases
GROUP BY 
	country,
	year_week,
	indicator_col
HAVING count(*) > 1;


SELECT *
FROM
 (
   SELECT 
	country,
	country_code,
	continent,
	population,
	indicator_col,
	weekly_count,
	year_week,
	cumulative_count,
	source_col,
	rate_14_day,
    row_number() over(partition BY country, country_code, continent, population, indicator_col, weekly_count, year_week, cumulative_count, source_col, rate_14_day) rn
    FROM cases
 ) dt
WHERE rn >1;
""")

# Query #6
# Add EXPLAIN ANALYZE and check planning and execution times.
# Possible suggestions include adding indexes on recurring columns used in the queries (for example countries which is used on joins and year_week which is used on from statements)

# Query #7 - Extras - 10 european countries with most delta variant cases in a week, among the 20 richest european countries.
cur.execute("""
SELECT *
FROM
	(SELECT 
		t1.country,
		t1.gdp,
		MAX(t2.number_detections_variant) as highest_delta_cases_week
	FROM countries AS t1
	JOIN cases_variant AS t2 ON t1.country = t2.country
	WHERE variant = 'B.1.617.2'
	GROUP BY t1.country
	ORDER BY gdp desc
	LIMIT 20) AS richest_20
ORDER BY highest_delta_cases_week DESC
LIMIT 10
""")

# Query #8 - Extras - Sum of cases by age group.
cur.execute("""
SELECT 
	age_group, 
	SUM(new_cases) AS cumulative_cases
FROM cases_age
GROUP BY age_group
ORDER BY cumulative_cases DESC
""")

# Query #9 - Extras - Weekly cases overall. Trends upward.
cur.execute("""
SELECT year_week, SUM(weekly_count)
FROM cases
GROUP BY year_week
ORDER BY year_week DESC
""")

# Query #10 - Extras - Weekly cases in Panama. Trends downward.
cur.execute("""
SELECT country, year_week, SUM(weekly_count)
FROM cases
WHERE country = 'Panama'
GROUP BY country, year_week
ORDER BY year_week DESC
""")