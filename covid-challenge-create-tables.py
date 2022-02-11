# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 19:40:06 2021

@author: adeel
"""

import psycopg2

# Connection setup
conn = psycopg2.connect(dbname="test", user="postgres", password="postgres")
cur = conn.cursor()

# Drops view created in exercise #3
cur.execute("""
    DROP VIEW IF EXISTS exercise3_view;
""")

# Creates countries table, primary key is country
cur.execute("""
    DROP TABLE IF EXISTS countries;
    CREATE TABLE countries(    
    country TEXT primary key,
    region TEXT,
    population BIGINT,
    area BIGINT,
    pop_density NUMERIC (10,1),
    coastline NUMERIC (7,2),
    migration NUMERIC (7,2),
    infant_mortality NUMERIC (7,2),
    gdp NUMERIC (7,2),
    literacy NUMERIC (4,1),
    phones NUMERIC (7,2),
    arable NUMERIC (7,2),
    crops NUMERIC (7,2),
    other NUMERIC (7,2),
    climate NUMERIC (3,1),
    birth_rate NUMERIC (7,2),
    death_rate NUMERIC (7,2),
    agriculture NUMERIC (4,3),
    industry NUMERIC (4,3),
    service NUMERIC (4,3),
    extraction_date DATE
);
""")

# Creates cases table, unique index is made up of country, year_week and indicator_col
cur.execute("""
    DROP TABLE IF EXISTS cases;
    CREATE TABLE cases(   
    country TEXT,
    country_code TEXT,
    continent TEXT,
    population BIGINT,
    indicator_col TEXT,
    weekly_count BIGINT,
    year_week TEXT,
    cumulative_count BIGINT,
    source_col TEXT,
    rate_14_day NUMERIC (12,6),
    extraction_date DATE
);
CREATE UNIQUE INDEX index_country_week_indicator
ON cases (country, year_week, indicator_col);
""")

# Creates variants table, unique index is made up of country, year_week and variant
cur.execute("""
    DROP TABLE IF EXISTS cases_variant;
    CREATE TABLE cases_variant(   
    country TEXT,
    country_code TEXT,
    year_week TEXT,
    source_col TEXT,
    new_cases NUMERIC (10,1),
    number_sequenced BIGINT,
    percent_cases_sequenced NUMERIC (4,1),
    valid_denominator TEXT,
    variant TEXT,
    number_detections_variant NUMERIC (10,1),
    percent_variant NUMERIC (4,1),
    extraction_date DATE
);
CREATE UNIQUE INDEX index_country_week_variant
ON cases_variant (country, year_week, variant);
""")

# Creates age groups table, unique index is made up of country, year_week and age group
cur.execute("""
    DROP TABLE IF EXISTS cases_age;
    CREATE TABLE cases_age(   
    country TEXT,
    country_code TEXT,
    year_week TEXT,
    age_group TEXT,
    new_cases NUMERIC (10,1),
    population BIGINT,
    rate_14_day_100k NUMERIC (12,6),
    source_col TEXT,
    extraction_date DATE
);
CREATE UNIQUE INDEX index_country_week_age
ON cases_age (country, year_week, age_group);
""")


conn.commit()