# -*- coding: utf-8 -*-
"""
Created on Tue Aug 31 19:40:06 2021

@author: adeel
"""

import psycopg2
import csv

conn = psycopg2.connect(dbname="test", user="postgres", password="postgres")
cur = conn.cursor()

# Insert into countries table
# On conflict do nothing, this table shouldn't be changing frequently
# Could have been done with a function, same code.
with open('.\Data\countries%20of%20the%20world.csv', 'r') as f:
    reader = csv.reader(f)
    # Skip header row
    next(reader)
    for row in reader:
        cur.execute(
        """INSERT INTO countries VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country)
        DO NOTHING""",
        row
    )

# Insert into cases table 
# On conflict update the old values      
with open('.\Data\covid.csv', 'r') as f:
    reader = csv.reader(f)
    # Skip the header row
    next(reader) 
    for row in reader:
        cur.execute(
        """INSERT INTO cases VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country, year_week, indicator_col)
        DO UPDATE SET cumulative_count = excluded.cumulative_count, weekly_count = excluded.weekly_count, rate_14_day = excluded.rate_14_day, population = excluded.population, extraction_date = excluded.extraction_date""",
        row
    )
        
# Insert into cases variant table 
# On conflict update the old values      
with open(r'.\Data\variants.csv', 'r') as f:
    reader = csv.reader(f)
    # Skip the header row
    next(reader) 
    for row in reader:
        cur.execute(
        """INSERT INTO cases_variant VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country, year_week, variant)
        DO UPDATE SET new_cases = excluded.new_cases, number_sequenced = excluded.number_sequenced, percent_cases_sequenced = excluded.percent_cases_sequenced, number_detections_variant = excluded.number_detections_variant, percent_variant = excluded.percent_variant, extraction_date = excluded.extraction_date""",
        row
    )
        
# Insert into cases variant table 
# On conflict update the old values      
with open(r'.\Data\age_group_cases.csv', 'r') as f:
    reader = csv.reader(f)
    # Skip the header row
    next(reader) 
    for row in reader:
        cur.execute(
        """INSERT INTO cases_age VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country, year_week, age_group)
        DO UPDATE SET new_cases = excluded.new_cases, population = excluded.population, rate_14_day_100k = excluded.rate_14_day_100k, extraction_date = excluded.extraction_date""",
        row
    )
conn.commit()