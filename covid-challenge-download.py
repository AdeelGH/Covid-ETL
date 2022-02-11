# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 19:40:20 2021

@author: adeel
"""

import urllib.request, json 
import csv
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
# Authenticate Kaggle API from User folder, documentation @ https://github.com/Kaggle/kaggle-api
api = KaggleApi()
api.authenticate()

# Download countries of the world from Kaggle dataset into data folder.
api.dataset_download_file(dataset = 'fernandol/countries-of-the-world', file_name = 'countries of the world.csv', path='.\Data')

# Read it as a dataframe to finish cleaning it up.
df = pd.read_csv ('.\Data\countries%20of%20the%20world.csv', encoding='iso-8859-1', decimal=',')

# Fill empty values with zeroes
df.fillna(0, inplace=True)

# Remove extra white space
df['Country'] = df['Country'].map(lambda x: x.strip())

# Add extraction date and write with '.' as decimal separator
df['extraction_date'] = pd.to_datetime('today')
df.to_csv('.\Data\countries%20of%20the%20world.csv', sep=',', encoding='iso-8859-1', index=False, decimal='.')
        
# Request covid data from ECDC
with urllib.request.urlopen('https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json') as url:
    data = json.loads(url.read().decode())

# Replace extra commas    
for item in data:
    item['country'] = item['country'].replace(',', '')
    item['source'] = item['source'].replace(',', '')

# Write JSON file as csv
with open('.\Data\covid.csv','w',newline='') as f:
    title = 'country,country_code,continent,population,indicator,weekly_count,year_week,cumulative_count,source,rate_14_day'.split(',')
    cw = csv.DictWriter(f,title,delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    cw.writeheader()
    cw.writerows(data)    

# Open csv to finish cleaning up
df = pd.read_csv ('.\Data\covid.csv', encoding='iso-8859-1')
# Add extraction date
df['extraction_date'] = pd.to_datetime('today')
# Fill empty values with zeroes.
df.rate_14_day.fillna(0, inplace=True)

# Download covid data into data folder. 
df.to_csv(r'.\Data\covid.csv', sep=',', encoding='iso-8859-1', index=False)

# Additional files to enrich information
# I decided to use additonal files from ECDC since they work with similar structure.
# I'm especially interested in finding more about the delta variant and cases by age groups.
df = pd.read_csv('https://opendata.ecdc.europa.eu/covid19/virusvariant/csv/data.csv', encoding='iso-8859-1')
# Add extraction date
df['extraction_date'] = pd.to_datetime('today')
# Fill empty values with zeroes.
df.fillna(0, inplace=True)
# Download variant data. 
df.to_csv(r'.\Data\variants.csv', encoding='iso-8859-1', sep=',', index=False)

# Repeat with age group data, could have created a function, same process.
df = pd.read_csv('https://opendata.ecdc.europa.eu/covid19/agecasesnational/csv/data.csv', encoding='iso-8859-1')
# Add extraction date
df['extraction_date'] = pd.to_datetime('today')
# Fill empty values with zeroes.
df.fillna(0, inplace=True)
# Download age group data. 
df.to_csv(r'.\Data\age_group_cases.csv', encoding='iso-8859-1', sep=',', index=False)

# Write JSON file, optional, used to troubleshoot.
with open('.\Data\covid.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
    


