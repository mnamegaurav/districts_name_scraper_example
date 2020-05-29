#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
import requests

url = 'https://en.wikipedia.org/wiki/List_of_districts_in_India'
html_code = requests.get(url).text

soup = BeautifulSoup(html_code,'lxml')

districts = []

tables = soup.find_all('table',class_='wikitable')[1:29] # Should use find_all() for all tables
state_id = 1
for table in tables:
    tbody = table.find('tbody') # Only one tbody for one table
    rows = tbody.find_all('tr')[1:] # Should use find_all() for all tables
    for row in rows:
        td = row.find_all('td')[2]
        district_name = td.a.text
        districts.append((state_id,district_name))
    state_id+=1


# In[ ]:


print(districts)
print(len(districts))


# In[ ]:


import psycopg2
from datetime import datetime

conn = psycopg2.connect(dbname='',host='',password='')
cur = conn.cursor()

i = 1
for district in districts:
    print(district,i)
    try:
        cur.execute("INSERT INTO public.config_district (district_id, district_name, state_id, is_active, created_by_id, created_on, updated_by_id, updated_on)     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",(i, district[1],district[0],True,1,datetime.now(),1,datetime.now()))
        i+=1
    except psycopg2.Error as e:
        i+=1
    
conn.commit()
conn.close()

