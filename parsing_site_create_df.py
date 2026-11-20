This code helps to parsing new  price list for ADF (extract only needed data) and create dataframe.
Then we have to save this df to dbfs, otherwise we extract previous df  and compare to current one to understand if we have changes in price.
If we have changes we add new data to dbo.table with new date










import json
from bs4 import BeautifulSoup
import requests
import pandas as pd
url ='https://azure.microsoft.com/en-us/pricing/details/data-factory/data-pipeline/'
req = requests.get(url)
soup = BeautifulSoup(req.content, 'html.parser')

td = soup.find(lambda tag: tag.name == 'td' and 'Data movement Activity' in tag.get_text())
span1 = json.loads(td.find_next_siblings('td')[0].find("span", class_="price-data")['data-amount'])
span2= json.loads(td.find_next_siblings('td')[1].find("span", class_="price-data")['data-amount'])
span3 = json.loads(td.find_next_siblings('td')[2].find("span", class_="price-data")['data-amount'])
name1='Data movement Activity'
print(f'{name1}',  span1['regional']['europe-west'], span2['regional']['europe-west'], span3['regional']['europe-west'])

td = soup.find(lambda tag: tag.name == 'td' and 'Orchestration' in tag.get_text())
span4 = json.loads(td.find_next_siblings('td')[0].find("span", class_="price-data")['data-amount'])
span5= json.loads(td.find_next_siblings('td')[1].find("span", class_="price-data")['data-amount'])
span6 = json.loads(td.find_next_siblings('td')[2].find("span", class_="price-data")['data-amount'])

name2='Orchestration'
print(f'{name2}',  span4['regional']['europe-west'], span5['regional']['europe-west'], span6['regional']['europe-west'])

td = soup.find(lambda tag: tag.name == 'td' and 'Pipeline Activity' in tag.get_text())
import json
span7 = json.loads(td.find_next_siblings('td')[0].find("span", class_="price-data")['data-amount'])
span8= json.loads(td.find_next_siblings('td')[1].find("span", class_="price-data")['data-amount'])
span9 = json.loads(td.find_next_siblings('td')[2].find("span", class_="price-data")['data-amount'])

name3='Pipeline Activity'
print(f'{name3}',  span7['regional']['europe-west'], span8['regional']['europe-west'], span9['regional']['europe-west'])


td = soup.find(lambda tag: tag.name == 'td' and 'External Pipeline Activity' in tag.get_text())
import json
span10 = json.loads(td.find_next_siblings('td')[0].find("span", class_="price-data")['data-amount'])
span11= json.loads(td.find_next_siblings('td')[1].find("span", class_="price-data")['data-amount'])
span12 = json.loads(td.find_next_siblings('td')[2].find("span", class_="price-data")['data-amount'])

name4='External Pipeline Activity'
print(f'{name4}',  span10['regional']['europe-west'], span11['regional']['europe-west'], span12['regional']['europe-west'])

import datetime
from datetime import datetime
    
now = datetime.now() 
day = now.date()
data = {f'{name1}': [span1['regional']['europe-west'], span2['regional']['europe-west'], span3['regional']['europe-west']],
f'{name2}': [span4['regional']['europe-west'], span5['regional']['europe-west'], span6['regional']['europe-west']],
f'{name3}': [span7['regional']['europe-west'], span8['regional']['europe-west'], span9['regional']['europe-west']], 
f'{name4}': [span10['regional']['europe-west'], span11['regional']['europe-west'], span12['regional']['europe-west']]}


f_table=pd.DataFrame.from_dict(data)
f_table=f_table.T
f_table.head(4)

day = datetime.now().date()
req = requests.get(url)
soup = BeautifulSoup(req.content, 'html.parser')
dict = soup.find(lambda tag: tag.name == 'script' and 'global.currencyData' in tag.get_text())
import re
currency_data = json.loads(re.search(r'global\.currencyData\s*=\s*(\{.*?\});', dict.get_text(), re.S).group(1))
exch=currency_data['eur']['modernConversion']
exch_day = {f'{day}': [exch]}
exch_table=pd.DataFrame.from_dict(exch_day)
exch_table=exch_table.reset_index(drop=True)
exch_table.head()
exch_table.iloc[0,0]

#we extract table from dbfs from previous day
from datetime import timedelta
from datetime import datetime

day = datetime.now().date()

# Format the filename with the previous day's date
file_name= f'final_{day - timedelta(days=1)}.csv'
df_last =  pd.read_csv(f"/dbfs/FileStore/{file_name}")
df_last=df_last.reset_index(drop=True)
df_last=df_last.drop(columns='Unnamed: 0')
df_last

current_table= f_table.rename({f_table.columns[0]: header_columns[0], f_table.columns[1]: header_columns[1], f_table.columns[2]: header_columns[2]}, axis=1)
final_day=current_table
final_day_=final_day.reset_index(drop=True)
final_day_.head()

#save current day table
import pandas as pd
from io import StringIO
import datetime


day = datetime.now().date()
now = datetime.now() 
day = now.date()
current_table.to_csv(f'/dbfs/FileStore/final_{day}.csv')

if exch_table_last.iloc[0,0]==exch_table.iloc[0,0]:
  print(f"no changes, {day}")
  append_rate=0
else:
  print("new exchange")
  append_rate=1

if df_last.equals(final_day_):
  print(f"no new data, {day}")
  append=0
else:
  print("new data")
  append=1

if append==1:
    import uuid
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
    from pyspark.sql.functions import current_timestamp
 
    def write_price1 (name_by_site, name_etl, notes, price):
 
 
        """
        [Id] [char](36) NOT NULL,
	    [Site] [nvarchar](128) NOT NULL,
	    [Etl] [nvarchar](128) NOT NULL,
	    [Date] [datetime2](7) NOT NULL,
	    [Price] [decimal](8, 6) NOT NULL,
	    [Notes] [nvarchar](128) NULL
        """
        fields = [
            StructField("Id",StringType(), True),
            StructField("Site", StringType(), True), \
            StructField("Etl", StringType(), True), \
            StructField("Date", TimestampType(), True), \
            StructField("Price", IntegerType(), True), \
            StructField("Notes", StringType(), True) \
        ]
        schema = StructType(fields)
 
        df_final = spark.createDataFrame([], schema)
 
        columns = ["Id", "Site", "Etl", "Price", "Notes"]
        id = str(uuid.uuid4())
        new_row = spark.createDataFrame([(id, name_by_site, name_etl, float(price), notes)], columns)
        new_row = new_row.withColumn("Date", current_timestamp()).select(*schema.fieldNames())
        df_final = df_final.union(new_row)
        df_final.createOrReplaceTempView("df_final")
 
        df_final.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", AZURE_SERVER_URL + ';encrypt=false;trustServerCertificate=true') \
            .option("dbtable", "dbo.ActivityPrice") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .save()
 
    name_by_site = "Pipeline Activity"
    name_etl = "Look up etc"
    notes= "in hour"
    price = final_day_E.at[2, 'Azure Integration Runtime Price']
 
    write_price1 (name_by_site, name_etl, notes, price)

