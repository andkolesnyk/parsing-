# parsing-

This repository contains code of solution to pasre price list for Azure Data Factory.
Code resolves task  to extract data (some of them), create df. 
Next task  is to find date when price for Azure Data Factory would be changed. Every day we save df to dbfs system and extract next day and compare iwth 'fresh' price list
When we find out changes we add new data to table T-Sql.
As we work in Europe area it important to pay attention exchange rate EUR/USD. I repeat  for exchange rate the same procedure as  for price list.