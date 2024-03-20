#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 13:04:29 2024

@author: deva
"""

from sqlalchemy import create_engine,MetaData,Table,Column,String,Integer,Float,Date
import pandas as pd
import datetime
from datetime import datetime
engine = create_engine('sqlite:///scd2.db')
con = engine.connect()
meta = MetaData()
prod = Table('product',meta,
             Column('prod_id',Integer,primary_key = True),
             Column('prod_name',String),
             Column('Prod_price',Float),
             Column('Prod_cost',Float),
             Column('Start_date',Date),
             Column('prod_cat_id',Integer)
             )

def update_scd2(engine,table,prod_id,new_price):
    
    prod_df = pd.read_sql(table, engine)
    if 'End_date' not in prod_df.columns:
        prod_df['End_date'] = pd.NaT
    if 'prod_sur_id' not in prod_df.columns:
        prod_df['prod_sur_id'] = range(100,len(prod_df)+100)
    
    current_time = pd.to_datetime(datetime.now().date()).date()
    yesterday = current_time  - pd.Timedelta(days = 1)

    prod_df.loc[(prod_df['prod_id'] == 7) & prod_df['End_date'].isna(),['End_date']] = yesterday
    maxi = prod_df['prod_sur_id'].max()
    new_row = {'prod_id':prod_id,'prod_sur_id':maxi+1,'prod_price':new_price,
                'Start_date':current_time,'End_date':pd.NaT}
    existing_product = prod_df[prod_df['prod_id'] == prod_id].iloc[0]
    new_row['prod_name'] = existing_product['prod_name']
    new_row['prod_cat_id'] = existing_product['prod_cat_id']
    new_row['prod_cost'] = existing_product['prod_cost']
    new_df = pd.DataFrame([new_row])
    prod_df = pd.concat([prod_df,new_df],ignore_index = True)    
    prod_df['Start_date'] = pd.to_datetime(prod_df['Start_date']).dt.date  
    prod_df['End_date'] = pd.to_datetime(prod_df['End_date']).dt.date  
# =============================================================================
    prod_df.to_sql('product',engine,index = False,if_exists = 'replace')
# =============================================================================
    return prod_df

result = update_scd2(engine,'product',7,3.99)
result_df = result[result['End_date'].isna()]
result_df.to_sql('prod_dim_scd2',engine,index = False,if_exists = 'replace')
