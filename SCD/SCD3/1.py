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
engine = create_engine('sqlite:///scd3.db')
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

def update_scd3(engine, table, prod_id, new_price):
    # Fetch existing product information
    query = f"SELECT * FROM {table} WHERE prod_id = :prod_id"
    existing_product = pd.read_sql(query, engine, params={"prod_id": prod_id})

    current_time = pd.to_datetime(datetime.now().date()).date()
    yesterday = current_time - pd.Timedelta(days=1)

    # If no existing record found, insert a new record with historical values as current values
    if existing_product.empty:
        new_row = {
            'prod_id': prod_id,
            'prod_name_current': None,
            'prod_price_current': None,
            'Prod_cost_current': None,
            'Start_date_current': current_time,
            'End_date_current': pd.NaT,
            'prod_name_historical': None,
            'Prod_price_historical': None,
            'Prod_cost_historical': None,
            'Start_date_historical': None,
            'End_date_historical': yesterday
        }
    else:
        # Update historical values with the current values before updating current values
        existing_product['prod_name_historical'] = existing_product['prod_name_current']
        existing_product['Prod_price_historical'] = existing_product['Prod_price_current']
        existing_product['Prod_cost_historical'] = existing_product['Prod_cost_current']
        existing_product['Start_date_historical'] = existing_product['Start_date_current']
        existing_product['End_date_historical'] = existing_product['End_date_current']

    # Update current values
    existing_product['prod_name_current'] = existing_product['prod_name_current'].fillna(existing_product['prod_name_historical'])
    existing_product['Prod_price_current'] = existing_product['Prod_price_current'].fillna(existing_product['Prod_price_historical'])
    existing_product['Prod_cost_current'] = existing_product['Prod_cost_current'].fillna(existing_product['Prod_cost_historical'])
    existing_product['Start_date_current'] = current_time
    existing_product['End_date_current'] = pd.NaT

    # Update new price
    existing_product.loc[existing_product['prod_id'] == prod_id, 'Prod_price_current'] = new_price

    # Convert dates to proper format
    existing_product['Start_date_current'] = pd.to_datetime(existing_product['Start_date_current']).dt.date
    existing_product['End_date_current'] = pd.to_datetime(existing_product['End_date_current']).dt.date
    existing_product['Start_date_historical'] = pd.to_datetime(existing_product['Start_date_historical']).dt.date
    existing_product['End_date_historical'] = pd.to_datetime(existing_product['End_date_historical']).dt.date

    # Write updated DataFrame back to the database
    existing_product.to_sql(table, engine, index=False, if_exists='replace')

# Call the function to update SCD3 for product with prod_id = 7
update_scd3(engine, 'product', 7, 8.99)
