#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 11:39:08 2024

@author: deva
"""
from sqlalchemy import Table, Column, Integer, String, MetaData,create_engine,select
from sqlalchemy import and_
# Create a metadata object
meta = MetaData()
engine = create_engine('sqlite:///guests.db')
# Define the 'Guest' table
guest = Table('Guest', meta,
              Column('Name', String),
              Column('Phone', Integer),
              Column('City', String),
              Column('Pro_fig', String)
             )

# Define the 'Customer' table
customer = Table('Customer', meta,
                 Column('C_id', Integer, primary_key=True, autoincrement=True),
                 Column('C_name', String),
                 Column('C_phone', Integer),
                 Column('C_city', String)
               )

# Define the 'Call_plan' table
call_plan = Table('Call_plan', meta,
                  Column('Call_id', Integer, primary_key=True, autoincrement=True),
                  Column('Call_name', String),
                  Column('Call_phone', Integer),
                  Column('Call_city', String)
                )
con = engine.connect()
g_select = con.execute(guest.select()).fetchall()
c_select = con.execute(customer.select()).fetchall()
call_select = con.execute(call_plan.select()).fetchall()
for guest_record in g_select:
    guest_name = guest_record['Name']
    guest_phone = guest_record['Phone']
    guest_city = guest_record['City']
    
    #checking for existing records
    existing_query = customer.select().where(and_(
        customer.c.C_name == guest_name,
        customer.c.C_phone == guest_phone,
        customer.c.C_city ==guest_city))
    
    
    existing_record = con.execute(existing_query).fetchall()
    
    if existing_record:
        delete_query = guest.delete().where(and_(
            guest.c.Name == guest_name,
            guest.c.Phone == guest_phone,
            guest.c.City == guest_city))
        con.execute(delete_query)
        
    elif guest_record['Pro_fig']!='Y':
        
        insert_query = call_plan.insert().values(
            Call_name = guest_name,
            Call_phone = guest_phone,
            Call_city = guest_city)
        
        update_query = guest.update().where(and_(
            guest.c.Name == guest_name,
            guest.c.Phone == guest_phone,
            guest.c.City == guest_city)).values(Pro_fig = 'Y')
        
        con.execute(insert_query)
        con.execute(update_query)
        

    