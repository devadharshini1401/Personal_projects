import pandas as pd
from sqlalchemy import create_engine,Table,Column,String,Integer,Date,MetaData
metadata = MetaData()
engine = create_engine('sqlite:///db_deva.db')
read_file = pd.read_excel('market.xlsx',usecols='D')
from datetime import datetime
unique = read_file['businessname'].drop_duplicates()
df_unique = pd.DataFrame(unique)
df_unique.rename(columns={'businessname':'Hospital_name'},inplace=True)
df_unique['Hospital_id'] = range(500,len(df_unique)+500)
today = datetime.today().strftime('%Y-%m-%d')
df_unique['created_date'] = today

#Creating metadata/schema for the hospital sql table:

hospital_table = Table(
    'hospital', metadata,
    Column('Hospital_id', Integer, primary_key=True),
    Column('Hospital_name', String),
    Column('Create_date', Date)
)
metadata.create_all(engine)
df_unique.to_sql('hospital',engine, if_exists='replace', index=False)