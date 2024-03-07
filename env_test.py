import pandas as pd
from datetime import datetime as dt
import numpy as np
from calendar import month_name,monthrange
import pyodbc
from sqlalchemy import URL,create_engine
from utils.sharepoint import read_file_from_sharepoint
import json
from utils.db_connector_sql import DBConnector
from process.reading_env_data import Env
from sqlalchemy import select,MetaData,create_engine,Table,inspect
from sqlalchemy.engine import URL
import inspect
from utils.log_module import Log
import traceback



connection_url = URL.create(
        "mssql+pyodbc",
        username="azuremdg.pelsqldb",
        password="$pEl*@driYOd",
        host="grcd-az-mdg-pp-sql-01.database.windows.net",
        port=1433,
        database="GRCD-AZ-PEL-PP-DBA-01",
        query={
            "driver": "ODBC Driver 17 for SQL Server"
        },
        )
engine=create_engine(connection_url,future=True)


bad_tags=pd.DataFrame(columns=['time','tag_name','value','plant'])
good_tags=pd.DataFrame(columns=['time','tag_name','value','plant'])

def ip_details():
    ips={'Vilayat': '10.3.35.12', 
     'Nagda': '10.26.0.38',
     'Renukoot': '10.3.66.51', 
     'Rehla': '10.196.0.21',
     'Ganjam': '10.3.44.16', 
     'Karwar': '10.3.1.40', 
     'Veraval': '10.6.0.74', 
     'BBPuram': '10.65.0.12'}
    plt_abbr={'Vilayat': 'VIL', 
     'Nagda': 'NAD',
     'Renukoot': 'RKT', 
     'Rehla': 'RHL',
     'Ganjam': 'GAN', 
     'Karwar': 'KWR', 
     'Veraval': 'VRL', 
     'BBPuram': 'BBP'}
    plt_code={'Vilayat': 544, 
     'Nagda': 480,
     'Renukoot': 449, 
     'Rehla': 464,
     'Ganjam': 2549, 
     'Karwar': 2541, 
     'Veraval': 504, 
     'BBPuram': 3715}
    return ips,plt_abbr,plt_code

def return_db_cursor():
    conn=pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server='grcd-az-mdg-pp-sql-01.database.windows.net',
                        database='GRCD-AZ-PEL-PP-DBA-01',uid='azuremdg.pelsqldb',pwd='$pEl*@driYOd'
                        )
    cursor=conn.cursor()
    return cursor

def return_month_date(mon,year):
    if mon==1:
        return dt(day=monthrange(year=year-1,month=12)[1],month=12,year=year-1)
    else:
        return dt(day=monthrange(year=year,month=mon-1)[1],month=mon-1,year=year)

def delete_waste_data_by_month(df,date,cur):
    df_waste=df[(df['type']=='waste')]
    if not df_waste.empty:
        print("Deleting Waste Data by Month.......")
        stmt=f"delete from [sustainability_snapshot] where date='{date}'"
        print(stmt)
        cur.execute(stmt)
        cur.commit()
    else:
        print("No waste data to delete")

def delete_emission_by_date(today,cur):
    print("Deleting Emission Data by Month.......")
    delete_stmt=f"""delete FROM [dbo].[sustainability_snapshot] where type='Emission' 
                    and CONVERT(date,date)='{today.date()}'"""
    print(delete_stmt)
    cur.execute(delete_stmt)
    cur.commit()


def check_alarm(value,alarm_value):
    print(alarm_value)
    if pd.isna(alarm_value):
        return 0
    else:
        if value>alarm_value:
            return 1
        return 0

def upload_data(df,cursor):
    print("Uploading Environment data................")
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            datatype=str(df[col].dtype)
            df[col] = np.where(df[col].isnull(), None, df[col])
            if 'datetime' in datatype:
                df[col]=pd.to_datetime(df[col])
    params=list(tuple(row) for row in df.values)
    cursor.fast_executemany=True
    sql="insert into sustainability_snapshot values (?,?,?,?,?,?,?,?,?)"
    cursor.executemany(sql,params)
    cursor.commit()

def return_unique_id(df):
    date=df['date'].apply(lambda x:str(x.timestamp()))
    df['u_id']=df['plant'] + df['type'] + df['tag_name/parameter'] + "|" +date
    return df

def main(env:Env,cur,db_connect:DBConnector,today):
    try:
        ips,plt_abbr,plt_code=ip_details()
        month_date=return_month_date(today.month,today.year)
        
        # final_df,master_df=env.read_environment_tag(ips,plt_abbr,plt_code,cur,month_date)
        # # final_df['date']=final_df['date'].apply(lambda x:dt.strptime(x,format=''))
        # final_df.to_csv('test.csv',index=False)
        # final_df=return_unique_id(final_df)
        # final_df.to_csv('tag_values.csv',index=False)
        # master_df.to_csv('master_values.csv',index=False)
        # delete_waste_data_by_month(final_df,month_date,cur)
        # # delete_emission_by_date(dt.today(),cur=cur)
        # upload_data(final_df,cur)
        # print(final_df.head())
        # db_connect.write_df_to_table(df=final_df,table='sustainability_snapshot',update=True)
        # db_connect.write_df_to_table(df=master_df,table='master_sustainability',update=True)
        log.capture_trace(inspect.stack())
        raise ValueError
    except Exception as exc:
        # print(exc.__class__)
        # pass
        log.capture_trace(exc)
        # log.capture_trace(traceback.print_exc())
        
            
            
            


if __name__=="__main__":
    cfg=json.load(open("db_cred.json"))
    # print(cfg)
    env=Env()
    log=Log()
    # log=Log()
    db_connect=DBConnector(cfg.get('Azure_sql'))
    # filepath="DCS Ambient, Emission & Water Monitoring_v1.xlsx"
    # read_file_from_sharepoint(filepath)
    # df=pd.read_excel('DCS Ambient, Emission & Water Monitoring_v1.xlsx',sheet_name='Combined')
    # waste_info=pd.read_csv('Waste Config.csv')
    today=dt.today()
    # today=dt(day=25,month=5,year=2023)
    cur=return_db_cursor()
    # print(df.columns)
    log.set_date(today.date())
    log.pipeline_name="DCA SUS"
    main(env,cur,db_connect,today)
    print(log.log_df)