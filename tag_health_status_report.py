import pandas as pd
import pyodbc
from datetime import datetime as dt
from sqlalchemy import select,MetaData,create_engine,Table,inspect
from sqlalchemy.engine import URL
import numpy as np
import json
from utils.db_connector_sql import DBConnector
from utils.log_module import Log
import traceback
import inspect

def return_db_cursor():
    try:   
        conn=pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',server='grcd-az-mdg-pp-sql-01.database.windows.net',
                            database='GRCD-AZ-PEL-PP-DBA-01',uid='azuremdg.pelsqldb',pwd='$pEl*@driYOd'
                            )
        cursor=conn.cursor()
        log.capture_trace(inspect.stack())
        return cursor
    except Exception as exc:
        log.capture_trace(exc)

def return_sql_alchemy_engine():
    try:
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
        log.capture_trace(inspect.stack())
        return engine
    except Exception as exc:
        log.capture_trace(exc)

def return_table_columns(table):
    try:
        engine=return_sql_alchemy_engine()
        con = engine.connect()
        metadata = MetaData(schema='dbo')
        tbl = Table(table, metadata, autoload_with=con)
        columns=[col.name for col in tbl.columns]
        log.capture_trace(inspect.stack())
        return columns
    except Exception as exc:
        log.capture_trace(exc)

def run_health_status_statement(yesterday,cursor):
    try:
        stmt=f"""
                with cte as
                (
                select  ss.*,ms.SubType from sustainability_snapshot ss
                left join master_sustainability ms on ss.type=ms.Type and ss.[tag_name/parameter]=ms.[CMC Tag/Parameter]
                where SubType!='Waste' and CONVERT(date,date)='{yesterday}'
                ),
                cte2 as
                (
                select date,Lead(date,1) over (order by date desc) as lead_val,
                DATEDIFF(MINUTE,Lead(date,1) over (partition by [tag_name/parameter] order by date desc),date) as minute_diff,
                DATEDIFF(SECOND,Lead(date,1) over (partition by [tag_name/parameter] order by date desc),date) as second_diff,
                plant,[tag_name/parameter],value,status
                from cte
                )
                select CONVERT(date,date) as date,plant,[tag_name/parameter],status, sum(second_diff)/60 as total_bad_minutes
                from cte2
                group by CONVERT(date,date),plant,[tag_name/parameter],status
            """
        print("Processing Tag Health Data.....")
        data=cursor.execute(stmt).fetchall()
        columns=return_table_columns('tag_health_status')
        print("Success: Data Processed...........")
        df=pd.DataFrame.from_records(data=data,columns=columns)
        log.capture_trace(inspect.stack())
        return df
    except Exception as exc:
        log.capture_trace(exc)
    
def run_alarm_status_statement(yesterday,cursor):
    try:
        stmt=f"""
                    with cte as
                    (
                    select  ss.*,ms.[Alert Condition],
                    case when 
                            ss.value>ms.[Alert Condition] 
                            then 'Alarm'
                            else 'No Alarm' end as alarm
                    from sustainability_snapshot ss
                    left join master_sustainability ms on ss.type=ms.Type and ss.[tag_name/parameter]=ms.[CMC Tag/Parameter]
                    where SubType!='Waste' and CONVERT(date,date)='{yesterday}'
                    ),cte2 as
                    (select date,Lead(date,1) over (order by date desc) as lead_val,
                    DATEDIFF(MINUTE,Lead(date,1) over (partition by [tag_name/parameter] order by date desc),date) as minute_diff,
                    DATEDIFF(SECOND,Lead(date,1) over (partition by [tag_name/parameter] order by date desc),date) as second_diff,
                    plant,[tag_name/parameter],value,[Alert Condition],alarm
                    from cte
                    )
                    select convert(date,date) as date,plant,[tag_name/parameter],alarm,sum(second_diff)/60 as alarm_status
                    from cte2
                    group by plant,[tag_name/parameter],convert(date,date),alarm
            """
        print("Processing Tag Alarm Data.....")
        data=cursor.execute(stmt).fetchall()
        columns=return_table_columns('tag_alarm_status')
        print("Success: Data Processed...........")
        df=pd.DataFrame.from_records(data=data,columns=columns)
        # print(df)
        log.capture_trace(inspect.stack())
        return df
    except Exception as exc:
        log.capture_trace(exc)

def upload_status(df,table,db_connect):
    try:
        print("Uploading Tag Health data................")
        for col in df.columns:
            if df[col].isnull().sum() > 0:
                datatype=str(df[col].dtype)
                df[col] = np.where(df[col].isnull(), None, df[col])
                if 'datetime' in datatype:
                    df[col]=pd.to_datetime(df[col])
        # params=list(tuple(row) for row in df.values)
        # cursor.fast_executemany=True
        # sql="insert into tag_health_status values (?,?,?,?,?,?,?,?,?)"
        # cursor.executemany(sql,params)
        # cursor.commit()
        # cursor.close()
        db_connect.write_df_to_table(df=df,table=table,update=True)
        print(f"Data Uploaded Successfully to {table}......")
        log.capture_trace(inspect.stack())
    except Exception as exc:
        log.capture_trace(exc)


if __name__=="__main__":
    cfg=json.load(open("db_cred.json"))
    db_connect=DBConnector(cfg.get('Azure_sql'))
    yesterday=(dt.today()-pd.Timedelta(days=1))
    yesterday=yesterday.date()
    log=Log()
    log.set_date(yesterday)
    log.pipeline_name="DCA SUS"
    # yesterday=dt(day=24,month=7,year=2023)
    cursor=return_db_cursor()
    health_df=run_health_status_statement(yesterday,cursor)
    alarm_df=run_alarm_status_statement(yesterday,cursor)
    # print(df)
    upload_status(health_df,'tag_health_status',db_connect)
    upload_status(alarm_df,'tag_alarm_status',db_connect)
    