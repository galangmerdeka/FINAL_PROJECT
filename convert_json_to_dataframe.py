import pandas as pd
import json
import sqlalchemy as sql
import convert_process as get_convert_data
import connection
import constant as const_value

with open("zips.json", mode="r", errors="ignore") as read_file_zips:
    dataZips = json.load(read_file_zips)

with open("companies.json", mode="r", errors="ignore") as read_file_comps:
    dataCompanies = json.load(read_file_comps)

try:
    print("GENERATE PROCESS ZIPS DATA INTO DATAFRAME...")
    df_zip_res = get_convert_data.ConvertProcess.convertZipsProcess(dataZips)
    print("GENERATE SUCCESFULLY")
    print(df_zip_res)
except Exception as e:
    print("ERROR GENERATE DATAFRAME ZIPS")
    print(f"${e}")

try:
    print("GENERATE PROCESS COMPANIES DATA INTO DATAFRAME...")
    df_companies_res = get_convert_data.ConvertProcess.convertCompaniesProcess(dataCompanies)
    print("GENERATE SUCCESFULLY")
    print(df_companies_res)
except Exception as e:
    print("ERROR GENERATE DATAFRAME COMPANIES")
    print(f"${e}")

try:
    print("Connect to Postgres...")
    connection.Connection
    # Create Engine
    try:
        print("Create Engine Conn...")
        engine = sql.create_engine(f'postgresql://{const_value.user}:{const_value.password}@{const_value.host_conn}:{const_value.port_conn}/{const_value.database}')
        df_zip_res.to_sql(name="zips", con=engine, if_exists='replace', index=False)
        df_companies_res.to_sql(name="companies", con=engine, if_exists='replace', index=False)
        print("Data Successfully Inserted...")
        print("LOG INSERTED")
        print(f"Total Inserted Row (Zips) : {len(df_zip_res)}")
        print(f"Total Inserted Row (Companies): {len(df_companies_res)}")
    except Exception as e:
        print(f"LOG_ERROR_ENGINE_CONN : ${e}")    
except Exception as e:
    print(f"LOG_ERROR : ${e}")