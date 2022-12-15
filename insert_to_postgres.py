import pandas as pd
import connection
import sqlalchemy as sql
import constant as const_value

if  __name__ == "__main__":
    df_app_test = pd.read_csv("./spark/resources/application_test.csv")
    # print(df_app_test.head(5))

    df_app_train = pd.read_csv("./spark/resources/application_train.csv")
    # print(df_app_train.head(5))
    
    # Insert Into Postgres DB
    try:
        print("Connect to Postgres...")
        connection.Connection
        try:
            print("Create Engine Conn...")
            engine = sql.create_engine(f'postgresql://{const_value.user}:{const_value.password}@{const_value.host_conn}:{const_value.port_conn}/{const_value.database}')
            df_app_test.to_sql(name="application_test", if_exists='replace', con=engine, index=False)
            df_app_train.to_sql(name="application_train", if_exists='replace', con=engine, index=False)
            print("Data Successfully Inserted...")
            print("LOG INSERTED")
            print(f"Total Inserted Row (Application Test) : {len(df_app_test)}")
            print(f"Total Inserted Row (Application Train): {len(df_app_train)}")
        except Exception as e:
            print(f"LOG_ERROR_ENGINE_CONN : ${e}") 
    except Exception as e:
        print(f"LOG_ERROR : ${e}")