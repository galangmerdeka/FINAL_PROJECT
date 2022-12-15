
# import findspark
# findspark.init()
from pyspark.sql import SparkSession
import spark_constant

if __name__ == "__main__":
    # initiate spark
    try:
        spark = SparkSession.builder\
            .master("local")\
            .config("spark.jars","/usr/local/spark/resources/mysql-connector-java-8.0.30.jar")\
            .appName("Conn_DB").getOrCreate()\
            
            
    except Exception as e:
        print("LOG ERROR SPARK SESSION")
        print(f"{e}")
    # print(spark_session)

    # READ DATA FROM CSV
    # APPLICATION_TEST.CSV
    try:
        # app_test_path = "/usr/local/spark/resources/application_test.csv"
        df_app_test = spark.read.format("csv")\
            .option("inferSchema", "true")\
            .option("header", "true")\
            .load("/usr/local/spark/resources/application_test.csv")

        df_app_test.head(5)
    except Exception as e:
        print("LOG ERROR DATAFRAME")
        print(f"{e}")
    
    try:
        jdbc_url = "jdbc:mysql://mysql:3306/source_home_credit"
        df_app_test.write.mode("overwrite").format("jdbc")\
            .option("url", jdbc_url)\
            .option("driver", "com.mysql.jdbc.Driver")\
            .option("dbtable", "application_test")\
            .option("user", "root")\
            .option("password", "root").save()
        print("INSERT SUCCESFULLY")
    except Exception as e:
        print("LOG ERROR APP TEST")
        print(f"{e}")

    # APPLICATION_TRAIN.CSV
    try:
        # app_train_path = "/usr/local/spark/resources/application_train.csv"
        df_app_train = spark.read.format("csv")\
            .option("inferSchema", "true")\
            .option("header", "true")\
            .load("/usr/local/spark/resources/application_train.csv")

        df_app_train.head(5)
    except Exception as e:
        print("LOG ERROR DATAFRAME")
        print(f"{e}")
    
    try:
        jdbc_url = "jdbc:mysql://mysql:3306/source_home_credit"
        df_app_train.write.mode("overwrite").format("jdbc")\
            .option("url", jdbc_url)\
            .option("driver", "com.mysql.jdbc.Driver")\
            .option("dbtable", "application_train")\
            .option("user", "root")\
            .option("password", "root").save()
        print("INSERT SUCCESFULLY")
    except Exception as e:
        print("LOG ERROR APP TRAIN")
        print(f"{e}")
