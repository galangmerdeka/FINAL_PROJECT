from pyspark.sql import SQLContext
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
import spark_constant

if __name__ == "__main__":
    try:
        spark = SparkSession.builder\
            .config("spark.jars","/usr/local/spark/resources/postgresql-42.2.25.jar, /usr/local/spark/resources/mysql-connector-java-8.0.30.jar")\
            .master("local")\
            .appName("LOAD_AND_INSERT_DATA_TO_POSTGRES").getOrCreate()
    except Exception as e:
        print("LOG ERROR SPARK SESSION")
        print(f"{e}")
    
    # READ FROM MYSQL
    # try:
    #     data_app_test = spark.read.format('jdbc')\
    #         .option(
    #             url = "jdbc:mysql://mysql:3306/source_home_credit",
    #             driver = spark_constant.MYSQL_DRIVER,
    #             dbtable = "application_test",
    #             user = "root",
    #             password = "root"
    #         ).load()
    #     print("LOADED SUCCESSFULLY")
    #     data_app_test.show(1)
    # except Exception as e:
    #     print("LOG ERROR LOAD DATA APP TEST")
    #     print(f"{e}")
    
    # try:
    #     data_app_train = spark.read.format('jdbc')\
    #     .option(
    #         url = "jjdbc:mysql://mysql:3306/source_home_credit",
    #         driver = spark_constant.MYSQL_DRIVER,
    #         dbtable = "application_train",
    #         user = "root",
    #         password = "root"
    #     ).load()
    #     print("LOADED SUCCESSFULLY")
    #     data_app_test.show(1)
    # except Exception as e:
    #     print("LOG ERROR LOAD DATA APP TRAIN")
    #     print(f"{e}")
    
    # # LOAD TO POSTGRES
    # try:
    #     data_app_test.write.mode("overwrite").format("jdbc")\
    #         .option("url", "jdbc:postgresql://localhost:3306/postgres?ssl=false&sslMode=disable")\
    #         .option("driver", "org.postgresql.Driver")\
    #         .option("dbtable", "home_credit_default_risk_application_train")\
    #         .option("user", "root")\
    #         .option("password", "root").save()
    # except Exception as e:
    #     print("LOG ERROR APP TEST")
    #     print(f"{e}")