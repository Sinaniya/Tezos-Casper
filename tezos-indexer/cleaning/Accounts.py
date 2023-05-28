from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import *

builder = SparkSession.builder.appName("Accounts") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "//mnt/indexer-build/jar/postgresql-42.5.0.jar") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "3") \
    .config('spark.cores.max', '6') \
    .config('spark.driver.memory','4g') \
    .config('spark.worker.cleanup.enabled', 'true') \
    .config('spark.worker.cleanup.interval', '60') \
    .config('spark.shuffle.service.db.enabled', 'true') \
    .config('spark.worker.cleanup.appDataTtl', '60') \
    .config('spark.sql.debug.maxToStringFields', 100)


spark = configure_spark_with_delta_pip(builder).getOrCreate()

# df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/tzkt_db") \
#     .option('dbtable', 'public."Accounts"') \
#     .option("user", "postgres") \
#     .option("password", "password") \
#     .option("driver", "org.postgresql.Driver") \
#     .load()

df = spark.read.option("header", True) \
    .option("inferSchema", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .csv("/Users/snraf/Documents/BDO/raw/Accounts2.csv") 

path="/Users/snraf/Documents/BDO/Cleaned/delegated_Accounts"
col = ["Id", "Address","Type","FirstLevel","LastLevel","Balance","Counter","ContractsCount","TokenBalancesCount",
"TokenTransfersCount","DelegationsCount","TransactionsCount","RevealsCount","DelegateId","DelegationLevel","Staked","Kind",
"CreatorId","ActivationLevel","FrozenDepositLimit","FrozenDeposit","BlocksCount","ProposalsCount","DoubleBakingCount","DoubleEndorsingCount",
"DoublePreendorsingCount","NonceRevelationsCount"]

#print(df.columns)
df2=df.select(*col).filter(df['Type'] == 1)
df2.write.option("header", True).mode('overwrite').csv(path)