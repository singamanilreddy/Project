import sys
import time
import boto3
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from awsglue.context import GlueContext

# -------------------------
# INIT
# -------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Glue Job Started...")

# -------------------------
# PATHS
# -------------------------
users_path = "s3://amazon-s3-bucket-30k-058264397013-us-east-1-an/test-anil/users.csv"
roles_path = "s3://amazon-s3-bucket-30k-058264397013-us-east-1-an/test-anil/roles.csv"

user_summary_stage = "s3://amazon-s3-bucket-30k-058264397013-us-east-1-an/test-anil/redshift_stage/user_summary/"
user_roles_stage = "s3://amazon-s3-bucket-30k-058264397013-us-east-1-an/test-anil/redshift_stage/user_roles/"

# -------------------------
# READ DATA
# -------------------------
users_df = spark.read.option("header", True).csv(users_path)
roles_df = spark.read.option("header", True).csv(roles_path)

# -------------------------
# CAST TYPES
# -------------------------
users_df = users_df.withColumn("user_id", col("user_id").cast("int")) \
                   .withColumn("signup_date", to_date(col("signup_date"))) \
                   .withColumn("last_login_date", to_date(col("last_login_date")))

roles_df = roles_df.withColumn("user_id", col("user_id").cast("int")) \
                   .withColumn("assigned_date", to_date(col("assigned_date"))) \
                   .withColumn("role_name", upper(col("role_name")))

# -------------------------
# USER METRICS
# -------------------------
users_df = users_df.withColumn("account_age_days", datediff(current_date(), col("signup_date"))) \
                   .withColumn("last_login_days", datediff(current_date(), col("last_login_date"))) \
                   .withColumn("is_active", when((col("status") == "active") & (col("account_age_days") > 30), 1).otherwise(0)) \
                   .withColumn("inactive_days", when(col("status") != "active", col("account_age_days"))) \
                   .withColumn("active_days_ratio", when((col("last_login_date").isNotNull()) & (col("account_age_days")>0), col("last_login_days")/col("account_age_days")).otherwise(None))

# -------------------------
# ROLE AGGREGATIONS
# -------------------------
agg_df = roles_df.groupBy("user_id").agg(
    count("*").alias("total_roles"),
    min("assigned_date").alias("first_role_assigned_date")
)

# -------------------------
# SUSPICIOUS FLAG (>5 roles in 30 days)
# -------------------------
roles_30 = roles_df.filter(col("assigned_date") >= date_sub(current_date(), 30))
suspicious_df = roles_30.groupBy("user_id").agg(
    count("*").alias("roles_last_30_days")
).withColumn("suspicious_flag", when(col("roles_last_30_days") > 5, 1).otherwise(0))

# -------------------------
# AVG DAYS BETWEEN ROLES
# -------------------------
window_spec = Window.partitionBy("user_id").orderBy("assigned_date")
roles_with_lag = roles_df.withColumn("prev_date", lag("assigned_date").over(window_spec))
roles_with_diff = roles_with_lag.withColumn("date_diff", datediff(col("assigned_date"), col("prev_date")))
avg_gap_df = roles_with_diff.groupBy("user_id").agg(avg("date_diff").alias("avg_days_between_roles"))

# -------------------------
# JOIN ALL
# -------------------------
user_summary_df = users_df \
    .join(agg_df, "user_id", "left") \
    .join(suspicious_df.select("user_id", "suspicious_flag"), "user_id", "left") \
    .join(avg_gap_df, "user_id", "left") \
    .withColumn("etl_load_time", current_timestamp())

# -------------------------
# FIX NULLS + CASTS FOR REDSHIFT
# -------------------------
user_summary_df = user_summary_df.fillna({
    "total_roles": 0,
    "is_active": 0,
    "suspicious_flag": 0
})

user_summary_df = user_summary_df.withColumn("active_days_ratio", col("active_days_ratio").cast("double")) \
                                 .withColumn("avg_days_between_roles", col("avg_days_between_roles").cast("double"))

user_summary_df = user_summary_df.withColumn("signup_date", col("signup_date").cast("date")) \
                                 .withColumn("last_login_date", col("last_login_date").cast("date")) \
                                 .withColumn("first_role_assigned_date", col("first_role_assigned_date").cast("date"))

# -------------------------
# USER ROLES OUTPUT
# -------------------------
user_roles_df = roles_df.withColumn("etl_load_time", current_timestamp()) \
                        .select("user_id", "role_name", "assigned_date", "etl_load_time")

# -------------------------
# WRITE TO S3 (STAGING FOR COPY)
# -------------------------
print("Writing to S3 staging...")
user_summary_df.repartition(8).write.mode("overwrite").option("header", False).csv(user_summary_stage)
user_roles_df.repartition(8).write.mode("overwrite").option("header", False).csv(user_roles_stage)
print("S3 staging completed.")

# -------------------------
# REDSHIFT COPY via boto3
# -------------------------
client = boto3.client('redshift-data')
cluster_id = "redshift-cluster-1"
database = "dev"
db_user = "admin"
iam_role_arn = "arn:aws:iam::058264397013:role/service-role/AmazonRedshift-CommandsAccessRole-20260402T170955"

copy_user_summary = f"""
TRUNCATE TABLE analytics.user_summary;
COPY analytics.user_summary (
    user_id, username, email, status, signup_date, last_login_date,
    account_age_days, last_login_days, is_active, inactive_days,
    active_days_ratio, total_roles, first_role_assigned_date,
    suspicious_flag, avg_days_between_roles, etl_load_time
)
FROM '{user_summary_stage}'
IAM_ROLE '{iam_role_arn}'
FORMAT AS CSV
DELIMITER ','
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
EMPTYASNULL
BLANKSASNULL;
"""

copy_user_roles = f"""
TRUNCATE TABLE analytics.user_roles;
COPY analytics.user_roles (
    user_id, role_name, assigned_date, etl_load_time
)
FROM '{user_roles_stage}'
IAM_ROLE '{iam_role_arn}'
FORMAT AS CSV
DELIMITER ','
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
EMPTYASNULL
BLANKSASNULL;
"""

def run_query(sql):
    response = client.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql
    )
    query_id = response['Id']
    while True:
        result = client.describe_statement(Id=query_id)
        status = result['Status']
        if status in ['FINISHED', 'FAILED', 'ABORTED']:
            print(f"Query Status: {status}")
            if status != 'FINISHED':
                print(result)
            break
        time.sleep(2)

print("Loading user_summary into Redshift...")
run_query(copy_user_summary)
print("Loading user_roles into Redshift...")
run_query(copy_user_roles)

print("Glue job completed. Data loaded into Redshift successfully ✅")