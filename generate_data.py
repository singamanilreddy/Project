import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext

# -------------------------
# INITIALIZE
# -------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------
# READ DATA
# -------------------------
users_df = spark.read.option("header", True).csv("s3://amazon-s3-bucket-30k/test-anil/users.csv")
roles_df = spark.read.option("header", True).csv("s3://amazon-s3-bucket-30k/test-anil/roles.csv")

# -------------------------
# CAST TYPES
# -------------------------
users_df = users_df.withColumn("user_id", col("user_id").cast("int")) \
                   .withColumn("signup_date", to_date(col("signup_date"))) \
                   .withColumn("last_login_date", to_date(col("last_login_date")))

roles_df = roles_df.withColumn("user_id", col("user_id").cast("int")) \
                   .withColumn("assigned_date", to_date(col("assigned_date")))

# -------------------------
# JOIN
# -------------------------
df = users_df.join(roles_df, "user_id", "left")

###

df = df.withColumn(
    "is_locked", when (col("status") == "active", False).otherwise(True)
    )

# -------------------------
# VALIDATION
# -------------------------
valid_roles = ["admin", "editor", "viewer"]

df = df.withColumn(
    "validation_status",
    when(col("username").isNull() | (col("username") == ""), "INVALID_USERNAME")
    .when(col("email").isNull() | (~col("email").like("%@%.com")), "INVALID_EMAIL")
    .when(col("status").isNull() | (~col("status").isin("active", "inactive")), "INVALID_STATUS")
    .when(datediff(current_date(), col("last_login_date")) > 180, "INACTIVE_USER")
    .when(col("role_name").isNull() | (col("role_name") == ""), "MISSING_ROLE")
    .when(~col("role_name").isin(valid_roles), "INVALID_ROLE")
    .when(col("assigned_date") < col("signup_date"), "INVALID_ASSIGN_DATE")
    .when(col("assigned_date") > current_date(), "FUTURE_DATE")
    .otherwise("VALID")
)

# -------------------------
# AGGREGATION (SEPARATE)
# -------------------------
agg_df = df.groupBy("user_id").agg(
    count("role_name").alias("total_roles"),
    min("assigned_date").alias("first_role_assigned_date")
)

# -------------------------
# JOIN BACK (SAFE)
# -------------------------
final_df = df.join(agg_df, on="user_id", how="left")

###
final_df = final_df.withColumn (
    "is_locked", when(col("status") == "active",False).otherwise(True))

# -------------------------
# DEBUG (IMPORTANT)
# -------------------------
print("Final Schema:")
final_df.printSchema()

# -------------------------
# SELECT FINAL COLUMNS (NO ERROR NOW)
# -------------------------
final_df = final_df.select(
    col("user_id"),
    col("username"),
    col("email"),
    col("status"),  
    col("role_name"),
    col("signup_date"),
    col("last_login_date"),
    col("assigned_date"),
    col("total_roles"),
    col("first_role_assigned_date"),
    col("validation_status"),
    col("is_locked")
)

# -------------------------
# SPLIT
# -------------------------
final_valid_df = final_df.filter(col("validation_status") == "VALID") & (col("is_locked") == False)

rejected_df = final_df.filter (
    (col("validation_status")!= "VALID") | (col("is_locked") == True)
)

# -------------------------
# WRITE TO REDSHIFT
# -------------------------
final_valid_df.write \
    .format("jdbc") \
    .option("url", "jdbc:redshift://redshift-cluster-1.cnibdkql4wal.us-east-1.redshift.amazonaws.com:5439/dev") \
    .option("dbtable", "analytics.user_summary") \
    .option("user", "awsuser") \
    .option("password", "aT$8#6Hp") \
    .option("driver", "com.amazon.redshift.jdbc42.Driver") \
    .mode("append") \
    .save()

# -------------------------
# WRITE INVALID DATA
# -------------------------
rejected_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3://amazon-s3-bucket-30k/test-anil/invalid_data/")