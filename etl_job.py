import sys
import logging
from pyspark.context import SparkContext
from pyspark.sql.functions import col, upper, count, datediff, current_date, when
from awsglue.context import GlueContext
from awsglue.job import Job

# 🔹 Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("user_etl_job", {})

logger.info("Job started")

# 🔹 Read from S3
logger.info("Reading users data from S3")
users_df = spark.read.csv(
    "s3://amazon-s3-bucket-anil/s3-anil/users.csv",
    header=True,
    inferSchema=True
)

logger.info("Reading roles data from S3")
roles_df = spark.read.csv(
    "s3://amazon-s3-bucket-anil/s3-anil/roles.csv",
    header=True,
    inferSchema=True
)

logger.info("Users Data Preview")
users_df.show()

logger.info("Roles Data Preview")
roles_df.show()

# 🔹 Join
logger.info("Joining users and roles")
joined_df = users_df.join(roles_df, "user_id", "left")

logger.info("Joined Data Preview")
joined_df.show()

# 🔹 Transformations
logger.info("Applying transformations")

# Normalize role_name
joined_df = joined_df.withColumn(
    "role_name",
    upper(col("role_name"))
)

# Add is_active flag
joined_df = joined_df.withColumn(
    "is_active",
    when(
        (col("status") == "active") &
        (datediff(current_date(), col("signup_date")) > 30),
        True
    ).otherwise(False)
)

logger.info("Transformed Data Preview")
joined_df.show()

# 🔹 user_roles output
logger.info("Creating user_roles dataframe")
user_roles_df = joined_df.select(
    "user_id", "role_name", "assigned_date"
)

user_roles_df.show()

# 🔹 user_summary output
logger.info("Creating user_summary dataframe")
user_summary_df = joined_df.groupBy(
    "user_id", "username", "email", "signup_date", "status", "is_active"
).agg(
    count("role_name").alias("total_roles")
)

user_summary_df.show()

# 🔹 Write to Redshift using JDBC
logger.info("Writing data to Redshift using JDBC")

redshift_url = "jdbc:redshift://redshift-cluster-1.ce8huznchd6b.us-east-1.redshift.amazonaws.com:5439/dev"

# Write user_summary
user_summary_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "analytics.user_summary") \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()

logger.info("user_summary data written successfully")

# Write user_roles
user_roles_df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", "analytics.user_roles") \
    .option("user", "your_username") \
    .option("password", "your_password") \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .mode("append") \
    .save()

logger.info("user_roles data written successfully")

# 🔹 Commit job
job.commit()

logger.info("Job completed successfully")