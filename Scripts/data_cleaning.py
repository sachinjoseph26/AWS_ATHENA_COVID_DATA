import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [""],  # s3 path uri of the raw data
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("type", "string", "type", "string"),
        ("id", "string", "id", "string"),
        ("`subreddit.id`", "string", "`subreddit.id`", "string"),
        ("`subreddit.name`", "string", "`subreddit.name`", "string"),
        ("`subreddit.nsfw`", "string", "`subreddit.nsfw`", "string"),
        ("created_utc", "string", "created_utc", "string"),
        ("permalink", "string", "permalink", "string"),
        ("body", "string", "body", "string"),
        ("sentiment", "string", "sentiment", "string"),
        ("score", "string", "score", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

ApplyMapping_node2DF = ApplyMapping_node2.toDF()
filterd_df = ApplyMapping_node2DF.where(f.col("type").isin(["comment"]))
date_added_df = filterd_df.withColumn('created_utc_date',  f.to_date(f.from_unixtime( filterd_df.created_utc, format="yyyy-MM-dd"), "yyyy-MM-dd"))
date_added_df = date_added_df.withColumn('year',  f.year(f.to_date(f.from_unixtime( date_added_df.created_utc, format="yyyy-MM-dd"), "yyyy-MM-dd")))
year_list = [2020, 2021]
date_added_df = date_added_df.filter(date_added_df.year.isin(year_list))
initial_df =  date_added_df.withColumn('created_utc_date', f.add_months(date_added_df['created_utc_date'], 24))
for i in range(2, 8):
    new_df =  date_added_df.withColumn('created_utc_date', f.add_months(date_added_df['created_utc_date'], i* 24))
    initial_df = initial_df.union(new_df)
date_added_df = initial_df.union(date_added_df)
date_added_df = date_added_df.withColumn('year',  f.year(date_added_df.created_utc_date))
print("============= new years added =============")
# print(date_added_df.count())
# print(date_added_df.head(5))
# print(date_added_df.printSchema())
# print(date_added_df.select('year').distinct().collect())
print(date_added_df.groupby(date_added_df.year).count().show(truncate=False))


ApplyMapping_node2 = DynamicFrame.fromDF(date_added_df, glueContext, "ApplyMapping_node2")
print(type(ApplyMapping_node2), ApplyMapping_node2)
print(ApplyMapping_node2.count())

# Script generated for node S3 Parquet save
S3Parquetsave_node3 = glueContext.getSink(
    path="",  # s3 path uri to save the formatted data parquet
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3Parquetsave_node3",
)
S3Parquetsave_node3.setCatalogInfo(
    catalogDatabase="databse_name", catalogTableName="covid_parquet_formatted_dataset"
)
S3Parquetsave_node3.setFormat("glueparquet")
S3Parquetsave_node3.writeFrame(ApplyMapping_node2)
# Script generated for node S3 csv save
S3csvsave_node1647338435154 = glueContext.getSink(
    path="",  # s3 path uri to save the formatted data csv
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3csvsave_node1647338435154",
)
S3csvsave_node1647338435154.setCatalogInfo(
    catalogDatabase="database_name", catalogTableName="covid_csv_formatted_dataset"
)
S3csvsave_node1647338435154.setFormat("csv")
S3csvsave_node1647338435154.writeFrame(ApplyMapping_node2)
# Script generated for node S3 json save
S3jsonsave_node1647340375636 = glueContext.getSink(
    path="",  # s3 path uri to save the formatted data json
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3jsonsave_node1647340375636",
)
S3jsonsave_node1647340375636.setCatalogInfo(
    catalogDatabase="database_name", catalogTableName="covid_json_formatted_dataset"
)
S3jsonsave_node1647340375636.setFormat("json")
S3jsonsave_node1647340375636.writeFrame(ApplyMapping_node2)
job.commit()