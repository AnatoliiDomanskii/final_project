import os
import boto3

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')


if aws_access_key_id is None or aws_secret_access_key is None:
    print(
        "You must set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        " environment variables to run this script"
    )
    exit(1)


session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

s3 = session.resource('s3')
BUCKET = "de2022-bucket1"

s3.Bucket(BUCKET).upload_file(
    "/Users/hunting/projects/r_d/DE2022/lect_06/data_lake/landing/src1/sales/2022-08-01/src1_sales_2022-08-01__01.csv",
    "src1_sales_2022-08-01__01.csv",
)
