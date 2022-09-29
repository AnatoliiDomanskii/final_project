"""
Upload a file to GCS

Notes:
    - all examples can be found here:
    https://github.com/googleapis/python-storage/tree/main/samples/snippets
"""
from google.cloud import storage


def upload_blob(bucket_name, source_file_path, destination_blob_name):
    """
    Uploads a file to the bucket.
    """
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client(project='de2022-robot-dreams')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)

    print(
        f"File {source_file_path} uploaded to {destination_blob_name}."
    )



if __name__ == "__main__":
    upload_blob(
        bucket_name='de2022-bucket1',
        source_file_path='/Users/hunting/projects/r_d/DE2022/lect_06/data_lake/landing/src1/sales/2022-08-02/src1_sales_2022-08-02__01.csv',
        destination_blob_name='2022-08-02/src1_sales_2022-08-02__01.csv',
    )