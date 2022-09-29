from google.cloud import storage


def list_files(bucket_name):
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

    for blob in bucket.list_blobs():
        # print(blob)
        blob: storage.Blob
        print(blob.path)


if __name__ == '__main__':
    list_files(bucket_name='de2022-bucket1')