import boto3

source_bucket = 'my-bucket'
source_folder = 'docs/docs'
dest_bucket = 'my-bucket'
dest_folder = 'docs'

s3 = boto3.client('s3')

# List all objects in the source folder
objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)

# Move each object to the destination folder
for obj in objects.get('Contents', []):
    key = obj['Key']
    dest_key = key.replace(source_folder, dest_folder, 1)
    s3.copy_object(Bucket=dest_bucket, CopySource=f"{source_bucket}/{key}", Key=dest_key)
    s3.delete_object(Bucket=source_bucket, Key=key)

print("All files moved successfully.")
