import csv
import threading
import boto3

s3 = boto3.client('s3')


def restore_object(bucket_name, key, version_id):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=key,
        MaxKeys=1
    )
    if response.get('KeyCount', 0) == 0:
        try:
            response = s3.head_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        except Exception as e:
            print(f"Object '{key}' version '{version_id}' does not exist.")
            return

        size = response['ContentLength']
        metadata = response['Metadata']
        metadata['Content-Type'] = response['ContentType']
        metadata['Content-Length'] = str(size)
        response = s3.copy_object(
            Bucket=bucket_name,
            CopySource={
                'Bucket': bucket_name,
                'Key': key,
                'VersionId': version_id
            },
            Key=key,
            Metadata=metadata,
            MetadataDirective='REPLACE'
        )
        print(f"Object '{key}' version '{version_id}' was restored.")
    else:
        print(f"Object '{key}' version '{version_id}' already exists in the bucket.")


def restore_deleted_objects(bucket_name):
    with open('deleted_objects.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header row

        threads = []
        for row in reader:
            key = row[1]
            version_id = row[2]

            if row[0] == 'Delete Marker':
                # This is a delete marker for a current or noncurrent version of an object
                response = s3.delete_object(
                    Bucket=bucket_name,
                    Key='docs/' + key,
                    VersionId=version_id
                )
                print(f"Object '{key}' version '{version_id}' was restored.")
            else:
                # This is a non-current version of the object
                t = threading.Thread(target=restore_object, args=(bucket_name, key, version_id))
                threads.append(t)
                t.start()

        # Wait for all threads to finish
        for t in threads:
            t.join()

    print("All deleted objects were restored to the 'docs' subfolder.")
