import boto3
import csv

s3 = boto3.client('s3')
bucket_name = 'my-bucket'
folder_name = 'docs'

response = s3.list_object_versions(Bucket=bucket_name, Prefix=folder_name, MaxKeys=1000)

deleted_count = 0

with open('deleted_objects.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Type', 'Key', 'VersionId', 'LastModified'])

    while True:
        for obj in response.get('Versions', []):
            if obj.get('IsLatest') == False:
                # This is a non-current version of the object
                writer.writerow(['Object', obj['Key'], obj['VersionId'], obj['LastModified']])
                deleted_count += 1

        for obj in response.get('DeleteMarkers', []):
            # This is a delete marker for a current or noncurrent version of an object
            writer.writerow(['Delete Marker', obj['Key'], obj['VersionId'], obj['LastModified']])
            deleted_count += 1

        # Check if there are more objects to retrieve
        if response.get('IsTruncated'):
            response = s3.list_object_versions(
                Bucket=bucket_name,
                Prefix=folder_name,
                MaxKeys=1000,
                KeyMarker=response['NextKeyMarker'],
                VersionIdMarker=response['NextVersionIdMarker']
            )
        else:
            break

print(f"{deleted_count} objects were deleted and written to 'deleted_objects.csv'.")
