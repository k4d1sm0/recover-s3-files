import boto3
import csv
from tqdm import tqdm

s3 = boto3.client('s3')
bucket_name = 'my-bucket'
folder_name = 'docs'
start_date = input("Enter start date (in YYYY-MM-DD format): ")
end_date = input("Enter end date (in YYYY-MM-DD format): ")

response = s3.list_object_versions(Bucket=bucket_name, Prefix=folder_name, MaxKeys=1000)

deleted_count = 0
total_count = response.get('KeyCount', 0)

with open('deleted_objects.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Type', 'Key', 'VersionId', 'LastModified'])

    with tqdm(total=total_count, desc="Processing") as pbar:
        while True:
            for obj in response.get('Versions', []):
                if obj.get('IsLatest') == False:
                    last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                    if obj['Key'].startswith(folder_name + '/') and '/' not in obj['Key'][len(folder_name)+1:] and start_date <= last_modified <= end_date:
                        writer.writerow(['Object', obj['Key'], obj['VersionId'], last_modified])
                        deleted_count += 1
                        pbar.update(1)

            for obj in response.get('DeleteMarkers', []):
                last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                if obj['Key'].startswith(folder_name + '/') and '/' not in obj['Key'][len(folder_name)+1:] and start_date <= last_modified <= end_date:
                    writer.writerow(['Delete Marker', obj['Key'], obj['VersionId'], last_modified])
                    deleted_count += 1
                    pbar.update(1)

            # Check if there are more objects to retrieve
            if response.get('IsTruncated'):
                response = s3.list_object_versions(
                    Bucket=bucket_name,
                    Prefix=folder_name,
                    MaxKeys=1000,
                    KeyMarker=response['NextKeyMarker'],
                    VersionIdMarker=response['NextVersionIdMarker']
                )
                total_count += response.get('KeyCount', 0)
            else:
                break

print(f"{deleted_count} objects were deleted and written to 'deleted_objects.csv'.")
