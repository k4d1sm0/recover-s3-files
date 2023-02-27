import boto3
import csv
import datetime
import pytz

s3 = boto3.client('s3')
bucket_name = 'my-bucket'
folder_name = 'docs'

tz = pytz.timezone('UTC')  # Set the timezone to UTC
start_time = tz.localize(datetime.datetime(2023, 2, 7, 0, 0, 0))  # Start time of the hour range
end_time = tz.localize(datetime.datetime(2023, 2, 9, 0, 0, 0))  # End time of the hour range

response = s3.list_object_versions(Bucket=bucket_name, Prefix=folder_name)

deleted_count = 0

with open('deleted_objects_date.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Type', 'Key', 'VersionId', 'LastModified'])

    for version in response['Versions']:
        if 'DeleteMarker' in version:
            if start_time <= version['LastModified'] <= end_time:
                writer.writerow(['Delete Marker', version['Key'], version['VersionId'], version['LastModified']])
                deleted_count += 1
        else:
            if start_time <= version['LastModified'] <= end_time:
                writer.writerow(['Object', version['Key'], version['VersionId'], version['LastModified']])
                deleted_count += 1

print(f"{deleted_count} objects deleted between {start_time} and {end_time} were written to 'deleted_objects_date.csv'.")
