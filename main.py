import base64
import sys

import boto3
import streamlit as st
import csv
import io
import threading

s3 = boto3.client('s3')


def get_download_link(data, filename, text):
    """Generates a link to download the given data as a file."""
    b64 = base64.b64encode(data).decode()
    href = f"<a href='data:file/csv;base64,{b64}' download='{filename}'>{text}</a>"
    return href


def retrieve_deleted_objects():
    st.title("S3 Deleted Objects Tool")

    # Input date range
    start_date = st.date_input("Enter start date", value=None, key=None, min_value=None, max_value=None)
    end_date = st.date_input("Enter end date", value=None, key=None, min_value=None, max_value=None)

    # Input S3 bucket and folder name
    bucket_name = st.text_input("Enter S3 bucket name", value="my-bucket")
    folder_name = st.text_input("Enter folder name(optional)", value="docs")

    # Run the object retrieval process
    if st.button("Retrieve Deleted Objects"):
        with st.spinner("Retrieving deleted objects..."):
            response = s3.list_object_versions(Bucket=bucket_name, Prefix=folder_name, MaxKeys=1000)

            deleted_objects = []

            csvfile = io.StringIO()
            writer = csv.writer(csvfile)

            writer.writerow(['Type', 'Key', 'VersionId', 'LastModified'])

            for obj in response.get('Versions', []):
                if obj.get('IsLatest') == False and obj.get('DeleteMarker') is not None:
                    last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                    if obj['Key'].startswith(folder_name + '/') and '/' not in obj['Key'][
                                                                               len(folder_name) + 1:] and start_date.strftime(
                        '%Y-%m-%d') <= last_modified <= end_date.strftime('%Y-%m-%d'):
                        deleted_objects.append({'Type': 'Object', 'Key': obj['Key'], 'VersionId': obj['VersionId'],
                                                'LastModified': last_modified})
                        writer.writerow(['Object', obj['Key'], obj['VersionId'], last_modified])

            for obj in response.get('DeleteMarkers', []):
                last_modified = obj['LastModified'].strftime('%Y-%m-%d')
                if obj['Key'].startswith(folder_name + '/') and '/' not in obj['Key'][
                                                                           len(folder_name) + 1:] and start_date.strftime(
                    '%Y-%m-%d') <= last_modified <= end_date.strftime('%Y-%m-%d'):
                    deleted_objects.append({'Type': 'Delete Marker', 'Key': obj['Key'], 'VersionId': obj['VersionId'],
                                            'LastModified': last_modified})
                    writer.writerow(['Delete Marker', obj['Key'], obj['VersionId'], last_modified])

            st.write(f"{len(deleted_objects)} deleted objects were retrieved.")

        # Generate the CSV file
        csvfile.seek(0)
        csv_data = csvfile.read().encode()

        # Save the CSV file to disk
        with open("scripts/deleted_objects.csv", "wb") as f:
            f.write(csv_data)

        st.write("CSV file saved to disk.")

        # Display a link to download the CSV file
        st.markdown(get_download_link(csv_data, "scripts/deleted_objects.csv", "Download CSV"), unsafe_allow_html=True)

    if st.button("Restore Deleted Objects"):
        with st.spinner("Restoring deleted objects..."):
            restore_deleted_objects(bucket_name)
        st.write("All deleted objects were restored to the 'docs' subfolder.")


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

    # Return the restored object
    return key, version_id


def restore_deleted_objects(bucket_name):
    with open('scripts/deleted_objects.csv', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header row

        threads = {}
        for row in reader:
            key = row[1]
            version_id = row[2]
            t = threading.Thread(target=restore_object, args=(bucket_name, key, version_id))
            threads[t] = (key, version_id)
            t.start()

        # Wait for all threads to finish
        for t in threads:
            t.join()

        print("All deleted objects were restored to the '' subfolder.")

        # Handle any exceptions raised during object restoration
        try:
            for t in threads:
                key, version_id = threads[t]
                print(f"Object '{key}' version '{version_id}' was restored.")
        except KeyboardInterrupt:
            print("Interrupted by user. Aborting...")
            sys.exit(1)
        except Exception as e:
            print("An error occurred while restoring objects:", str(e))


if __name__ == '__main__':
    retrieve_deleted_objects()
