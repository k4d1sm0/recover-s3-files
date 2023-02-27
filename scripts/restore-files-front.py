import csv
import io
import streamlit as st
import boto3
import base64
import threading


def get_download_link(data, filename, text):
    """Generates a link to download the given data as a file."""
    b64 = base64.b64encode(data).decode()
    href = f"<a href='data:file/csv;base64,{b64}' download='{filename}'>{text}</a>"
    return href


s3 = boto3.client('s3')

st.title("S3 Deleted Objects Tool")

# Input date range
start_date = st.date_input("Enter start date", value=None, key=None, min_value=None, max_value=None)
end_date = st.date_input("Enter end date", value=None, key=None, min_value=None, max_value=None)

# Input S3 bucket and folder name
bucket_name = st.text_input("Enter S3 bucket name", value="my-bucket")
folder_name = st.text_input("Enter folder name", value="docs")

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
    with open("deleted_objects.csv", "wb") as f:
        f.write(csv_data)

    st.write("CSV file saved to disk.")

    # Display a link to download the CSV file
    st.markdown(get_download_link(csv_data, "deleted_objects.csv", "Download CSV"), unsafe_allow_html=True)
