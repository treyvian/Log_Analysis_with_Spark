import argparse
from gcloud import storage

from constants import STATS, KEY_JSON, BUCKET_NAME


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--folder_name', dest='folder_name', type=str, default='')
    args = parser.parse_args()

    folder = args.folder_name 

    # create storage client
    # NOTE:The file must be changed base on your account
    storage_client = storage.Client.from_service_account_json(KEY_JSON)


    # give blob credentials
    bucket_name = BUCKET_NAME
    bucket = storage_client.get_bucket(bucket_name)


    for p in STATS:
        prefix = f'output/{folder}/{p}/'
            
        blobs = bucket.list_blobs(prefix=prefix)

        # Iterating through for loop one by one using API call
        for blob in blobs:
            if blob.name[-4:] == '.csv':
                destination_uri = f'data/{p}.csv'
                blob.download_to_filename(destination_uri)