import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
import time

# Initialize clients at the top
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
client_glue = boto3.client('glue')

# Function to check if Glue crawler is running
def is_crawler_running(crawler_name):
    try:
        # Use get_crawler to get the status of the crawler
        response = client_glue.get_crawler(Name=crawler_name)
        state = response['Crawler']['State']
        if state == 'RUNNING':
            return True
        return False
    except client_glue.exceptions.EntityNotFoundException:
        return False  # The crawler doesn't exist

# Function to start the crawler with retries if it's already running
def start_crawler_with_retry(crawler_name, retries=3, delay=10):
    for attempt in range(retries):
        if not is_crawler_running(crawler_name):
            response = client_glue.start_crawler(Name=crawler_name)
            print(f'Crawler started: {json.dumps(response)}')
            return response
        else:
            print(f'Crawler is already running, retrying... ({attempt+1}/{retries})')
            time.sleep(delay)  # Wait before retrying
    print('Failed to start crawler after retries.')
    return None

# Function to process album data
def album(data):
    album_list = []
    for row in data['items']:
        album_info = row['track']['album']
        album_element = {
            'album_id': album_info['id'],
            'name': album_info['name'],
            'release_date': album_info['release_date'],
            'total_tracks': album_info['total_tracks'],
            'url': album_info['external_urls']['spotify']
        }
        album_list.append(album_element)
    return album_list

# Function to process artist data
def artist(data):
    artist_list = []
    for row in data['items']:
        for artist in row['track']['artists']:
            artist_list.append({
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            })
    return artist_list

# Function to process song data
def songs(data):
    song_list = []
    for row in data['items']:
        song_info = row['track']
        song_element = {
            'song_id': song_info['id'],
            'song_name': song_info['name'],
            'duration_ms': song_info['duration_ms'],
            'url': song_info['external_urls']['spotify'],
            'popularity': song_info['popularity'],
            'song_added': row['added_at'],
            'album_id': song_info['album']['id'],
            'artist_id': song_info['album']['artists'][0]['id']
        }
        song_list.append(song_element)
    return song_list

# Main Lambda handler function
def lambda_handler(event, context):
    Bucket = 'spotify-project-st'
    Key = 'raw_data/to_processed/'

    # Process all files in the specified S3 bucket and prefix
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.endswith('.json'):
            # Get file content
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            data = json.loads(response['Body'].read())

            # Extract and process data
            album_df = pd.DataFrame(album(data)).drop_duplicates(subset=['album_id'])
            artist_df = pd.DataFrame(artist(data)).drop_duplicates(subset=['artist_id'])
            song_df = pd.DataFrame(songs(data))

            # Convert date columns to datetime
            album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
            song_df['song_added'] = pd.to_datetime(song_df['song_added'])

            # Save transformed data to S3
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            for name, df in [('songs', song_df), ('album', album_df), ('artist', artist_df)]:
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                s3.put_object(Bucket=Bucket, Key=f'transformed_data/{name}_data/{name}_transformed_{timestamp}.csv', Body=buffer.getvalue())

            # Move processed file
            s3_resource.meta.client.copy(
                {'Bucket': Bucket, 'Key': file_key},
                Bucket, f'raw_data/processed/{file_key.split("/")[-1]}'
            )
            s3_resource.Object(Bucket, file_key).delete()

    # Start Glue Crawler with retry mechanism
    crawler_name = 'spotify_crawler_1'
    start_crawler_with_retry(crawler_name)



