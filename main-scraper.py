import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import re

# Configuration
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = 'personalization-on-sheets.json' #This is the file that will contain the google sheet email address that is added there.
YOUTUBE_API_KEYS = [
    #Add multiple API keys here so that we can switch while the program keeps moving and doesn't stops due to API limit.
]
SPREADSHEET_NAME = 'Spreadsheet name' #Add Google sheet name
SHEET_NAME = 'Sheet name' #Add Sheet name
RATE_LIMIT_DELAY = 5  # in seconds of moving from one search to another

# Initialize Google Sheets client
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPE)
client = gspread.authorize(creds)
sheet = client.open(SPREADSHEET_NAME).worksheet(SHEET_NAME)

# Initialize YouTube clients
youtube_clients = [build('youtube', 'v3', developerKey=key) for key in YOUTUBE_API_KEYS]
current_youtube_client_index = 0

def get_youtube_client():
    global current_youtube_client_index
    return youtube_clients[current_youtube_client_index]

def switch_youtube_client():
    global current_youtube_client_index
    current_youtube_client_index = (current_youtube_client_index + 1) % len(youtube_clients)
    print(f"Switching to API key {current_youtube_client_index + 1}")

def all_keys_exhausted():
    global current_youtube_client_index
    return current_youtube_client_index == 0

def safe_execute(request):
    try:
        response = request.execute()
        return response
    except HttpError as e:
        if e.resp.status == 403 and 'quota' in e._get_reason().lower():
            print(f"Quota exceeded for API key {current_youtube_client_index + 1}. Switching API key...")
            switch_youtube_client()
            if all_keys_exhausted():
                print("All API keys exhausted. Waiting for quota reset...")
                time.sleep(3600)  # Wait for 1 hour before retrying
            return safe_execute(request)
        else:
            print(f"HttpError occurred: {e}")
            raise

def extract_channel_id(url):
    print(f"Extracting channel ID from URL: {url}")

    # Extract from channel URL
    channel_match = re.match(r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/channel\/([a-zA-Z0-9_-]+)', url)
    if channel_match:
        return channel_match.group(1)
    
    # Extract from handle URL
    handle_match = re.match(r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/@([a-zA-Z0-9_-]+)', url)
    if handle_match:
        handle = handle_match.group(1)
        return get_channel_id_from_handle(handle)
    
    # Extract from video URL
    video_match = re.match(r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)', url)
    if video_match:
        video_id = video_match.group(1)
        return get_channel_id_from_video(video_id)
    
    video_match_short = re.match(r'(?:https?:\/\/)?(?:www\.)?youtu\.be\/([a-zA-Z0-9_-]+)', url)
    if video_match_short:
        video_id = video_match_short.group(1)
        return get_channel_id_from_video(video_id)
    
    # Extract from custom URL
    custom_match = re.match(r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/(c|user|channel|playlist)\/([a-zA-Z0-9_-]+)', url)
    if custom_match:
        custom_url = custom_match.group(2)
        return get_channel_id_from_search(custom_url)
    
    # Extract from shortened URLs
    if 'bit.ly' in url or 'tinyurl.com' in url:
        print(f"Handling shortened URL: {url}")
        request = get_youtube_client().search().list(
            part='id',
            q=url,
            type='channel',
            maxResults=1
        )
        response = safe_execute(request)
        if 'items' in response and len(response['items']) > 0:
            return response['items'][0]['id']['channelId']

    return None

def get_channel_id_from_handle(handle):
    print(f"Searching for channel with handle: {handle}")
    request = get_youtube_client().search().list(
        part='id',
        q=handle,
        type='channel',
        maxResults=1
    )
    response = safe_execute(request)
    print("Response for handle search:", response)
    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]['id']['channelId']
    return None

def get_channel_id_from_video(video_id):
    print(f"Fetching channel ID from video ID: {video_id}")
    request = get_youtube_client().videos().list(
        part='snippet',
        id=video_id
    )
    response = safe_execute(request)
    print("Response for video ID fetch:", response)
    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]['snippet']['channelId']
    return None

def get_channel_id_from_search(query):
    print(f"Searching for channel with query: {query}")
    request = get_youtube_client().search().list(
        part='snippet',
        q=query,
        type='channel',
        maxResults=1
    )
    response = safe_execute(request)
    print("Response for search query:", response)
    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]['id']['channelId']
    return None

def get_channel_data(channel_id):
    print(f"Requesting data for channel ID: {channel_id}")
    request = get_youtube_client().channels().list(
        part='snippet,statistics,contentDetails',
        id=channel_id
    )
    response = safe_execute(request)
    print("YouTube API Response for Channel Data:", response)
    
    if 'items' in response and len(response['items']) > 0:
        item = response['items'][0]
        snippet = item['snippet']
        statistics = item['statistics']
        content_details = item['contentDetails']
        
        latest_video_title = 'N/A'
        uploads_playlist_id = content_details['relatedPlaylists'].get('uploads')
        if uploads_playlist_id:
            latest_video_title = get_latest_video_title(uploads_playlist_id)
        
        return {
            'Description': snippet.get('description', 'Description Absent'),
            'Subscribers': statistics.get('subscriberCount', 'N/A'),
            'Video Count': statistics.get('videoCount', 'N/A'),
            'View Count': statistics.get('viewCount', 'N/A'),
            'Latest Video Title': latest_video_title
        }
    return {'Description': 'Description Absent', 'Subscribers': 'Data Not Found', 'Video Count': 'Data Not Found', 'View Count': 'Data Not Found', 'Latest Video Title': 'Data Not Found'}

def get_latest_video_title(playlist_id):
    print(f"Fetching latest video title from playlist ID: {playlist_id}")
    request = get_youtube_client().playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=1
    )
    try:
        response = safe_execute(request)
        print("YouTube API Response for Latest Video Title:", response)
        
        if 'items' in response and len(response['items']) > 0:
            latest_video = response['items'][0]['snippet']
            return latest_video.get('title', 'N/A')
    except HttpError as e:
        if e.resp.status == 404:
            print(f"Playlist not found: {playlist_id}")
    return 'N/A'

def process_channel_data():
    row = 2  # Starting from row 2 (adjust as needed)
    while True:
        url = sheet.cell(row, 2).value  # Read URL from the 2th column
        if url is None:
            print(f"Blank URL at row {row}. Skipping this row.")
            row += 1
            continue
        
        if not url.strip():  # Skip empty strings
            print(f"Empty URL at row {row}. Skipping this row.")
            row += 1
            continue
        
        channel_id = extract_channel_id(url)
        if not channel_id:
            print(f"Could not extract channel ID for URL: {url}. Marking as 'Invalid URL'")
            sheet.update_cell(row, 6, 'Invalid URL')
            row += 1
            continue
        
        channel_data = get_channel_data(channel_id)
        print(f"Writing data to Google Sheet for row {row}: {channel_data}")
        sheet.update_cell(row, 3, channel_data['Description'])
        sheet.update_cell(row, 4, channel_data['Subscribers'])
        sheet.update_cell(row, 5, channel_data['Video Count'])
        sheet.update_cell(row, 6, channel_data['View Count'])
        sheet.update_cell(row, 7, channel_data['Latest Video Title'])
        
        time.sleep(RATE_LIMIT_DELAY)  # To respect rate limiting
        row += 1

process_channel_data()
