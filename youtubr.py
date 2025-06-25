import streamlit as st
import pandas as pd
import mysql.connector
import os
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime

# Load API key
load_dotenv()
api_key = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=api_key)

# ---------------- DB CONNECTION ----------------
def create_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data",
        port=8889
    )

# ---------------- HELPERS ----------------
def convert_datetime(yt_datetime_str):
    return datetime.strptime(yt_datetime_str, "%Y-%m-%dT%H:%M:%SZ")

# ---------------- API FUNCTIONS ----------------
def get_channel_details(channel_id):
    response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()
    return response['items'][0] if 'items' in response and len(response['items']) > 0 else None

def get_upload_playlist_id(channel_id):
    data = get_channel_details(channel_id)
    return data['contentDetails']['relatedPlaylists']['uploads']

def get_videos_from_playlist(playlist_id):
    videos = []
    next_page_token = None
    while True:
        response = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        for item in response["items"]:
            videos.append({
                "video_id": item["snippet"]["resourceId"]["videoId"],
                "video_name": item["snippet"]["title"],
                "published_at": item["snippet"]["publishedAt"]
            })
        next_page_token = response.get("nextPageToken")
        if not next_page_token or len(videos) >= 100:
            break
    return videos

def get_video_details(video_ids):
    all_data = []
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(video_ids[i:i+50])
        ).execute()
        for video in response["items"]:
            stats = video["statistics"]
            snippet = video["snippet"]
            content = video["contentDetails"]
            all_data.append({
                "video_id": video["id"],
                "video_name": snippet["title"],
                "published_at": snippet["publishedAt"],
                "view_count": int(stats.get("viewCount", 0)),
                "like_count": int(stats.get("likeCount", 0)),
                "comment_count": int(stats.get("commentCount", 0)),
                "duration": content.get("duration", "")
            })
    return all_data

# ---------------- INSERT FUNCTIONS ----------------
def insert_channel(channel_id):
    conn = create_connection()
    cursor = conn.cursor()
    details = get_channel_details(channel_id)
    if not details:
        st.error("❌ Channel not found.")
        return

    sql = """
        INSERT INTO channels (channel_id, channel_name, description, subscriber_count, video_count, view_count, playlist_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE subscriber_count=VALUES(subscriber_count), view_count=VALUES(view_count)
    """
    cursor.execute(sql, (
        details["id"],
        details["snippet"]["title"],
        details["snippet"].get("description", ""),
        int(details["statistics"].get("subscriberCount", 0)),
        int(details["statistics"].get("videoCount", 0)),
        int(details["statistics"].get("viewCount", 0)),
        details["contentDetails"]["relatedPlaylists"]["uploads"]
    ))
    conn.commit()
    cursor.close()
    conn.close()

def insert_videos(channel_id):
    conn = create_connection()
    cursor = conn.cursor()

    playlist_id = get_upload_playlist_id(channel_id)
    video_items = get_videos_from_playlist(playlist_id)
    video_ids = [v["video_id"] for v in video_items]
    video_details = get_video_details(video_ids)

    sql = """
        INSERT INTO videos (video_id, channel_id, video_name, published_at, view_count, like_count, comment_count, duration)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE view_count=VALUES(view_count), like_count=VALUES(like_count), comment_count=VALUES(comment_count)
    """
    for video in video_details:
        cursor.execute(sql, (
            video["video_id"],
            channel_id,
            video["video_name"],
            convert_datetime(video["published_at"]),
            video["view_count"],
            video["like_count"],
            video["comment_count"],
            video["duration"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

# ---------------- STREAMLIT UI ----------------
st.title("📺 YouTube Data Warehouse")

# Migrate channel
with st.expander("📥 Migrate YouTube Channel"):
    input_channel = st.text_input("Enter Channel ID:")
    if st.button("🚀 Migrate Now"):
        if input_channel:
            insert_channel(input_channel)
            insert_videos(input_channel)
            st.success("✅ Data inserted successfully!")
        else:
            st.warning("Please enter a channel ID.")

# Channel dashboard
st.subheader("📊 Channel Dashboard")
def get_channel_info():
    conn = create_connection()
    df = pd.read_sql("SELECT * FROM channels", conn)
    conn.close()
    return df

def get_video_info(channel_id):
    conn = create_connection()
    df = pd.read_sql(f"SELECT * FROM videos WHERE channel_id = '{channel_id}'", conn)
    conn.close()
    return df

channels_df = get_channel_info()
if not channels_df.empty:
    selected = st.selectbox("Select a channel", channels_df["channel_name"])
    channel = channels_df[channels_df["channel_name"] == selected].iloc[0]
    st.write(f"👥 Subscribers: {int(channel['subscriber_count']):,}")
    st.write(f"👁️ Views: {int(channel['view_count']):,}")
    st.write(f"🎬 Videos: {int(channel['video_count'])}")
    st.write(f"📝 Description: {channel['description']}")

    videos_df = get_video_info(channel["channel_id"])
    st.subheader("🔥 Top 10 Videos by Views")
    top_videos = videos_df.sort_values(by="view_count", ascending=False).head(10)
    st.dataframe(top_videos[["video_name", "view_count", "like_count", "comment_count"]])
    st.bar_chart(top_videos.set_index("video_name")["view_count"])

# SQL Query Explorer
st.markdown("---")
st.subheader("🔍 SQL Query Explorer")
query_options = {
    "1. All video names and their channel names":
        "SELECT video_name, channel_name FROM videos JOIN channels ON videos.channel_id = channels.channel_id",

    "2. Channels with most videos":
        "SELECT channel_name, video_count FROM channels ORDER BY video_count DESC",

    "3. Top 10 most viewed videos":
        "SELECT video_name, videos.view_count, channel_name FROM videos JOIN channels USING(channel_id) ORDER BY videos.view_count DESC LIMIT 10",

    "4. Number of comments on each video":
        "SELECT video_name, comment_count FROM videos",

    "5. Most liked videos with channel names":
        "SELECT video_name, like_count, channel_name FROM videos JOIN channels USING(channel_id) ORDER BY like_count DESC LIMIT 10",

    "6. Likes and dislikes for all videos":
        "SELECT video_name, like_count, 0 AS dislike_count FROM videos",

    "7. Total views for each channel":
        "SELECT channel_name, view_count FROM channels ORDER BY view_count DESC",

    "8. Channels that uploaded videos in 2022":
        "SELECT DISTINCT channel_name FROM videos JOIN channels USING(channel_id) WHERE YEAR(published_at) = 2022",

    "9. Average video duration per channel":
        "SELECT channel_name, AVG(CHAR_LENGTH(duration)) AS avg_duration FROM videos JOIN channels USING(channel_id) GROUP BY channel_name",

    "10. Videos with the highest number of comments":
        "SELECT video_name, comment_count FROM videos ORDER BY comment_count DESC LIMIT 10"
}
selected_query = st.selectbox("🧠 Choose a query to run", list(query_options.keys()))
if selected_query:
    conn = create_connection()
    df = pd.read_sql(query_options[selected_query], conn)
    conn.close()
    st.write(f"📄 Results for: _{selected_query}_")
    st.dataframe(df)
