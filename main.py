import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dateutil import parser as date_parser

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_env_vars():
    """環境変数からAPIキー、認証情報、2つのスプレッドシートIDを取得"""
    api_key = os.environ.get("YOUTUBE_API_KEY")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    dist_spreadsheet_id = os.environ.get("DIST_SPREADSHEET_ID")
    service_account_key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")

    if not all([api_key, spreadsheet_id, dist_spreadsheet_id, service_account_key]):
        print("エラー: 環境変数が不足しています。")
        sys.exit(1)
    return api_key, spreadsheet_id, dist_spreadsheet_id, service_account_key

def read_channel_ids(file_path):
    if not os.path.exists(file_path):
        print(f"エラー: {file_path} が見つかりません。")
        sys.exit(1)
    with open(file_path, 'r', encoding='utf-8') as file:
        channel_ids = [line.strip() for line in file if line.strip()]
    return list(set(channel_ids))

def get_duration_seconds(iso_duration):
    """ISO8601形式（PT1M5Sなど）を合計秒数に変換"""
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return 0
    h = int(match.group(1)) if match.group(1) else 0
    m = int(match.group(2)) if match.group(2) else 0
    s = int(match.group(3)) if match.group(3) else 0
    return h * 3600 + m * 60 + s

def iso8601_to_duration(iso_duration):
    """表示用の時間フォーマット変換"""
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return "00:00:00"
    h, m, s = [int(match.group(i)) if match.group(i) else 0 for i in range(1, 4)]
    return str(timedelta(hours=h, minutes=m, seconds=s))

def convert_to_japan_time(utc_time_str):
    utc_dt = date_parser.parse(utc_time_str)
    JST = timezone(timedelta(hours=9))
    return utc_dt.astimezone(JST).strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_time():
    JST = timezone(timedelta(hours=9))
    return datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_digit_date():
    JST = timezone(timedelta(hours=9))
    return datetime.now(JST).strftime("%Y%m%d")

def calc_engagement_rate(like, comment, view):
    if view == 0:
        return 0.0
    return round((like + comment) / view * 100, 2)

def get_uploads_playlist_id(youtube, channel_id):
    try:
        res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        return res['items'][0]['contentDetails']['relatedPlaylists']['uploads'] if res.get('items') else None
    except:
        return None

def get_all_videos_since_2025(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    playlist_id = get_uploads_playlist_id(youtube, channel_id)
    if not playlist_id:
        return []

    cutoff_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    video_ids, next_page_token = [], None

    while True:
        try:
            pl_res = youtube.playlistItems().list(
                playlistId=playlist_id, part='snippet', maxResults=50, pageToken=next_page_token
            ).execute()
            for item in pl_res['items']:
                if date_parser.parse(item['snippet']['publishedAt']) < cutoff_date:
                    return self_fetch_details(youtube, video_ids) # 途中で期限切れなら詳細取得へ
                video_ids.append(item['snippet']['resourceId']['videoId'])
            next_page_token = pl_res.get('nextPageToken')
            if not next_page_token: break
        except: break

    return fetch_video_details(youtube, video_ids)

def fetch_video_details(youtube, video_ids):
    """動画詳細を取得し、Shorts判定を行う"""
    final_data = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            vid_res = youtube.videos().list(
                part='snippet,statistics,contentDetails', id=','.join(batch)
            ).execute()
            for item in vid_res['items']:
                snippet = item['snippet']
                stats = item.get('statistics', {})
                content = item['contentDetails']
                
                # --- 動画種別判定 (3分以内 ＆ 縦長/正方形) ---
                duration_sec = get_duration_seconds(content.get('duration', "PT0S"))
                
                # サムネイル情報からアスペクト比を推測
                # (Shortsは thumbnails.standard や maxres が縦長/正方形になる特性を利用)
                thumb = snippet.get('thumbnails', {}).get('standard', snippet.get('thumbnails', {}).get('default', {}))
                width = thumb.get('width', 1)
                height = thumb.get('height', 1)
                
                # 判定: 180秒以内 かつ 横幅が高さ以下なら Shorts
                video_type = "Shorts" if (duration_sec <= 180 and width <= height) else "Long"

                final_data.append({
                    'title': snippet['title'],
                    'channel': snippet['channelTitle'],
                    'published_at': snippet['publishedAt'],
                    'video_id': item['id'],
                    'view_count': int(stats.get('viewCount', 0)),
                    'like_count': int(stats.get('likeCount', 0)),
                    'comment_count': int(stats.get('commentCount', 0)),
                    'duration': content.get('duration', "PT0S"),
                    'video_type': video_type
                })
        except: continue
    return final_data

def prepare_rows(video_data, exec_time_jst):
    headers = [
        "動画タイトル", "チャンネル名", "投稿日時（日本時間）", "動画ID",
        "動画URL", "再生回数", "高評価数", "視聴者コメント数", "動画の長さ",
        "Short判定", "エンゲージメント率(%)", "ダウンロード実行時間（日本時間）"
    ]
    rows = []
    for v in video_data:
        eng_rate = calc_engagement_rate(v['like_count'], v['comment_count'], v['view_count'])
        rows.append([
            v['title'], v['channel'], convert_to_japan_time(v['published_at']),
            v['video_id'], f"https://www.youtube.com/watch?v={v['video_id']}",
            v['view_count'], v['like_count'], v['comment_count'],
            iso8601_to_duration(v['duration']), v['video_type'],
            eng_rate, exec_time_jst
        ])
    return headers, rows

def save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows):
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.add_worksheet(title=sheet_name, rows=str(len(rows)+100), cols="20")
    ws.update('A1', [headers])
    if rows:
        ws.update('A2', rows, value_input_option='USER_ENTERED')
    print(f"✅ 履歴用シート({sheet_name})に保存完了")

def save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows):
    sh = gc.open_by_key(dist_spreadsheet_id)
    ws = sh.get_worksheet(0)
    ws.clear()
    ws.update_title("Latest_Data")
    ws.update('A1', [headers])
    if rows:
        ws.update('A2', rows, value_input_option='USER_ENTERED')
    print(f"✅ 配布用シート(Latest_Data)を更新完了")

def check_if_processed(service_account_key, spreadsheet_id, sheet_name):
    try:
        creds = Credentials.from_service_account_info(json.loads(service_account_key), scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        return sheet_name in [ws.title for ws in sh.worksheets()], gc
    except Exception as e:
        print(f"エラー: {e}")
        sys.exit(1)

def main():
    api_key, spreadsheet_id, dist_spreadsheet_id, sa_key = get_env_vars()
    sheet_name = get_current_japan_digit_date()
    is_processed, gc = check_if_processed(sa_key, spreadsheet_id, sheet_name)
    
    if is_processed:
        print(f"✅ 本日の処理（{sheet_name}）は既に完了しているためスキップします。")
        return

    channel_ids = read_channel_ids('channel_ID.txt')
    exec_time_jst = get_current_japan_time()
    all_video_data = []

    print(f"➡️ YouTubeデータ取得開始 (対象チャンネル: {len(channel_ids)}件)")
    for idx, cid in enumerate(channel_ids, 1):
        print(f"   [{idx}/{len(channel_ids)}] Channel: {cid} 取得中...")
        videos = get_all_videos_since_2025(api_key, cid)
        all_video_data.extend(videos)

    if not all_video_data:
        print("⚠️ 対象動画が見つかりませんでした。")
        return

    # 重複排除と再生回数順ソート
    unique_v = {v['video_id']: v for v in all_video_data}.values()
    final_list = sorted(list(unique_v), key=lambda x: x['view_count'], reverse=True)

    headers, rows = prepare_rows(final_list, exec_time_jst)

    save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows)
    save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows)

    print("🎉 全ての処理が正常に完了しました。")

if __name__ == "__main__":
    main()
