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
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")         # 履歴保存用
    dist_spreadsheet_id = os.environ.get("DIST_SPREADSHEET_ID") # 配布用
    service_account_key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")

    if not api_key:
        print("エラー: 環境変数 'YOUTUBE_API_KEY' が設定されていません。")
        sys.exit(1)
    if not spreadsheet_id:
        print("エラー: 環境変数 'SPREADSHEET_ID' が設定されていません。")
        sys.exit(1)
    if not dist_spreadsheet_id:
        print("エラー: 環境変数 'DIST_SPREADSHEET_ID' が設定されていません。")
        sys.exit(1)
    if not service_account_key:
        print("エラー: 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
        sys.exit(1)

    return api_key, spreadsheet_id, dist_spreadsheet_id, service_account_key

def read_channel_ids(file_path):
    if not os.path.exists(file_path):
        print(f"エラー: {file_path} が見つかりません。")
        sys.exit(1)
    with open(file_path, 'r', encoding='utf-8') as file:
        channel_ids = [line.strip() for line in file if line.strip()]
    unique_ids = list(set(channel_ids))
    if not unique_ids:
        print("エラー: チャンネルIDが記載されていません。")
        sys.exit(1)
    return unique_ids

def iso8601_to_duration(iso_duration):
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return "00:00:00"
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return str(timedelta(hours=hours, minutes=minutes, seconds=seconds))

def convert_to_japan_time(utc_time_str):
    utc_dt = date_parser.parse(utc_time_str)
    JST = timezone(timedelta(hours=9))
    japan_dt = utc_dt.astimezone(JST)
    return japan_dt.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_time():
    now_utc = datetime.now(timezone.utc)
    JST = timezone(timedelta(hours=9))
    now_jst = now_utc.astimezone(JST)
    return now_jst.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_digit_date():
    now_utc = datetime.now(timezone.utc)
    JST = timezone(timedelta(hours=9))
    now_jst = now_utc.astimezone(JST)
    return now_jst.strftime("%Y%m%d")

def calc_engagement_rate(like_count, comment_count, view_count):
    if view_count == 0:
        return 0.0
    return round((like_count + comment_count) / view_count * 100, 2)

def get_uploads_playlist_id(youtube, channel_id):
    try:
        response = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        ).execute()
        if not response['items']:
            return None
        return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        print(f"   ?? チャンネル情報取得エラー ({channel_id}): {e}")
        return None

def get_all_videos_since_2025(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)
    uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
    if not uploads_playlist_id:
        print(f"   ?? チャンネルが見つかりませんまたは取得できません: {channel_id}")
        return []

    cutoff_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    video_ids = []
    next_page_token = None
    is_fetching = True

    while is_fetching:
        try:
            pl_request = youtube.playlistItems().list(
                playlistId=uploads_playlist_id,
                part='snippet',
                maxResults=50,
                pageToken=next_page_token
            )
            pl_response = pl_request.execute()
            for item in pl_response['items']:
                published_at_str = item['snippet']['publishedAt']
                dt = date_parser.parse(published_at_str)
                if dt < cutoff_date:
                    is_fetching = False
                    break
                video_ids.append(item['snippet']['resourceId']['videoId'])
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            print(f"   ?? プレイリスト取得エラー: {e}")
            break

    final_video_data = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        try:
            vid_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(batch_ids)
            ).execute()
            for item in vid_response['items']:
                snippet = item['snippet']
                statistics = item.get('statistics', {})
                content_details = item['contentDetails']
                final_video_data.append({
                    'title': snippet['title'],
                    'channel': snippet['channelTitle'],
                    'published_at': snippet['publishedAt'],
                    'video_id': item['id'],
                    'view_count': int(statistics.get('viewCount', 0)),
                    'like_count': int(statistics.get('likeCount', 0)),
                    'comment_count': int(statistics.get('commentCount', 0)),
                    'duration': content_details.get('duration', "PT0S")
                })
        except Exception as e:
            print(f"   ?? 詳細取得エラー: {e}")
            continue
    return final_video_data

def prepare_rows(video_data, exec_time_jst):
    headers = [
        "動画タイトル", "チャンネル名", "投稿日時（日本時間）", "動画ID",
        "動画URL", "再生回数", "高評価数", "視聴者コメント数", "動画の長さ",
        "エンゲージメント率(%)", "ダウンロード実行時間（日本時間）"
    ]
    rows = []
    for video in video_data:
        engagement_rate = calc_engagement_rate(video['like_count'], video['comment_count'], video['view_count'])
        video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
        jst_time = convert_to_japan_time(video['published_at'])
        rows.append([
            video['title'],
            video['channel'],
            jst_time,
            video['video_id'],
            video_url,
            video['view_count'],
            video['like_count'],
            video['comment_count'],
            iso8601_to_duration(video['duration']),
            engagement_rate,
            exec_time_jst
        ])
    return headers, rows

def save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows):
    """【履歴用】新規シート作成（同名シートがある場合はエラーになる前提）"""
    sh = gc.open_by_key(spreadsheet_id)
    # ここでは単純にadd_worksheetするだけ（重複チェックはmain関数で済み）
    worksheet = sh.add_worksheet(title=sheet_name, rows=str(len(rows)+100), cols="20")
    
    worksheet.update('A1', [headers])
    if rows:
        worksheet.update('A2', rows, value_input_option='USER_ENTERED')
    print(f"? 履歴用シート({sheet_name})に保存完了")

def save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows):
    """【配布用】上書き"""
    sh = gc.open_by_key(dist_spreadsheet_id)
    worksheet = sh.get_worksheet(0)
    worksheet.clear()
    worksheet.update_title("Latest_Data")
    worksheet.update('A1', [headers])
    if rows:
        worksheet.update('A2', rows, value_input_option='USER_ENTERED')
    print(f"? 配布用シート(Latest_Data)を上書き更新完了")

def check_if_processed(service_account_key, spreadsheet_id, sheet_name):
    """履歴用スプレッドシートに既に今日のシートがあるか確認"""
    try:
        credentials_dict = json.loads(service_account_key)
        creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        
        existing_sheets = [ws.title for ws in sh.worksheets()]
        if sheet_name in existing_sheets:
            return True, gc # 存在するのでTrueと、ついでに認証済みクライアントを返す
        return False, gc
    except Exception as e:
        print(f"エラー: スプレッドシートの確認に失敗しました: {e}")
        sys.exit(1)

def main():
    channel_id_file = 'channel_ID.txt'
    api_key, spreadsheet_id, dist_spreadsheet_id, service_account_key = get_env_vars()
    
    sheet_name = get_current_japan_digit_date()
    
    # --- 【スキップ機能】実行済みチェック ---
    is_processed, gc = check_if_processed(service_account_key, spreadsheet_id, sheet_name)
    if is_processed:
        print(f"? シート '{sheet_name}' は既に存在するため、本日の処理をスキップします。")
        print("   (配布用シートの更新も行いません)")
        return
    # ------------------------------------

    channel_ids = read_channel_ids(channel_id_file)
    exec_time_jst = get_current_japan_time()

    print(f"?? YouTubeデータ取得開始 (対象チャンネル: {len(channel_ids)}件, 2025年以降)")
    all_video_data = []
    for idx, channel_id in enumerate(channel_ids, 1):
        print(f"   [{idx}/{len(channel_ids)}] Channel ID: {channel_id} 処理中...")
        channel_videos = get_all_videos_since_2025(api_key, channel_id)
        print(f"     -> {len(channel_videos)}件 取得完了")
        all_video_data.extend(channel_videos)

    if not all_video_data:
        print("?? 動画が1件も見つかりませんでした。")
        return

    unique_videos = {v['video_id']: v for v in all_video_data}.values()
    final_list = list(unique_videos)
    final_list.sort(key=lambda x: x['view_count'], reverse=True)

    print(f"?? 合計 {len(final_list)} 件の動画を出力します...")

    headers, rows = prepare_rows(final_list, exec_time_jst)

    # チェック時に作ったgc(gspread client)を再利用して書き込み
    # 1. 履歴用へ保存
    save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows)

    # 2. 配布用へ上書き保存
    save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows)

    print(f"  全処理完了")

if __name__ == "__main__":
    main()

