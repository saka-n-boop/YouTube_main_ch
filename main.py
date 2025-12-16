import json
import os
import re
import sys
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_env_vars():
    """環境変数からAPIキー、認証情報、スプレッドシートIDを取得"""
    api_key = os.environ.get("YOUTUBE_API_KEY")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    service_account_key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")

    if not api_key:
        print("エラー: 環境変数 'YOUTUBE_API_KEY' が設定されていません。")
        sys.exit(1)
    if not spreadsheet_id:
        print("エラー: 環境変数 'SPREADSHEET_ID' が設定されていません。")
        sys.exit(1)
    if not service_account_key:
        print("エラー: 環境変数 'GCP_SERVICE_ACCOUNT_KEY' が設定されていません。")
        sys.exit(1)

    return api_key, spreadsheet_id, service_account_key

def read_channel_ids(file_path):
    """channel_ID.txt からチャンネルIDのリストを読み込む"""
    if not os.path.exists(file_path):
        print(f"エラー: {file_path} が見つかりません。")
        sys.exit(1)
        
    with open(file_path, 'r', encoding='utf-8') as file:
        # 空行を除去してリスト化
        channel_ids = [line.strip() for line in file if line.strip()]
    
    if not channel_ids:
        print("エラー: チャンネルIDが記載されていません。")
        sys.exit(1)
        
    return channel_ids

def jst_to_utc(jst_str):
    """JST日時文字列をUTCのISO8601に変換"""
    jst_dt = datetime.strptime(jst_str, "%Y-%m-%d %H:%M:%S")
    utc_dt = jst_dt - timedelta(hours=9)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def iso8601_to_duration(iso_duration):
    """PT表記（YouTube ISO8601）をHH:MM:SS化"""
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return "00:00:00"
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return str(timedelta(hours=hours, minutes=minutes, seconds=seconds))

def convert_to_japan_time(utc_time):
    """UTC時刻をJST変換し表示用に"""
    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")
    japan_datetime = utc_datetime + timedelta(hours=9)
    return japan_datetime.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_time():
    """現在時刻 (JST表示)"""
    now_utc = datetime.utcnow()
    now_jst = now_utc + timedelta(hours=9)
    return now_jst.strftime("%Y/%m/%d %H:%M:%S")

def get_current_japan_digit_date():
    """今日の日付 (JST, シート名用 'YYYYMMDD' フォーマット)"""
    now_utc = datetime.utcnow()
    now_jst = now_utc + timedelta(hours=9)
    return now_jst.strftime("%Y%m%d")

def calc_engagement_rate(like_count, comment_count, view_count):
    """エンゲージメント率 (％)"""
    if view_count == 0:
        return 0.0
    return round((like_count + comment_count) / view_count * 100, 2)

def get_youtube_data_by_channel(api_key, channel_id, start_datetime_jst, end_datetime_jst, max_total_results=500):
    """
    指定チャンネル・期間の動画情報を取得
    ※ search APIを使用するため、APIコスト(100/req)に注意。
    ※ max_total_resultsは多めに設定していますが、API制限考慮のため調整してください。
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    start_utc = jst_to_utc(start_datetime_jst)
    end_utc = jst_to_utc(end_datetime_jst)
    start_dt = datetime.strptime(start_datetime_jst, "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(end_datetime_jst, "%Y-%m-%d %H:%M:%S")

    video_ids = []
    next_page_token = None

    # 検索ループ（指定期間内の動画IDを収集）
    while len(video_ids) < max_total_results:
        try:
            search_response = youtube.search().list(
                channelId=channel_id,  # チャンネルID指定
                part='snippet',
                type='video',          # 動画のみ
                order='date',          # 日付順（新しい順）
                maxResults=min(50, max_total_results - len(video_ids)),
                publishedAfter=start_utc,
                publishedBefore=end_utc,
                pageToken=next_page_token
            ).execute()
        except Exception as e:
            print(f"   ⚠️ APIエラー (Channel ID: {channel_id}): {e}")
            break

        video_ids += [item['id']['videoId'] for item in search_response['items']]
        next_page_token = search_response.get('nextPageToken')
        
        # 次のページがない、または上限に達したら終了
        if not next_page_token or len(video_ids) >= max_total_results:
            break

    # 詳細データ取得（統計情報など）
    video_data = []
    # 50件ずつバッチ処理
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i+50]
        try:
            video_response = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(batch_ids)
            ).execute()

            for item in video_response['items']:
                snippet = item['snippet']
                statistics = item.get('statistics', {})
                content_details = item['contentDetails']

                published_at_utc = snippet['publishedAt']
                published_at_jst = datetime.strptime(published_at_utc, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=9)

                # 念のための期間チェック
                if not (start_dt <= published_at_jst <= end_dt):
                    continue

                video_data.append({
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
            print(f"   ⚠️ 詳細取得エラー: {e}")
            continue

    return video_data

def merge_and_deduplicate(video_data_list):
    """
    複数チャンネルのリストを統合し、重複を排除（video_id基準）
    キーワードフィルタリングは行わず、取得した全動画を対象とする
    """
    merged = {}
    for video_data in video_data_list:
        for video in video_data:
            # video_idをキーにして上書き（重複排除）
            merged[video['video_id']] = video
    
    # 辞書の値（動画データ）をリストに戻して返却
    return list(merged.values())

def export_to_google_sheet(video_data, spreadsheet_id, service_account_key, exec_time_jst, sheet_name):
    """
    Googleスプレッドシートに出力
    """
    # サービスアカウント認証
    credentials_dict = json.loads(service_account_key)
    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)

    # 新しいシートを作成
    try:
        worksheet = sh.add_worksheet(title=sheet_name, rows=str(len(video_data)+10), cols="20")
    except gspread.exceptions.APIError as e:
        # シートが既に存在する場合などのハンドリング
        print(f"⚠️ シート作成エラー（既に存在している可能性があります）: {e}")
        worksheet = sh.worksheet(sheet_name)
        worksheet.clear() # 既存の場合はクリアして上書き

    headers = [
        "動画タイトル", "チャンネル名", "投稿日時（日本時間）", "動画ID",
        "動画URL", "再生回数", "高評価数", "視聴者コメント数", "動画の長さ",
        "エンゲージメント率(%)", "ダウンロード実行時間（日本時間）"
    ]
    rows = []
    for video in video_data:
        engagement_rate = calc_engagement_rate(video['like_count'], video['comment_count'], video['view_count'])
        video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
        rows.append([
            video['title'],
            video['channel'],
            convert_to_japan_time(video['published_at']),
            video['video_id'],
            video_url,
            video['view_count'],
            video['like_count'],
            video['comment_count'],
            iso8601_to_duration(video['duration']),
            engagement_rate,
            exec_time_jst
        ])
    
    # ヘッダーとデータをシートに追加
    worksheet.clear()
    worksheet.append_row(headers)
    if rows:
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')

def main():
    # 入力ファイル設定
    channel_id_file = 'channel_ID.txt'

    # 環境変数と設定の読み込み
    api_key, spreadsheet_id, service_account_key = get_env_vars()
    channel_ids = read_channel_ids(channel_id_file)

    # 日付設定
    sheet_name = get_current_japan_digit_date()
    exec_time_jst = get_current_japan_time()
    
    # 検索期間設定（2020年1月1日 〜 今日の10:01:00）
    # ※毎日実行しても「2020年からの全リスト」を取得する仕様です
    start_datetime_jst = "2022-01-01 00:00:00"
    end_datetime_jst = f"{sheet_name[:4]}-{sheet_name[4:6]}-{sheet_name[6:]} 23:59:59"

    # --- シート存在チェック（APIアクセス前） ---
    try:
        credentials_dict = json.loads(service_account_key)
        creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(spreadsheet_id)
        existing_sheets = [ws.title for ws in sh.worksheets()]
        
        if sheet_name in existing_sheets:
            print(f"✅ {sheet_name}シートは既に存在しているため処理をスキップします。")
            return
    except Exception as e:
        print(f"エラー: スプレッドシートへのアクセスに失敗しました。IDや権限を確認してください。\n{e}")
        sys.exit(1)

    # --- YouTube Data APIアクセス ---
    video_data_list = []
    print(f"➡️ YouTubeデータ取得開始 (対象チャンネル: {len(channel_ids)}件, 期間: {start_datetime_jst} 〜)")
    
    for channel_id in channel_ids:
        print(f"   - チャンネルID '{channel_id}' 検索中...")
        # 各チャンネル最大500件まで取得（APIコスト節約のため制限を設けています）
        video_data = get_youtube_data_by_channel(
            api_key, 
            channel_id, 
            start_datetime_jst, 
            end_datetime_jst, 
            max_total_results=500
        )
        video_data_list.append(video_data)
        print(f"     -> {len(video_data)}件取得")

    # データ統合、重複排除（タイトルフィルタリングなし）
    merged_video_data = merge_and_deduplicate(video_data_list)
    print(f"➡️ 重複排除後の総動画数: {len(merged_video_data)}件")
    
    if not merged_video_data:
        print("⚠️ 対象期間の動画が見つかりませんでした。")
        return

    # 再生回数でソート（降順）
    merged_video_data.sort(key=lambda x: x['view_count'], reverse=True)
    
    # Googleスプレッドシートに出力
    print("➡️ スプレッドシートへ出力中...")
    export_to_google_sheet(merged_video_data, spreadsheet_id, service_account_key, exec_time_jst, sheet_name)
    print(f"🎉 処理完了（シート名: {sheet_name}）")

if __name__ == "__main__":
    main()

