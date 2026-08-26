import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dateutil import parser as date_parser

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_env_vars():
    """環境変数からAPIキー、認証情報、2つのスプレッドシートIDを取得"""
    api_key = os.environ.get("YOUTUBE_API_KEY")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")           # 履歴保存用
    dist_spreadsheet_id = os.environ.get("DIST_SPREADSHEET_ID") # 配布用
    service_account_key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")

    if not api_key:
        print("エラー: 設定している環境変数 'YOUTUBE_API_KEY' が取得できません。")
        sys.exit(1)
    if not spreadsheet_id:
        print("エラー: 設定している環境変数 'SPREADSHEET_ID' が取得できません。")
        sys.exit(1)
    if not dist_spreadsheet_id:
        print("エラー: 設定している環境変数 'DIST_SPREADSHEET_ID' が取得できません。")
        sys.exit(1)
    if not service_account_key:
        print("エラー: 設定している環境変数 'GCP_SERVICE_ACCOUNT_KEY' が取得できません。")
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

def iso8601_to_seconds(iso_duration):
    """
    ISO8601形式の長さ(例: 'PT3M20S')を秒数に変換
    """
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return 0
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

def make_remarks(iso_duration, view_count):
    """
    備考列の文字列を作成
    - 3分未満(180秒未満)なら「3分未満」
    - それ以外で再生回数1万未満なら「1万回未満」
    - それ以外は空文字
    ※ 3分未満→1万回未満の順でチェックし、併記はしない
    """
    duration_seconds = iso8601_to_seconds(iso_duration)
    if duration_seconds < 180:
        return "3分未満"
    if view_count < 10000:
        return "1万回未満"
    return ""

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

def execute_with_retry(request, max_retries=5, wait_seconds=5):
    """
    YouTube APIリクエストを実行し、HTTP 500 / 503 の場合はリトライする。
    - max_retries: 最大リトライ回数
    - wait_seconds: 各リトライ間の待機秒数
    """
    for attempt in range(1, max_retries + 1):
        try:
            return request.execute()
        except HttpError as e:
            status = e.resp.status if hasattr(e, 'resp') else None
            if status in (500, 503):
                if attempt == max_retries:
                    print(f"   ?? リトライ上限到達 (HTTP {status})。処理を中断します: {e}")
                    raise
                else:
                    print(
                        f"   ?? HTTP {status} エラー発生。{wait_seconds}秒待機してリトライします "
                        f"({attempt}/{max_retries})"
                    )
                    time.sleep(wait_seconds)
                    continue
            else:
                print(f"   ?? HTTPエラー発生 (status={status}): {e}")
                raise
        except Exception as e:
            print(f"   ?? リクエスト実行エラー: {e}")
            raise

def get_uploads_playlist_id(youtube, channel_id):
    try:
        request = youtube.channels().list(
            id=channel_id,
            part='contentDetails'
        )
        response = execute_with_retry(request)
        if not response['items']:
            return None
        return response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        print(f"   ?? チャンネル情報取得エラー ({channel_id}): {e}")
        return None

def get_all_videos_since_2026(api_key, channel_id):
    """
    日本時間(JST)で 2026/01/01 以降に公開された動画のみ取得する
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    uploads_playlist_id = get_uploads_playlist_id(youtube, channel_id)
    if not uploads_playlist_id:
        print(f"   ?? チャンネルが見つかりませんまたは取得できません: {channel_id}")
        return []

    # 日本時間での締切日時（2026-01-01 00:00:00 JST）
    JST = timezone(timedelta(hours=9))
    cutoff_jst = datetime(2026, 1, 1, 0, 0, 0, tzinfo=JST)

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
            pl_response = execute_with_retry(pl_request)

            for item in pl_response['items']:
                published_at_str = item['snippet']['publishedAt']
                # APIは通常UTCで返すので、まずUTCとして解釈し、その後JSTに変換
                dt_utc = date_parser.parse(published_at_str)
                dt_jst = dt_utc.astimezone(JST)

                # 日本時間の cutoff_jst より前なら、それ以降の動画は取得しない
                if dt_jst < cutoff_jst:
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
            vid_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=','.join(batch_ids)
            )
            vid_response = execute_with_retry(vid_request)

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

def get_previous_view_counts(gc, spreadsheet_id, current_sheet_name):
    """
    履歴用スプレッドシートから「前日」または「最後に存在する日付」のシートを探し、
    動画IDごとの前回再生回数を返す。
    戻り値: { video_id: 前回再生回数(int) }
    """
    try:
        sh = gc.open_by_key(spreadsheet_id)
        worksheets = sh.worksheets()
        titles = [ws.title for ws in worksheets]

        # 日付形式(YYYYMMDD)だけを対象
        date_pattern = re.compile(r'^\d{8}$')
        date_titles = [t for t in titles if date_pattern.match(t)]

        if not date_titles:
            return {}

        # 現在のシートの日付
        current_date = datetime.strptime(current_sheet_name, "%Y%m%d").date()
        yesterday_str = (current_date - timedelta(days=1)).strftime("%Y%m%d")

        # 1. 前日シートがあればそれを使う
        if yesterday_str in date_titles:
            target_title = yesterday_str
        else:
            # 2. 前日がなければ、current_date より前で最大の日付を使う（直近実行日）
            previous_dates = [
                d for d in date_titles
                if datetime.strptime(d, "%Y%m%d").date() < current_date
            ]
            if not previous_dates:
                return {}
            target_title = max(previous_dates)

        ws = sh.worksheet(target_title)
        values = ws.get_all_values()
        if len(values) < 2:
            return {}

        header = values[0]

        # ヘッダーから「動画ID」「再生回数」の列インデックスを動的に取得
        try:
            id_idx = header.index("動画ID")
            view_idx = header.index("再生回数")
        except ValueError:
            # 古いシートでヘッダー名が変わっている場合などは安全側で何も返さない
            return {}

        prev_views = {}
        for row in values[1:]:
            # 行が短い場合はスキップ
            if len(row) <= max(id_idx, view_idx):
                continue
            video_id = row[id_idx].strip()
            if not video_id:
                continue
            raw_view = row[view_idx].replace(',', '').strip()
            try:
                view_count = int(raw_view)
            except ValueError:
                continue
            prev_views[video_id] = view_count

        print(f"?? 前回シート({target_title})から {len(prev_views)} 件の動画の再生回数を読み込みました")
        return prev_views

    except Exception as e:
        print(f"   ?? 前回再生回数の取得に失敗しました: {e}")
        return {}

def prepare_rows(video_data, exec_time_jst, prev_view_counts):
    """
    video_data: 今回取得した動画データのリスト
    exec_time_jst: ダウンロード実行時間（JST文字列）
    prev_view_counts: { video_id: 前回再生回数 } の辞書
    """
    headers = [
        "動画URL",                 # 1
        "動画タイトル",            # 2
        "チャンネル名",            # 3
        "投稿日時",                # 4 (JST)
        "再生回数",                # 5 (今回)
        "視聴者コメント数",        # 6
        "高評価数",                # 7
        "動画の長さ",              # 8 (HH:MM:SS)
        "前日再生回数",            # 9
        "再生回数差分",            # 10
        "エンゲージメント率(%)",  # 11
        "ダウンロード実行時間",    # 12
        "備考",                    # 13
        "動画ID",                  # 14
    ]

    rows = []
    for video in video_data:
        current_views = video['view_count']
        prev_views = prev_view_counts.get(video['video_id'], 0)  # 前回データがなければ0扱い
        diff_views = current_views - prev_views

        engagement_rate = calc_engagement_rate(
            video['like_count'],
            video['comment_count'],
            current_views
        )
        video_url = f"https://www.youtube.com/watch?v={video['video_id']}"
        jst_time = convert_to_japan_time(video['published_at'])
        duration_str = iso8601_to_duration(video['duration'])
        remarks = make_remarks(video['duration'], current_views)

        rows.append([
            video_url,              # 動画URL
            video['title'],         # 動画タイトル
            video['channel'],       # チャンネル名
            jst_time,               # 投稿日時(JST)
            current_views,          # 再生回数
            video['comment_count'], # 視聴者コメント数
            video['like_count'],    # 高評価数
            duration_str,           # 動画の長さ
            prev_views,             # 前日再生回数
            diff_views,             # 再生回数差分
            engagement_rate,        # エンゲージメント率(%)
            exec_time_jst,          # ダウンロード実行時間
            remarks,                # 備考
            video['video_id'],      # 動画ID
        ])
    return headers, rows

def save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows):
    """【履歴用】新規シート作成（同名シートがある場合はエラーになる前提）
       追加したシートを一番左に配置する
    """
    sh = gc.open_by_key(spreadsheet_id)
    # まず一番右に追加
    worksheet = sh.add_worksheet(title=sheet_name, rows=str(len(rows)+100), cols="20")

    # 追加したシートを一番左に移動
    try:
        all_ws = sh.worksheets()
        reordered = [worksheet] + [ws for ws in all_ws if ws.id != worksheet.id]
        sh.reorder_worksheets(reordered)
        print(f"? 履歴用シート({sheet_name})を一番左に配置しました")
    except Exception as e:
        print(f"   ?? シート並び替えに失敗しましたが、シート自体は作成されています: {e}")

    worksheet.update('A1', [headers])
    if rows:
        worksheet.update('A2', rows, value_input_option='USER_ENTERED')
    print(f"? 履歴用シート({sheet_name})に保存完了")

def save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows):
    """【配布用】上書き（履歴と同じ列構成）"""
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
            return True, gc  # 存在するのでTrueと、ついでに認証済みクライアントを返す
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

    print(f"?? YouTubeデータ取得開始 (対象チャンネル: {len(channel_ids)}件, 2026年以降・日本時間基準)")
    all_video_data = []
    for idx, channel_id in enumerate(channel_ids, 1):
        print(f"   [{idx}/{len(channel_ids)}] Channel ID: {channel_id} 処理中...")
        channel_videos = get_all_videos_since_2026(api_key, channel_id)
        print(f"     -> {len(channel_videos)}件 取得完了")
        all_video_data.extend(channel_videos)

    if not all_video_data:
        print("?? 動画が1件も見つかりませんでした。")
        return

    # 動画IDでユニーク化
    unique_videos = {v['video_id']: v for v in all_video_data}.values()
    final_list = list(unique_videos)
    # 再生回数降順ソート
    final_list.sort(key=lambda x: x['view_count'], reverse=True)

    print(f"?? 合計 {len(final_list)} 件の動画を出力します...")

    # 前回シートから再生回数を取得（前日優先、なければ直近実行日）
    prev_view_counts = get_previous_view_counts(gc, spreadsheet_id, sheet_name)

    # 前日再生回数・差分・備考を含めた行データ作成
    headers, rows = prepare_rows(final_list, exec_time_jst, prev_view_counts)

    # 1. 履歴用へ保存（新規シートを一番左に追加）
    save_to_history_sheet(gc, spreadsheet_id, sheet_name, headers, rows)

    # 2. 配布用へ上書き保存（同じ列構成）
    save_to_distribution_sheet(gc, dist_spreadsheet_id, headers, rows)

    print("  全処理完了")

if __name__ == "__main__":
    main()
