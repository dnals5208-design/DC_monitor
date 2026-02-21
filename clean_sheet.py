import gspread
from datetime import datetime
import time

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

print("🧹 [시스템] 구글 시트 사전 청소를 시작합니다...")
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
ws = gc.open_by_url(SHEET_URL).get_worksheet(0)

rows = ws.get_all_values()
today = datetime.now().strftime("%Y-%m-%d")
kept = [["날짜", "갤러리명", "환경", "위치", "URL", "이미지", "텍스트문구"]]

if rows:
    for r in rows[1:]:
        if r and r[0] != today: kept.append(r)

ws.clear()
if len(kept) > 1:
    for i in range(0, len(kept), 100):
        ws.append_rows(kept[i:i+100])
        time.sleep(1)
        
print("✨ [시스템] 시트 초기화 완료! 이제 수집 로봇 5대가 출동합니다.")
