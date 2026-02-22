import gspread
from datetime import datetime, timedelta, timezone

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

def init_sheet():
    # 🔥 여기도 한국 시간(KST) 강제 주입!
    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"🧹 [초기화 봇 가동] 현재 한국 시간: {now_kst}")
    
    try:
        # 구글 시트 연결
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        ws = gc.open_by_url(SHEET_URL).get_worksheet(0)
        
        # 1. 기존 데이터 싹 날리기 (초기화)
        ws.clear()
        print("✅ 기존 데이터 삭제 완료!")
        
        # 2. 루커 스튜디오가 인식할 수 있게 첫 줄(헤더) 다시 세팅
        headers = ['date', 'gallery', 'env', 'pos', 'url', 'img', 'text']
        ws.append_row(headers)
        print("✅ 첫 줄(헤더) 세팅 완료! 수집 봇들을 출동시킬 준비가 되었습니다.")
        
    except Exception as e:
        print(f"❌ 초기화 중 에러 발생: {e}")

if __name__ == "__main__":
    init_sheet()
