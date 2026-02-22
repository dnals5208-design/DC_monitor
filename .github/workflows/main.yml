import gspread
from datetime import datetime, timedelta, timezone

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

def smart_init_sheet():
    # 🔥 한국 시간(KST) 강제 적용
    KST = timezone(timedelta(hours=9))
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")
    
    print(f"🧹 [스마트 초기화 봇 가동] 기준 날짜: {today_kst}")
    
    try:
        # 구글 시트 연결
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        ws = gc.open_by_url(SHEET_URL).get_worksheet(0)
        
        # 1. 현재 시트의 모든 데이터 가져오기
        all_data = ws.get_all_values()
        
        if not all_data:
            ws.append_row(['date', 'gallery', 'env', 'pos', 'url', 'img', 'text'])
            print("✅ 빈 시트에 헤더를 추가했습니다.")
            return

        headers = all_data[0]
        
        # 🔥 [핵심 패치] 오늘 날짜(today_kst)가 아닌 '과거 데이터'만 안전하게 걸러내기
        historical_data = [row for row in all_data[1:] if row and row[0] != today_kst]
        
        # 2. 시트 전체 초기화 (데이터 덮어쓰기를 위해 잠시 비움)
        ws.clear()
        
        # 3. 헤더 다시 넣기
        ws.append_row(headers)
        
        # 4. 안전하게 대피시켰던 과거 데이터 다시 밀어넣기
        if historical_data:
            ws.append_rows(historical_data)
            print(f"✅ 과거 데이터 {len(historical_data)}개 안전하게 유지 완료!")
            
        print(f"✅ 오직 오늘({today_kst}) 쌓인 데이터만 깔끔하게 비웠습니다. (새 수집 준비 완료)")
        
    except Exception as e:
        print(f"❌ 초기화 중 에러 발생: {e}")

if __name__ == "__main__":
    smart_init_sheet()
