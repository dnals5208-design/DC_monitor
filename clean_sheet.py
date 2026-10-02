import gspread
from datetime import datetime, timedelta, timezone

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 

# 🔥 [자동 라우팅 설정] (dc_monitor.py와 완벽히 동일하게 세팅)
SHEET_URLS = [
    'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0', 
    'https://docs.google.com/spreadsheets/d/1T_m7FXkFwjWqDPeNzNPVYx-AWwNd3jyEuHCic6oZNjw/edit?gid=0#gid=0', 
    'https://docs.google.com/spreadsheets/d/17BQ0QYStEXvZTVwdmrHdymB1rB1PSuccClayv_BjJnY/edit?gid=0#gid=0',
    'https://docs.google.com/spreadsheets/d/1krVb_3wlhRSyld3I9auzz__Dye3LRGSZ6O3XETIVH0k/edit?gid=0#gid=0',
    'https://docs.google.com/spreadsheets/d/1uFWAT2hiwXq50iGzwlyUNzzA80IM6P25JWWGnDZzZbI/edit?gid=0#gid=0',
    'https://docs.google.com/spreadsheets/d/1juYK2brq-UHPkCElSi6ENrR6vpehmy8xbIH9YO3DtV8/edit?gid=0#gid=0'
]
ROW_LIMIT = 300000 

def smart_init_sheet():
    KST = timezone(timedelta(hours=9))
    today_kst = datetime.now(KST).strftime("%Y-%m-%d")
    print(f"🧹 [스마트 초기화 & 자동 라우팅] 기준 날짜: {today_kst}")
    
    try:
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        
        active_ws = None
        dates = []
        
        for idx, url in enumerate(SHEET_URLS):
            ws = gc.open_by_url(url).get_worksheet(0)
            current_dates = ws.col_values(1)
            
            if (today_kst in current_dates) or (len(current_dates) < ROW_LIMIT):
                active_ws = ws
                dates = current_dates
                print(f"🎯 [청소 봇 타겟 시트 확정] {idx+1}번 시트에 연결되었습니다. (현재 {len(dates)}줄 / 최대 {ROW_LIMIT}줄)")
                break
                
        if not active_ws:
            print("❌ [비상] 모든 구글 시트가 꽉 찼습니다! SHEET_URLS에 새로운 시트 URL을 추가해주세요.")
            return

        if not dates:
            active_ws.append_row(['date', 'gallery', 'env', 'pos', 'url', 'img', 'text'])
            print("✅ 빈 시트에 헤더를 추가했습니다.")
            return

        try:
            first_today_index = dates.index(today_kst)
            first_today_row = first_today_index + 1
            total_rows = len(dates)
            
            print(f"🗑️ {first_today_row}행부터 {total_rows}행까지 (오늘 수집 잔해) 삭제 진행...")
            active_ws.delete_rows(first_today_row, total_rows)
            print(f"✅ 오직 오늘({today_kst}) 쌓인 찌꺼기만 비웠습니다. 과거 데이터는 100% 보존됩니다.")
        except ValueError:
            print(f"✨ 오늘({today_kst}) 기록된 찌꺼기 데이터가 없어 삭제를 생략합니다.")
            
    except Exception as e:
        print(f"❌ 초기화 중 에러 발생: {e}")

if __name__ == "__main__":
    smart_init_sheet()
