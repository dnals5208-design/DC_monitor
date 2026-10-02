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
        
        # 1. 전체 데이터를 불러오는 멍청한 짓 금지! A열(날짜)만 가볍게 가져옵니다.
        dates = ws.col_values(1)
        
        if not dates:
            ws.append_row(['date', 'gallery', 'env', 'pos', 'url', 'img', 'text'])
            print("✅ 빈 시트에 헤더를 추가했습니다.")
            return

        # 2. 오늘 날짜(today_kst)가 처음으로 등장하는 행 번호 찾기
        first_today_row = None
        try:
            # 파이썬 리스트는 0부터 시작, 구글 시트 행은 1부터 시작하므로 +1
            first_today_index = dates.index(today_kst)
            first_today_row = first_today_index + 1
        except ValueError:
            pass # 오늘 날짜가 아직 시트에 한 줄도 없으면 에러 없이 넘어감

        # 3. 과거 데이터는 1%도 안 건드리고, 딱 오늘 데이터 부분만 잘라내기
        if first_today_row:
            total_rows = len(dates)
            print(f"🗑️ {first_today_row}행부터 {total_rows}행까지 (오늘 찌꺼기 데이터) 삭제 진행...")
            
            # delete_rows(시작행, 끝행) -> 이 한 줄로 깔끔하고 안전하게 삭제 완료
            ws.delete_rows(first_today_row, total_rows)
            print(f"✅ 오직 오늘({today_kst}) 쌓인 데이터만 깔끔하게 비웠습니다. 과거 데이터는 100% 보존됩니다.")
        else:
            print(f"✨ 오늘({today_kst}) 날짜로 기록된 데이터가 없어 삭제할 내용이 없습니다.")
            
    except Exception as e:
        print(f"❌ 초기화 중 에러 발생: {e}")

if __name__ == "__main__":
    smart_init_sheet()
