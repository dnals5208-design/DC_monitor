import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# 환경 변수 (GitHub Secrets에서 설정할 값들)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
TARGET_URL = "https://gall.dcinside.com/board/lists/?id=coq" # 모니터링할 갤러리 주소

def send_telegram_msg(message, image_path=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/"
    if image_path:
        files = {'photo': open(image_path, 'rb')}
        requests.post(url + "sendPhoto", data={'chat_id': CHAT_ID, 'caption': message}, files=files)
    else:
        requests.post(url + "sendMessage", data={'chat_id': CHAT_ID, 'text': message})

def monitor_dc_ads():
    chrome_options = Options()
    chrome_options.add_argument('--headless') # 서버에서 실행되므로 화면 없이 실행
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(TARGET_URL)
        time.sleep(5) # 광고 로딩 대기

        # 1. 리스트 상단 배너 영역 탐색 (디시 배너의 일반적인 선택자)
        # 배너 선택자는 주기적으로 바뀔 수 있으므로 확인이 필요합니다.
        ad_selectors = [
            "div.top_ad",           # 일반적인 상단 광고창
            "#zzbang_img",          # 자동 짤방/배너 영역
            "div.ad_top_banner"     # 상단 확장형 배너
        ]
        
        found = False
        for selector in ad_selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if elements and elements[0].is_displayed():
                # 광고 영역 스크린샷 저장
                elements[0].screenshot("ad_capture.png")
                send_telegram_msg(f"✅ 광고 정상 작동 중: {selector}", "ad_capture.png")
                found = True
                break
        
        if not found:
            driver.save_screenshot("full_page.png")
            send_telegram_msg("⚠️ 광고가 발견되지 않았습니다! 확인이 필요합니다.", "full_page.png")

    except Exception as e:
        send_telegram_msg(f"❌ 모니터링 중 오류 발생: {str(e)}")
    finally:
        driver.quit()

if __name__ == "__main__":
    monitor_dc_ads()
