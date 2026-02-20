import asyncio
import random
from playwright.async_api import async_playwright
import gspread
from datetime import datetime

# --- ⚙️ 설정 ---
SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

# 📝 여기에 모니터링할 갤러리 20개든 30개든 자유롭게 추가하세요!
TARGET_GALLERIES = [
    {"name": "학점은행제갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=acbs", "mo": "https://m.dcinside.com/board/acbs"},
    {"name": "토익갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toeic", "mo": "https://m.dcinside.com/board/toeic"},
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "편입갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=admission", "mo": "https://m.dcinside.com/board/admission"}
]

# 🎯 루커 스튜디오 필터 7종 100% 매핑 함수
def get_korean_position(env, page_type, raw_pos, img_src):
    raw = str(raw_pos).lower()
    
    # 1. 이미지가 없으면 텍스트배너
    if not img_src: return "텍스트배너"
    
    # 2. X버튼이 있는 팝업레이어나 플로팅 아이콘은 아이콘배너
    if "icon" in raw or "float" in raw or "pop-layer" in raw: return "아이콘배너"
    
    if env == "PC":
        if page_type == "본문": 
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "게시글배너"
        else: # 리스트 페이지
            if "right" in raw or "wing" in raw: return "우측배너"
            if "left" in raw: return "좌측배너"
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "상단배너"
    else: # 모바일 (MO)
        if page_type == "본문":
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "게시글배너"
        else:
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "상단배너"

# 🔗 최종 랜딩 URL 즉시 추적
async def get_final_landing_url(context, redirect_url):
    if not redirect_url or not redirect_url.startswith("http"): return redirect_url
    if "addc.dcinside" not in redirect_url and "NetInsight" not in redirect_url: return redirect_url
    try:
        temp_page = await context.new_page()
        await temp_page.goto(redirect_url, wait_until="commit", timeout=4000)
        final_url = temp_page.url
        await temp_page.close()
        return final_url
    except:
        return redirect_url

# ⚡ 불필요한 자원 다운로드 차단 (속도 향상)
async def block_unnecessary_resources(route):
    if route.request.resource_type in ["font", "media", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# 🔍 스마트 광고 탐색 (광고 노출 기준 10회 포착)
async def capture_all_visible_ads(context, page, env, gallery_name, page_type):
    collected = []
    seen_keys = set()
    today_str = datetime.now().strftime("%Y-%m-%d")
    prefix = f"[{env} | {gallery_name[:5]} | {page_type}]"
    
    valid_refreshes = 0
    max_attempts = 35 # 광고가 너무 안 뜰 경우를 대비한 무한 루프 방지
    attempt = 0
    
    print(f"\n   🔍 {prefix} 유효 광고 10회 포착 모드 시작...")
    
    while valid_refreshes < 10 and attempt < max_attempts:
        attempt += 1
        found_ad_in_this_round = False
        
        try:
            await page.reload(wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2.5) 
            if page_type == "본문":
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(1.5)
        except: pass

        for frame in page.frames:
            try:
                ads = await frame.locator("a").all()
                for ad in ads:
                    href = await ad.get_attribute("href") or ""
                    img = ad.locator("img")
                    img_src = await img.first.evaluate("node => node.src") if await img.count() > 0 else ""
                    raw_pos = await ad.evaluate("node => { let p = node.closest('div'); return p ? p.className : 'unknown'; }")
                    
                    if "google" in href.lower() or "adsrvr.org" in href.lower() or "criteo" in href.lower(): continue
                    if "googleactiveview" in str(raw_pos).lower(): continue
                    if "dc/w/images" in img_src or "info_polic" in href or "close" in img_src.lower(): continue
                    if href == "#" or "javascript" in href.lower() or not href: continue
                        
                    is_ad = any(k in href or k in (img_src or "") for k in ["addc.dcinside", "NetInsight", "nstatic.dcinside", "toast.com"])
                    
                    if is_ad:
                        found_ad_in_this_round = True # 광고가 화면에 하나라도 떴음을 확인!
                        key = img_src if img_src else href
                        
                        if key not in seen_keys:
                            seen_keys.add(key)
                            final_url = await get_final_landing_url(context, href)
                            text_val = (await img.first.get_attribute("alt") if img_src else await ad.inner_text()) or "이미지 배너"
                            korean_pos = get_korean_position(env, page_type, raw_pos, img_src)
                            
                            print(f"✅ {prefix} [{valid_refreshes+1}/10회차] {korean_pos} 포착!")
                            
                            collected.append({
                                "date": today_str, "gallery": gallery_name, "env": env,
                                "pos": korean_pos, "url": final_url, "img": img_src, "text": text_val.strip()
                            })
            except: continue
        
        # 이번 새로고침에서 광고를 1개라도 봤다면 유효 카운트 증가!
        if found_ad_in_this_round:
            valid_refreshes += 1
        else:
            print(f"      ⚠️ {prefix} 빈 구좌(광고 없음). 재시도 중... (누적 {attempt}회)")
            
    return collected

# ⚡ 병렬 작업 함수
async def run_scraper_task(sem, context, env, target):
    async with sem:
        await asyncio.sleep(random.uniform(0, 2)) 
        final_data = []
        page = await context.new_page()
        await page.route("**/*", block_unnecessary_resources)
        
        url = target['pc'] if env == "PC" else target['mo']
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            final_data.extend(await capture_all_visible_ads(context, page, env, target['name'], "리스트"))
            
            post = page.locator("tr.us-post:not(.notice) td.gall_tit > a:not(.reply_numbox)").first if env == "PC" else page.locator("ul.gall-detail-lst li:not(.notice) .gall-detail-lnktit a").first
                
            if await post.count() > 0:
                await post.click()
                await asyncio.sleep(2)
                final_data.extend(await capture_all_visible_ads(context, page, env, target['name'], "본문"))
        except Exception as e:
            print(f"⚠️ [{env}] {target['name']} 에러 발생 (건너뜀)")
        finally:
            await page.close()
            
        return final_data

async def main():
    print("==================================================")
    print("🚀 디시인사이드 대규모 광고 병렬 수집 (유효노출 보장 버전)")
    print("==================================================")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        pc_context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=ua)
        mo_context = await browser.new_context(**p.devices['iPhone 13']) 

        # 🚀 한 번에 동시 실행할 갤러리 탭 개수 (서버 과부하 방지를 위해 4~5개가 적당합니다)
        sem = asyncio.Semaphore(4)

        tasks = []
        for target in TARGET_GALLERIES:
            tasks.append(run_scraper_task(sem, pc_context, "PC", target))
            tasks.append(run_scraper_task(sem, mo_context, "MO", target))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_final_data = []
        for res in results:
            if isinstance(res, list): 
                all_final_data.extend(res)

        await browser.close()

    if all_final_data:
        print(f"\n📊 {len(all_final_data)}건의 데이터를 구글 시트에 업데이트 중...")
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        sh = gc.open_by_url(SHEET_URL)
        ws = sh.get_worksheet(0)
        
        all_rows = ws.get_all_values()
        today_str = datetime.now().strftime("%Y-%m-%d")
        new_sheet_data = [["날짜", "갤러리명", "환경", "위치", "URL", "이미지", "텍스트문구"]]
        
        if all_rows:
            for row in all_rows[1:]:
                if len(row) > 0 and row[0] != today_str: 
                    new_sheet_data.append(row)
                    
        for d in all_final_data:
            new_sheet_data.append([d['date'], d['gallery'], d['env'], d['pos'], d['url'], d['img'], d['text']])
            
        ws.clear()
        ws.append_rows(new_sheet_data)
        print("🎉 모든 작업이 초고속으로 완료되었습니다! 구글 시트를 확인하세요!")
    else:
        print("\n❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
