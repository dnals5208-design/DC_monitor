import asyncio
import random
from playwright.async_api import async_playwright
import gspread
from datetime import datetime

# --- ⚙️ 설정 ---
SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

TARGET_GALLERIES = [
    {"name": "학점은행제갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=acbs", "mo": "https://m.dcinside.com/board/acbs"},
    {"name": "토익갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toeic", "mo": "https://m.dcinside.com/board/toeic"},
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "편입갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=admission", "mo": "https://m.dcinside.com/board/admission"}
]

def translate_position(env, page_type, raw_pos):
    raw = str(raw_pos).lower()
    if env == "PC":
        if page_type == "본문": return "본문짤방(게시글배너)"
        if "right" in raw or "wing" in raw or "pop-layer" in raw: return "리스트 우측"
        if "left" in raw: return "리스트 좌측"
        return "리스트 상단"
    else: 
        if page_type == "본문":
            if "mid" in raw: return "본문 중간"
            if "float" in raw or "icon" in raw or "pop-layer" in raw: return "아이콘 플로팅"
            return "본문 짤방"
        return "리스트 상단"

# 🚀 최적화 1: 랜딩 추적 시 화면 로딩 기다리지 않고 주소만 즉시 탈취
async def get_final_landing_url(context, redirect_url):
    if not redirect_url or not redirect_url.startswith("http"): return redirect_url
    if "addc.dcinside" not in redirect_url and "NetInsight" not in redirect_url: return redirect_url
        
    try:
        temp_page = await context.new_page()
        # domcontentloaded 대신 commit 사용 -> 이동(리다이렉트) 즉시 종료
        await temp_page.goto(redirect_url, wait_until="commit", timeout=4000)
        final_url = temp_page.url
        await temp_page.close()
        return final_url
    except:
        return redirect_url

# 🚀 최적화 2: 불필요한 리소스(폰트, 미디어) 차단하여 로딩 속도 극대화
async def block_unnecessary_resources(route):
    if route.request.resource_type in ["font", "media", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

async def capture_all_visible_ads(context, page, env, gallery_name, page_type):
    collected = []
    seen_keys = set()
    today_str = datetime.now().strftime("%Y-%m-%d")
    prefix = f"[{env} | {gallery_name[:5]} | {page_type}]"
    
    for i in range(1, 11):
        try:
            # 타임아웃을 15초로 줄이고 대기 시간을 타이트하게 가져갑니다.
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
                        key = img_src if img_src else href
                        if key not in seen_keys:
                            seen_keys.add(key)
                            
                            final_url = await get_final_landing_url(context, href)
                            text_val = (await img.first.get_attribute("alt") if img_src else await ad.inner_text()) or "이미지 배너"
                            korean_pos = translate_position(env, page_type, raw_pos)
                            
                            print(f"✅ {prefix} {korean_pos} 포착! (랜딩: {final_url[:35]}...)")
                            
                            collected.append({
                                "date": today_str, "gallery": gallery_name, "env": env,
                                "pos": korean_pos, "url": final_url, "img": img_src, "text": text_val.strip()
                            })
            except: continue
    return collected

# 🚀 최적화 3: Semaphore를 통한 트래픽 제어 (한 번에 3개씩만 처리)
async def run_scraper_task(sem, context, env, target):
    async with sem:
        await asyncio.sleep(random.uniform(0, 2)) 
        final_data = []
        page = await context.new_page()
        
        # 네트워크 속도 향상을 위한 리소스 차단 적용
        await page.route("**/*", block_unnecessary_resources)
        
        url = target['pc'] if env == "PC" else target['mo']
        print(f"🌐 [{env}] {target['name']} 접속 완료. 탐색 시작!")
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
    print("🚀 디시인사이드 광고 초고속 병렬 수집 (서버 최적화 버전)")
    print("==================================================")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        pc_context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=ua)
        mo_context = await browser.new_context(**p.devices['iPhone 13']) 

        # 🔥 한 번에 실행되는 브라우저 탭 개수를 3개로 제한 (서버 과부하 원천 차단)
        sem = asyncio.Semaphore(3)

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
        print("🎉 모든 작업이 초고속으로 완료되었습니다!")
    else:
        print("\n❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
