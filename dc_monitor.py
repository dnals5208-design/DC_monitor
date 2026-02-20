import asyncio
import random
import time
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

def get_korean_position(env, page_type, raw_pos, img_src):
    raw = str(raw_pos).lower()
    if not img_src: return "텍스트배너"
    if "icon" in raw or "float" in raw or "pop-layer" in raw: return "아이콘배너"
    
    if env == "PC":
        if page_type == "본문": 
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "게시글배너"
        else: 
            if "right" in raw or "wing" in raw: return "우측배너"
            if "left" in raw: return "좌측배너"
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "상단배너"
    else: 
        if page_type == "본문":
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "게시글배너"
        else:
            if "bottom" in raw or "btm" in raw: return "하단배너"
            return "상단배너"

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

async def block_unnecessary_resources(route):
    req_url = route.request.url
    if route.request.resource_type in ["font", "media", "stylesheet"]:
        await route.abort()
    elif route.request.resource_type == "image" and not any(k in req_url for k in ["dcinside", "toast.com", "ads"]):
        await route.abort()
    else:
        await route.continue_()

async def capture_all_visible_ads(context, page, env, gallery_name, page_type):
    collected = []
    seen_keys = set()
    today_str = datetime.now().strftime("%Y-%m-%d")
    prefix = f"[{env}|{gallery_name[:4]}|{page_type}]"
    
    valid_refreshes = 0
    attempt = 0
    
    while valid_refreshes < 10 and attempt < 30:
        attempt += 1
        found_ad_in_this_round = False
        current_round = valid_refreshes + 1
        ad_count_in_round = 0 
        
        try:
            await page.reload(wait_until="domcontentloaded", timeout=12000)
            await asyncio.sleep(2) 
            if page_type == "본문":
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(1)
        except: pass

        for frame in page.frames:
            try:
                ads = await frame.locator("a").all()
                for ad in ads:
                    href = await ad.get_attribute("href") or ""
                    img = ad.locator("img")
                    img_src = await img.first.evaluate("node => node.src") if await img.count() > 0 else ""
                    raw_pos = await ad.evaluate("node => { let p = node.closest('div'); return p ? p.className : 'unknown'; }")
                    text_content = await ad.inner_text() or ""
                    
                    # 🚫 1차: 구글/네트워크 및 닫기 버튼 차단
                    if "google" in href.lower() or "adsrvr.org" in href.lower() or "criteo" in href.lower(): continue
                    if "googleactiveview" in str(raw_pos).lower(): continue
                    if "dc/w/images" in img_src or "info_polic" in href or "close" in img_src.lower(): continue
                    if href == "#" or "javascript" in href.lower() or not href: continue
                        
                    # 🚫 2차 (핵심): 이용약관, 정책, 갤러리 안내 등 텍스트/URL 원천 차단
                    if "nstatic.dcinside.com/dc/" in href or "policy" in href or "useinfo" in href or "dcad" in href: continue
                    if any(word in text_content for word in ["이용안내", "이용약관", "개인정보", "청소년보호", "광고안내"]): continue

                    is_ad = any(k in href or k in (img_src or "") for k in ["addc.dcinside", "NetInsight", "nstatic.dcinside", "toast.com"])
                    
                    if is_ad:
                        found_ad_in_this_round = True
                        key = img_src if img_src else href
                        
                        if key not in seen_keys:
                            seen_keys.add(key)
                            ad_count_in_round += 1
                            final_url = await get_final_landing_url(context, href)
                            text_val = (await img.first.get_attribute("alt") if img_src else text_content) or "이미지 배너"
                            korean_pos = get_korean_position(env, page_type, raw_pos, img_src)
                            
                            print(f"✅ {prefix} [{current_round}회차 새로고침 - {ad_count_in_round}번째 발견] {korean_pos}")
                            
                            collected.append({
                                "date": today_str, "gallery": gallery_name, "env": env,
                                "pos": korean_pos, "url": final_url, "img": img_src, "text": text_val.strip()
                            })
            except: continue
        
        if found_ad_in_this_round:
            valid_refreshes += 1
            
    return collected

async def run_scraper_task(sem, context, env, target):
    async with sem:
        await asyncio.sleep(random.uniform(0, 1.5)) 
        page = await context.new_page()
        await page.route("**/*", block_unnecessary_resources)
        data = []
        try:
            url = target['pc'] if env == "PC" else target['mo']
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            data.extend(await capture_all_visible_ads(context, page, env, target['name'], "리스트"))
            
            post = page.locator("tr.us-post:not(.notice) td.gall_tit > a:not(.reply_numbox)").first if env == "PC" else page.locator("ul.gall-detail-lst li:not(.notice) .gall-detail-lnktit a").first
            if await post.count() > 0:
                await post.click()
                await asyncio.sleep(1.5)
                data.extend(await capture_all_visible_ads(context, page, env, target['name'], "본문"))
        except: pass
        finally: await page.close()
        return data

async def main():
    print("==================================================")
    print("🚀 디시인사이드 광고 초고속 병렬 수집 (쓰레기 배너 차단 완료)")
    print("==================================================")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        pc_context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=ua)
        mo_context = await browser.new_context(**p.devices['iPhone 13']) 

        sem = asyncio.Semaphore(8)

        tasks = []
        for target in TARGET_GALLERIES:
            tasks.append(run_scraper_task(sem, pc_context, "PC", target))
            tasks.append(run_scraper_task(sem, mo_context, "MO", target))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()
        
        all_final_data = [item for sublist in results if isinstance(sublist, list) for item in sublist]

    if all_final_data:
        print(f"\n📊 구글 시트 정리 및 분할 업로드를 준비합니다... (총 {len(all_final_data)}건)")
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        ws = gc.open_by_url(SHEET_URL).get_worksheet(0)
        
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
        
        chunk_size = 30
        total_chunks = (len(new_sheet_data) // chunk_size) + 1
        
        print(f"📦 데이터를 {total_chunks}개의 덩어리로 나누어 안전하게 업로드합니다.")
        for i in range(0, len(new_sheet_data), chunk_size):
            chunk = new_sheet_data[i : i + chunk_size]
            ws.append_rows(chunk)
            print(f"   ▶️ {min(i + chunk_size, len(new_sheet_data))} / {len(new_sheet_data)} 건 업로드 완료...")
            time.sleep(1.5) 
            
        print("\n🎉 모든 분할 업로드 및 초고속 수집이 완벽하게 끝났습니다!")
    else:
        print("\n❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
