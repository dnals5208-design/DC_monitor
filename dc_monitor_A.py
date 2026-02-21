import asyncio
import random
import time
from playwright.async_api import async_playwright
import gspread
from datetime import datetime

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

TARGET_GALLERIES = [
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "7급공무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=7th", "mo": "https://m.dcinside.com/board/7th"},
    {"name": "HSK갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=hsk123456", "mo": "https://m.dcinside.com/board/hsk123456"},
    {"name": "JLPT갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=jlpt", "mo": "https://m.dcinside.com/board/jlpt"},
    {"name": "고시시험갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=exam_new", "mo": "https://m.dcinside.com/board/exam_new"},
    {"name": "공무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=government", "mo": "https://m.dcinside.com/board/government"},
    {"name": "공인중개사갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=lreaexam", "mo": "https://m.dcinside.com/board/lreaexam"},
    {"name": "군무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=soider", "mo": "https://m.dcinside.com/board/soider"},
    {"name": "대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=univ_new", "mo": "https://m.dcinside.com/board/univ_new"},
    {"name": "듀오링고갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=duolingo", "mo": "https://m.dcinside.com/board/duolingo"},
    {"name": "러시아어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=russiangall", "mo": "https://m.dcinside.com/board/russiangall"},
    {"name": "마이스터고갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=meister", "mo": "https://m.dcinside.com/board/meister"},
    {"name": "법학전문대학원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=lawschool", "mo": "https://m.dcinside.com/board/lawschool"},
    {"name": "세무사갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=cta", "mo": "https://m.dcinside.com/board/cta"},
    {"name": "소방갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=fire", "mo": "https://m.dcinside.com/board/fire"},
    {"name": "순경갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=policeofficer", "mo": "https://m.dcinside.com/board/policeofficer"},
    {"name": "어학연수갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=language", "mo": "https://m.dcinside.com/board/language"},
    {"name": "영어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=English", "mo": "https://m.dcinside.com/board/English"},
    {"name": "영어회화갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=englishspeech", "mo": "https://m.dcinside.com/board/englishspeech"}
]

def safe_batch_upload(ws, data_chunk):
    if not data_chunk: return
    rows = [[d['date'], d['gallery'], d['env'], d['pos'], d['url'], d['img'], d['text']] for d in data_chunk]
    for i in range(0, len(rows), 30):
        try:
            ws.append_rows(rows[i:i+30])
            time.sleep(1.5)
        except Exception as e:
            time.sleep(5)

async def uploader_worker(queue, ws):
    buffer = []
    while True:
        item = await queue.get()
        if item is None: break
        buffer.append(item)
        if len(buffer) >= 250:
            print(f"🚀 [A팀] 250개 도달! 구글 시트 분할 업로드 중...")
            await asyncio.to_thread(safe_batch_upload, ws, buffer)
            buffer.clear()
        queue.task_done()
    if buffer:
        await asyncio.to_thread(safe_batch_upload, ws, buffer)

def get_korean_position(env, page_type, raw_pos, img_src):
    raw = str(raw_pos).lower()
    if not img_src: return "텍스트배너"
    if "icon" in raw or "float" in raw or "pop-layer" in raw: return "아이콘배너"
    if env == "PC":
        if page_type == "본문": return "하단배너" if "bottom" in raw or "btm" in raw else "게시글배너"
        else:
            if "right" in raw or "wing" in raw: return "우측배너"
            if "left" in raw: return "좌측배너"
            return "하단배너" if "bottom" in raw or "btm" in raw else "상단배너"
    else: 
        if page_type == "본문": return "하단배너" if "bottom" in raw or "btm" in raw else "게시글배너"
        else: return "하단배너" if "bottom" in raw or "btm" in raw else "상단배너"

async def get_final_landing_url(context, redirect_url):
    if not redirect_url or "addc.dcinside" not in redirect_url: return redirect_url
    try:
        temp = await context.new_page()
        await temp.goto(redirect_url, wait_until="commit", timeout=4000)
        url = temp.url
        await temp.close()
        return url
    except: return redirect_url

async def block_resources(route):
    # 폰트, 미디어만 차단하고 이미지는 허용하여 광고가 정상 로출되도록 롤백
    if route.request.resource_type in ["font", "media"]:
        await route.abort()
    else: 
        await route.continue_()

async def capture_ads(context, page, env, gallery, page_type):
    collected, seen = [], set()
    today = datetime.now().strftime("%Y-%m-%d")
    valid_refreshes = 0
    attempt = 0
    prefix = f"[A팀|{env}|{gallery[:4]}|{page_type}]"
    
    while valid_refreshes < 10 and attempt < 30:
        attempt += 1
        found_ad_in_this_round = False
        current_round = valid_refreshes + 1
        ad_count_in_round = 0
        
        try:
            # 정상적인 광고 로딩을 위해 domcontentloaded로 롤백
            await page.reload(wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            if page_type == "본문": await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        except: pass

        for frame in page.frames:
            try:
                for ad in await frame.locator("a").all():
                    href = await ad.get_attribute("href") or ""
                    img = ad.locator("img")
                    img_src = await img.first.evaluate("n => n.src") if await img.count() > 0 else ""
                    raw_pos = await ad.evaluate("n => { let p = n.closest('div'); return p ? p.className : ''; }")
                    txt = await ad.inner_text() or ""
                    
                    if any(k in href.lower() for k in ["google", "adsrvr", "criteo", "policy", "useinfo", "#", "javascript"]): continue
                    if "close" in img_src.lower() or "googleactiveview" in raw_pos.lower(): continue
                    if any(w in txt for w in ["이용안내", "이용약관", "개인정보", "광고안내"]): continue

                    if any(k in href or k in img_src for k in ["addc.dc", "NetInsight", "nstatic", "toast"]):
                        found_ad_in_this_round = True
                        key = img_src or href
                        if key not in seen:
                            seen.add(key)
                            ad_count_in_round += 1
                            final_url = await get_final_landing_url(context, href)
                            pos = get_korean_position(env, page_type, raw_pos, img_src)
                            
                            # 상세 로그 복구!
                            print(f"✅ {prefix} [{current_round}회차 새로고침 - {ad_count_in_round}번째 발견] {pos}")
                            collected.append({"date": today, "gallery": gallery, "env": env, "pos": pos, "url": final_url, "img": img_src, "text": txt.strip()})
            except: continue
            
        if found_ad_in_this_round: 
            valid_refreshes += 1
            
    return collected

async def task_runner(sem, ctx, env, tgt, queue):
    async with sem:
        await asyncio.sleep(random.uniform(0, 1.5))
        page = await ctx.new_page()
        await page.route("**/*", block_resources)
        try:
            await page.goto(tgt['pc'] if env=="PC" else tgt['mo'], wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(1.5)
            for item in await capture_ads(ctx, page, env, tgt['name'], "리스트"): await queue.put(item)
            
            post = page.locator("tr.us-post:not(.notice) td.gall_tit > a:not(.reply_numbox)").first if env=="PC" else page.locator("ul.gall-detail-lst li:not(.notice) .gall-detail-lnktit a").first
            if await post.count() > 0:
                await post.click()
                await asyncio.sleep(2)
                for item in await capture_ads(ctx, page, env, tgt['name'], "본문"): await queue.put(item)
        except: pass
        finally: await page.close()

async def main():
    print("🧹 [A팀] 구글 시트 사전 초기화 (오늘자 중복 데이터 방지)...")
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
            
    print("🚀 [A팀] 1~19번 갤러리 정밀 수집 시작!")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
        pc_ctx, mo_ctx = await browser.new_context(viewport={"width": 1920, "height": 1080}), await browser.new_context(**p.devices['iPhone 13'])
        sem, queue = asyncio.Semaphore(6), asyncio.Queue()
        uploader = asyncio.create_task(uploader_worker(queue, ws))

        tasks = [task_runner(sem, pc_ctx, "PC", t, queue) for t in TARGET_GALLERIES] + [task_runner(sem, mo_ctx, "MO", t, queue) for t in TARGET_GALLERIES]
        await asyncio.gather(*tasks)
        await browser.close()
        await queue.put(None)
        await uploader
        print("🎉 [A팀] 작업 완료!")

if __name__ == "__main__": asyncio.run(main())
