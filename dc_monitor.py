import asyncio
import random
import time
from playwright.async_api import async_playwright
import gspread
from datetime import datetime

# --- ⚙️ 설정 ---
SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

# 📝 확장된 37개 갤러리 리스트
TARGET_GALLERIES = [
[
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "7급공채갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=neo7gall", "mo": "https://m.dcinside.com/board/neo7gall"},
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
    {"name": "영어회화갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=englishspeech", "mo": "https://m.dcinside.com/board/englishspeech"},
    {"name": "오픽갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=opic", "mo": "https://m.dcinside.com/board/opic"},
    {"name": "유학시험갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=eju", "mo": "https://m.dcinside.com/board/eju"},
    {"name": "일어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=japanese", "mo": "https://m.dcinside.com/board/japanese"},
    {"name": "임용고시갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=imyoung", "mo": "https://m.dcinside.com/board/imyoung"},
    {"name": "자격증갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=coq", "mo": "https://m.dcinside.com/board/coq"},
    {"name": "전산세무회계갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=accounting", "mo": "https://m.dcinside.com/board/accounting"},
    {"name": "정병권갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=jeongbk", "mo": "https://m.dcinside.com/board/jeongbk"},
    {"name": "중국어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=chinese", "mo": "https://m.dcinside.com/board/chinese"},
    {"name": "지텔프갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=gtelf", "mo": "https://m.dcinside.com/board/gtelf"},
    {"name": "컴퓨터활용능력갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=itlicense", "mo": "https://m.dcinside.com/board/itlicense"},
    {"name": "텝스갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=teps", "mo": "https://m.dcinside.com/board/teps"},
    {"name": "토익갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toeic", "mo": "https://m.dcinside.com/board/toeic"},
    {"name": "토익스피킹갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toespic", "mo": "https://m.dcinside.com/board/toespic"},
    {"name": "토플갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toefl", "mo": "https://m.dcinside.com/board/toefl"},
    {"name": "편입갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=admission", "mo": "https://m.dcinside.com/board/admission"},
    {"name": "학점은행제갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=acbs", "mo": "https://m.dcinside.com/board/acbs"},
    {"name": "해양경찰갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=kcg", "mo": "https://m.dcinside.com/board/kcg"},
    {"name": "회계사갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=cpa", "mo": "https://m.dcinside.com/board/cpa"}
]

# 🎯 구글 시트 안전 업로드 함수 (동기 처리용)
def safe_batch_upload(ws, data_chunk):
    if not data_chunk: return
    rows_to_append = [[d['date'], d['gallery'], d['env'], d['pos'], d['url'], d['img'], d['text']] for d in data_chunk]
    
    # 데이터를 30개씩 쪼개서 업로드 (API 보호)
    for i in range(0, len(rows_to_append), 30):
        sub_chunk = rows_to_append[i : i + 30]
        try:
            ws.append_rows(sub_chunk)
            time.sleep(1.5) # 꿀맛 휴식 (API 에러 원천 차단)
        except Exception as e:
            print(f"\n⚠️ 구글 시트 업로드 중 에러 발생: {e}")
            time.sleep(5) # 에러 시 5초 대기 후 다음 작업 진행

# 🎯 250개 도달 시 업로드를 담당하는 백그라운드 워커 (Consumer)
async def uploader_worker(queue, ws):
    buffer = []
    total_uploaded = 0
    print("\n📦 [시스템] 백그라운드 업로드 매니저 가동 완료 (250개 단위 대기 중...)")
    
    while True:
        item = await queue.get()
        if item is None: # None이 들어오면 모든 탐색이 끝났다는 신호
            break
            
        buffer.append(item)
        
        # 바구니에 250개가 차면 즉시 업로드 실시!
        if len(buffer) >= 250:
            print(f"\n🚀 [시스템] 데이터 250개 도달! 구글 시트 중간 업로드를 시작합니다 (30개씩 분할)...")
            await asyncio.to_thread(safe_batch_upload, ws, buffer)
            total_uploaded += len(buffer)
            print(f"✅ [시스템] 중간 업로드 완료. (누적 업로드: {total_uploaded}건)")
            buffer.clear()
            
        queue.task_done()
        
    # 탐색 종료 후 바구니에 남은 잔여 데이터(250개 미만) 최종 업로드
    if buffer:
        print(f"\n🚀 [시스템] 탐색 완전 종료. 남은 자투리 데이터 {len(buffer)}건을 최종 업로드합니다...")
        await asyncio.to_thread(safe_batch_upload, ws, buffer)
        total_uploaded += len(buffer)
        
    print(f"\n🎉 [시스템] 모든 구글 시트 업로드 작업이 안전하게 종료되었습니다. (총 {total_uploaded}건)")

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
                    
                    if "google" in href.lower() or "adsrvr.org" in href.lower() or "criteo" in href.lower(): continue
                    if "googleactiveview" in str(raw_pos).lower(): continue
                    if "dc/w/images" in img_src or "info_polic" in href or "close" in img_src.lower(): continue
                    if href == "#" or "javascript" in href.lower() or not href: continue
                        
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

# ⚡ 데이터를 수집하자마자 큐(Queue) 바구니에 집어넣는 역할
async def run_scraper_task(sem, context, env, target, data_queue):
    async with sem:
        await asyncio.sleep(random.uniform(0, 2.0)) 
        page = await context.new_page()
        await page.route("**/*", block_unnecessary_resources)
        
        try:
            url = target['pc'] if env == "PC" else target['mo']
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            
            # 리스트 페이지 데이터 수집 및 큐에 전송
            list_data = await capture_all_visible_ads(context, page, env, target['name'], "리스트")
            for item in list_data:
                await data_queue.put(item)
            
            # 본문 페이지 이동
            post = page.locator("tr.us-post:not(.notice) td.gall_tit > a:not(.reply_numbox)").first if env == "PC" else page.locator("ul.gall-detail-lst li:not(.notice) .gall-detail-lnktit a").first
            if await post.count() > 0:
                await post.click()
                await asyncio.sleep(1.5)
                
                # 본문 페이지 데이터 수집 및 큐에 전송
                body_data = await capture_all_visible_ads(context, page, env, target['name'], "본문")
                for item in body_data:
                    await data_queue.put(item)
        except: pass
        finally: await page.close()

async def main():
    print("==================================================")
    print("🚀 [대규모 37개 갤러리] 초고속 병렬 수집 & 실시간 분할 업로드 가동")
    print("==================================================")
    
    # 🧹 1단계: 시트 사전 청소 (과거 데이터 보존, 오늘 데이터만 초기화)
    print("\n🧹 구글 시트 사전 초기화 중 (오늘 중복 데이터 방지)...")
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    ws = gc.open_by_url(SHEET_URL).get_worksheet(0)
    
    existing_rows = ws.get_all_values()
    today_str = datetime.now().strftime("%Y-%m-%d")
    headers = ["날짜", "갤러리명", "환경", "위치", "URL", "이미지", "텍스트문구"]
    kept_rows = [headers]
    
    if existing_rows:
        for row in existing_rows[1:]:
            if len(row) > 0 and row[0] != today_str: 
                kept_rows.append(row)
                
    ws.clear()
    
    # 남겨진 과거 데이터가 있다면 100개씩 쪼개서 복구 (API 보호)
    if len(kept_rows) > 1:
        print("   ▶️ 과거 히스토리 데이터를 안전하게 복구합니다.")
        for i in range(0, len(kept_rows), 100):
            ws.append_rows(kept_rows[i:i+100])
            time.sleep(1.5)

    # 🚀 2단계: 수집 및 실시간 업로드 시작
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--disable-dev-shm-usage", "--no-sandbox"]
        )
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        pc_context = await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=ua)
        mo_context = await browser.new_context(**p.devices['iPhone 13']) 

        sem = asyncio.Semaphore(8)
        data_queue = asyncio.Queue() # 실시간 데이터 바구니 생성

        # 백그라운드 업로더 실행 (봇들이 일하는 동안 뒤에서 대기)
        uploader_task = asyncio.create_task(uploader_worker(data_queue, ws))

        # 37개 갤러리 (총 74개 작업) 병렬 출발
        tasks = []
        for target in TARGET_GALLERIES:
            tasks.append(run_scraper_task(sem, pc_context, "PC", target, data_queue))
            tasks.append(run_scraper_task(sem, mo_context, "MO", target, data_queue))

        # 모든 봇들의 탐색이 끝날 때까지 대기
        await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()
        
        # 봇 탐색 종료 ➡️ 업로더에게 종료 신호(None) 전송
        await data_queue.put(None)
        
        # 업로더가 남은 자투리 데이터를 모두 시트에 올릴 때까지 대기
        await uploader_task

if __name__ == "__main__":
    asyncio.run(main())
