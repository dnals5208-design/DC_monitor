import asyncio
import random
import time
import os
from playwright.async_api import async_playwright
import gspread
from datetime import datetime, timedelta, timezone

SERVICE_ACCOUNT_FILE = 'service_account2020.json' 
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

# 🔥 100% 완벽한 정답 주소 리스트 유지
ALL_GALLERIES = [
    # 🏢 [정규 갤러리]
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "7급공무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=7th", "mo": "https://m.dcinside.com/board/7th"},
    {"name": "고시시험갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=exam_gosi", "mo": "https://m.dcinside.com/board/exam_gosi"},
    {"name": "공무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=government", "mo": "https://m.dcinside.com/board/government"},
    {"name": "대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=pgschool", "mo": "https://m.dcinside.com/board/pgschool"},
    {"name": "법학전문대학원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=lawschool", "mo": "https://m.dcinside.com/board/lawschool"},
    {"name": "세무사갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=cta", "mo": "https://m.dcinside.com/board/cta"},
    {"name": "소방갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=firefighter", "mo": "https://m.dcinside.com/board/firefighter"},
    {"name": "순경갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=policeofficer", "mo": "https://m.dcinside.com/board/policeofficer"},
    {"name": "어학연수갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=lsa", "mo": "https://m.dcinside.com/board/lsa"},
    {"name": "영어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=English", "mo": "https://m.dcinside.com/board/English"},
    {"name": "영어회화갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=ec", "mo": "https://m.dcinside.com/board/ec"},
    {"name": "일어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=japanese", "mo": "https://m.dcinside.com/board/japanese"},
    {"name": "임용갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=tce", "mo": "https://m.dcinside.com/board/tce"},
    {"name": "자격증갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=coq", "mo": "https://m.dcinside.com/board/coq"},
    {"name": "중국어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=chinese", "mo": "https://m.dcinside.com/board/chinese"},
    {"name": "토익갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toeic", "mo": "https://m.dcinside.com/board/toeic"},
    {"name": "토플갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toefl", "mo": "https://m.dcinside.com/board/toefl"},
    {"name": "편입갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=admission", "mo": "https://m.dcinside.com/board/admission"},
    {"name": "학점은행제갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=acbs", "mo": "https://m.dcinside.com/board/acbs"},
    {"name": "해양경찰갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=korea_coast_guard", "mo": "https://m.dcinside.com/board/korea_coast_guard"},
    {"name": "회계사갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=cpa", "mo": "https://m.dcinside.com/board/cpa"},

    # ⛺ [마이너 갤러리]
    {"name": "HSK갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=hsk123456", "mo": "https://m.dcinside.com/board/hsk123456"},
    {"name": "JLPT갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=jlpt", "mo": "https://m.dcinside.com/board/jlpt"},
    {"name": "공인중개사갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=bokdukbang", "mo": "https://m.dcinside.com/board/bokdukbang"},
    {"name": "군무원갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=soider", "mo": "https://m.dcinside.com/board/soider"},
    {"name": "듀오링고갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=duolingo", "mo": "https://m.dcinside.com/board/duolingo"},
    {"name": "러시아갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=russia", "mo": "https://m.dcinside.com/board/russia"},
    {"name": "마이스터고갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=meister", "mo": "https://m.dcinside.com/board/meister"},
    {"name": "오픽갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=opic", "mo": "https://m.dcinside.com/board/opic"},
    {"name": "유학시험갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=ue", "mo": "https://m.dcinside.com/board/ue"},
    {"name": "전산세무회계갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=fat", "mo": "https://m.dcinside.com/board/fat"},
    {"name": "정병권갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=jeongbyeongkwon", "mo": "https://m.dcinside.com/board/jeongbyeongkwon"},
    {"name": "지텔프갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=gtelp", "mo": "https://m.dcinside.com/board/gtelp"},
    {"name": "컴퓨터활용능력갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=cbt", "mo": "https://m.dcinside.com/board/cbt"},
    {"name": "텝스갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=tepstopia", "mo": "https://m.dcinside.com/board/tepstopia"},
    {"name": "토익스피킹갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=toeicspeaking", "mo": "https://m.dcinside.com/board/toeicspeaking"}
]

CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", 0))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", 1))

base_size = len(ALL_GALLERIES) // TOTAL_CHUNKS 
remainder = len(ALL_GALLERIES) % TOTAL_CHUNKS   

if CHUNK_INDEX < remainder:
    start_idx = CHUNK_INDEX * (base_size + 1)
    end_idx = start_idx + (base_size + 1)
else:
    start_idx = remainder * (base_size + 1) + (CHUNK_INDEX - remainder) * base_size
    end_idx = start_idx + base_size

TARGET_GALLERIES = ALL_GALLERIES[start_idx:end_idx]

def safe_batch_upload(ws, data_chunk):
    if not data_chunk: return
    rows = [[d['date'], d['gallery'], d['env'], d['pos'], d['url'], d['img'], d['text']] for d in data_chunk]
    for i in range(0, len(rows), 30):
        try:
            ws.append_rows(rows[i:i+30])
            time.sleep(1.5)
        except Exception: time.sleep(5)

async def uploader_worker(queue, ws):
    buffer = []
    while True:
        item = await queue.get()
        if item is None: break
        buffer.append(item)
        if len(buffer) >= 100:
            print(f"🚀 [서버 {CHUNK_INDEX+1}] 100개 도달! 중간 업로드 중...")
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
    if route.request.resource_type in ["font", "media"]: await route.abort()
    else: await route.continue_()

async def capture_ads(context, page, env, gallery, page_type):
    collected, seen = [], set()
    
    # 🔥 [핵심 패치] 한국 시간(KST) 강제 적용!
    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST).strftime("%Y-%m-%d")
    
    valid_refreshes, attempt = 0, 0
    prefix = f"[서버 {CHUNK_INDEX+1}|{env}|{gallery[:4]}|{page_type}]"
    
    while valid_refreshes < 4 and attempt < 6:
        attempt += 1; found_ad_in_this_round = False
        current_round = valid_refreshes + 1
        ad_count_in_round = 0
        try:
            await page.reload(wait_until="load", timeout=12000)
            await asyncio.sleep(2)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3);")
            await asyncio.sleep(0.5)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 1.5);")
            await asyncio.sleep(0.5)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(1)
        except: pass

        for frame in page.frames:
            try:
                for ad in await frame.locator("a").all():
                    raw_href = await ad.evaluate("""n => {
                        let oc = n.getAttribute('onclick');
                        if (oc) {
                            let m = oc.match(/['"](http[^'"]+)['"]/);
                            if (m) return m[1];
                        }
                        return n.href || '';
                    }""")
                    
                    clean_href = raw_href.strip().lower()
                    
                    img_src = await ad.evaluate("""n => {
                        let img = n.querySelector('img');
                        if (img) {
                            if (img.src && !img.src.includes('data:image')) return img.src;
                            if (img.getAttribute('data-src')) return img.getAttribute('data-src');
                        }
                        let bg = window.getComputedStyle(n).backgroundImage;
                        if (bg && bg !== 'none' && bg.includes('url')) {
                            return bg.replace(/^url\\(['"]?/, '').replace(/['"]?\\)$/, '');
                        }
                        return '';
                    }""")
                    
                    raw_pos = await ad.evaluate("n => { let p = n.closest('div'); return p ? p.className : ''; }")
                    txt = await ad.inner_text() or ""
                    
                    clean_img = img_src.strip().lower()
                    clean_txt = txt.strip()
                    
                    if not clean_href and not clean_img and not clean_txt: continue
                    
                    if clean_txt.lower() == "null": clean_txt = ""
                    if "dcinside.com" in clean_txt.lower(): clean_txt = ""
                    junk_texts = ["갤러리", "실시간 베스트", "광고안내", "이용안내", "개인정보", "운영자"]
                    if any(j in clean_txt for j in junk_texts) or clean_txt == "갤러리":
                        continue
                        
                    junk_images = [
                        "noimage", "tit_", "sp_", "logo", "g_img", "blank", "/images/", "/dcad/",
                        "traffic_", "150106_traffic", "default_banner", "icon"
                    ]
                    if any(j in clean_img for j in junk_images):
                        continue
                        
                    external_ad_networks = ["google", "adsrvr", "criteo", "taboola", "doubleclick", "adnxs", "smartadserver"]
                    if any(k in clean_href for k in external_ad_networks): 
                        continue
                    
                    internal_urls = [
                        "https://www.dcinside.com", "https://gall.dcinside.com", "https://m.dcinside.com", 
                        "https://gall.dcinside.com/m", "https://gall.dcinside.com/mini",
                        "https://www.dcinside.com/", "https://gall.dcinside.com/", "https://m.dcinside.com/"
                    ]
                    if clean_href in internal_urls:
                        continue
                    if "board/dcbest" in clean_href or "board/lists" in clean_href:
                        continue

                    is_real_ad = False
                    if "addc.dc" in clean_href or "netinsight" in clean_href or "toast" in clean_href:
                        is_real_ad = True
                    elif "/ad/" in clean_img and "traffic_" not in clean_img and "/dcad/" not in clean_img:
                        is_real_ad = True
                        
                    if not is_real_ad: 
                        continue

                    found_ad_in_this_round = True
                    key = clean_img or raw_href
                    if key not in seen:
                        seen.add(key)
                        ad_count_in_round += 1
                        
                        final_url = ""
                        if not raw_href.startswith("javascript") and raw_href != "#" and raw_href != "__click__":
                            final_url = await get_final_landing_url(context, raw_href)
                        else:
                            final_url = raw_href
                            
                        clean_final = final_url.strip()
                        
                        if clean_final.rstrip('/').lower() in internal_urls:
                            continue 
                            
                        clean_final = clean_final.replace("__CLICK__", "").replace("__click__", "")
                        if not clean_final or clean_final.lower() in ["null", "#", "http://null", "https://null", "__click__"]:
                            clean_final = "랜딩 URL 없음"
                        
                        pos = get_korean_position(env, page_type, raw_pos, clean_img)
                        text_val = clean_txt if clean_txt else "이미지 배너"
                        
                        print(f"✅ {prefix} [{current_round}회차 새로고침 - {ad_count_in_round}번째 발견] {pos}")
                        collected.append({"date": today, "gallery": gallery, "env": env, "pos": pos, "url": clean_final, "img": img_src.strip(), "text": text_val})
            except: continue
        if found_ad_in_this_round: valid_refreshes += 1
    return collected

async def task_runner(sem, ctx, env, tgt, queue):
    async with sem:
        await asyncio.sleep(random.uniform(0, 1.5))
        page = await ctx.new_page()
        
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        
        await page.route("**/*", block_resources)
        try:
            target_url = tgt['pc'] if env=="PC" else tgt['mo']
            
            gallery_id = ""
            if "id=" in target_url:
                gallery_id = target_url.split("id=")[-1].split("&")[0]
            else:
                gallery_id = target_url.split("/")[-1]

            await page.goto(target_url, wait_until="load", timeout=15000)
            await asyncio.sleep(1.5)
            
            page_title = await page.title()
            current_url = page.url.lower()
            keyword = tgt['name'].replace("갤러리", "").strip()
            
            bounce_urls = ["https://www.dcinside.com", "https://gall.dcinside.com", "https://m.dcinside.com", "https://gall.dcinside.com/m", "https://gall.dcinside.com/mini"]
            
            if keyword not in page_title.replace(" ", "") or current_url in bounce_urls:
                if env == "PC":
                    print(f"⚠️ [서버 {CHUNK_INDEX+1}|{tgt['name']}] 잘못된 주소 감지. 스피드 우회 탐색 시작...")
                    test_urls = [
                        f"https://gall.dcinside.com/board/lists/?id={gallery_id}",
                        f"https://gall.dcinside.com/mgallery/board/lists/?id={gallery_id}",
                        f"https://gall.dcinside.com/mini/board/lists/?id={gallery_id}"
                    ]
                    for t_url in test_urls:
                        await page.goto(t_url, wait_until="load", timeout=12000)
                        await asyncio.sleep(1)
                        temp_title = await page.title()
                        if keyword in temp_title.replace(" ", ""):
                            print(f"✅ [서버 {CHUNK_INDEX+1}|{tgt['name']}] 올바른 주소 안착 완료!")
                            break
                elif env == "MO":
                    print(f"⚠️ [서버 {CHUNK_INDEX+1}|{tgt['name']}] 잘못된 주소 감지. 스피드 우회 탐색 시작...")
                    test_urls = [
                        f"https://m.dcinside.com/board/{gallery_id}",
                        f"https://m.dcinside.com/mini/{gallery_id}"
                    ]
                    for t_url in test_urls:
                        await page.goto(t_url, wait_until="load", timeout=12000)
                        await asyncio.sleep(1)
                        temp_title = await page.title()
                        if keyword in temp_title.replace(" ", ""):
                            print(f"✅ [서버 {CHUNK_INDEX+1}|{tgt['name']}] 올바른 주소 안착 완료!")
                            break

            for item in await capture_ads(ctx, page, env, tgt['name'], "리스트"): await queue.put(item)
            
            post = page.locator("tr.us-post:not(.notice) td.gall_tit > a:not(.reply_numbox)").first if env=="PC" else page.locator("ul.gall-detail-lst li:not(.notice) .gall-detail-lnktit a").first
            if await post.count() > 0:
                await post.click()
                await asyncio.sleep(2.5)
                for item in await capture_ads(ctx, page, env, tgt['name'], "본문"): await queue.put(item)
        except: pass
        finally: await page.close()

async def main():
    if not TARGET_GALLERIES: return
    
    gallery_names = [g['name'] for g in TARGET_GALLERIES]
    print(f"🔥 [서버 {CHUNK_INDEX+1}] 가동! 할당된 갤러리 {len(TARGET_GALLERIES)}개: {', '.join(gallery_names)}")
    
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    ws = gc.open_by_url(SHEET_URL).get_worksheet(0)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-web-security"])
        pc_ctx, mo_ctx = await browser.new_context(viewport={"width": 1920, "height": 1080}), await browser.new_context(**p.devices['iPhone 13'])
        
        sem, queue = asyncio.Semaphore(5), asyncio.Queue()
        uploader = asyncio.create_task(uploader_worker(queue, ws))

        tasks = [task_runner(sem, pc_ctx, "PC", t, queue) for t in TARGET_GALLERIES] + [task_runner(sem, mo_ctx, "MO", t, queue) for t in TARGET_GALLERIES]
        await asyncio.gather(*tasks)
        await browser.close()
        
        await queue.put(None)
        await uploader
        print(f"🎉 [서버 {CHUNK_INDEX+1}] 담당 구역 완벽하게 수집 종료!")

if __name__ == "__main__": asyncio.run(main())
