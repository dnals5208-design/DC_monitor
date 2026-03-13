import asyncio
import random
import time
import os
import re
from playwright.async_api import async_playwright
import gspread
from datetime import datetime, timedelta, timezone

# --- ⚙️ 설정 ---
SERVICE_ACCOUNT_FILE = 'service_account2020.json'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1omDVgsy4qwCKZMbuDLoKvJjNsOU1uqkfBqZIM7euezk/edit?gid=0#gid=0'

ALL_GALLERIES = [
    {"name": "자격증갤러리","pc":"https://gall.dcinside.com/board/lists/?id=coq","mo":"https://m.dcinside.com/board/coq"},
    {"name": "편입갤러리","pc":"https://gall.dcinside.com/board/lists/?id=admission","mo":"https://m.dcinside.com/board/admission"},
    {"name": "정병권갤러리","pc":"https://gall.dcinside.com/mgallery/board/lists/?id=jeongbyeongkwon","mo":"https://m.dcinside.com/board/jeongbyeongkwon"},
    {"name": "학점은행제갤러리","pc":"https://gall.dcinside.com/board/lists/?id=acbs","mo":"https://m.dcinside.com/board/acbs"},
    {"name": "4년제대학갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=4year_university", "mo": "https://m.dcinside.com/board/4year_university"},
    {"name": "법학전문대학원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=lawschool", "mo": "https://m.dcinside.com/board/lawschool"},
    {"name": "공무원갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=government_new1", "mo": "https://m.dcinside.com/board/government_new1"},
    {"name": "군무원갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=soider", "mo": "https://m.dcinside.com/board/soider"},
    {"name": "공인중개사갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=bokdukbang", "mo": "https://m.dcinside.com/board/bokdukbang"},
    {"name": "주택관리사갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=housingmanager", "mo": "https://m.dcinside.com/board/housingmanager"},
    {"name": "감정평가사갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=gampyeong", "mo": "https://m.dcinside.com/board/gampyeong"},
    {"name": "행정사갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=hangjungsa", "mo": "https://m.dcinside.com/board/hangjungsa"},
    {"name": "소방갤러리", "pc": "https://gall.dcinside.com/mgallery/board/lists/?id=firefighter", "mo": "https://m.dcinside.com/board/firefighter"},
    {"name": "순경갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=policeofficer", "mo": "https://m.dcinside.com/board/policeofficer"},
    {"name": "임용갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=tce", "mo": "https://m.dcinside.com/board/tce"},
    {"name": "토익갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=toeic", "mo": "https://m.dcinside.com/board/toeic"},
    {"name": "일어갤러리", "pc": "https://gall.dcinside.com/board/lists?id=japanese", "mo": "https://m.dcinside.com/board/japanese"},
    {"name": "영어갤러리", "pc": "https://gall.dcinside.com/board/lists?id=English", "mo": "https://m.dcinside.com/board/English"},
    {"name": "영어회화갤러리", "pc": "https://gall.dcinside.com/board/lists?id=ec", "mo": "https://m.dcinside.com/board/ec"},
    {"name": "중국어갤러리", "pc": "https://gall.dcinside.com/board/lists/?id=chinese", "mo": "https://m.dcinside.com/board/chinese"},
    {"name": "세무사갤러리","pc":"https://gall.dcinside.com/board/lists/?id=cta","mo":"https://m.dcinside.com/board/cta"},
    {"name": "회계사갤러리","pc":"https://gall.dcinside.com/board/lists/?id=cpa","mo":"https://m.dcinside.com/board/cpa"}
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


# --- 📊 구글 시트 안전 배치 업로드 ---
def safe_batch_upload(ws, data_chunk):
    if not data_chunk:
        return
    rows = [[d['date'], d['gallery'], d['env'], d['pos'], d['url'], d['img'], d['text']] for d in data_chunk]
    for i in range(0, len(rows), 30):
        try:
            ws.append_rows(rows[i:i+30])
            time.sleep(1.5)
        except Exception:
            time.sleep(5)


# --- 📤 비동기 업로더 워커 ---
async def uploader_worker(queue, ws):
    buffer = []
    while True:
        item = await queue.get()
        if item is None:
            break
        buffer.append(item)
        if len(buffer) >= 100:
            print(f"🚀 [서버 {CHUNK_INDEX+1}] 100개 도달! 중간 업로드 중...")
            await asyncio.to_thread(safe_batch_upload, ws, buffer)
            buffer.clear()
        queue.task_done()
    if buffer:
        await asyncio.to_thread(safe_batch_upload, ws, buffer)


# --- 📝 위치명 정밀 판독 (URL 파싱 + fallback) ---
def get_korean_position(env, page_type, raw_pos, is_image, raw_href, urls_text):
    target_url = raw_href.split('?')[0].lower()
    pos_result = ""

    if "click/dcinside" in target_url:
        try:
            parts = target_url.split('/')
            last_part = parts[-1]
            if '@' in last_part:
                page_str, pos_gallery = last_part.split('@', 1)
                pos_str = pos_gallery.split('_')[0]
                page_kr = "리스트" if page_str == "list" else "본문"
                if pos_str == "top": pos_kr = "상단배너"
                elif pos_str == "middle": pos_kr = "중단배너"
                elif pos_str in ["bottom", "reply"]: pos_kr = "하단배너"
                elif pos_str == "left": pos_kr = "좌측배너"
                elif pos_str == "right": pos_kr = "우측배너"
                elif "auto" in pos_str: pos_kr = "짤방배너"
                elif "icon" in pos_str or "float" in pos_str: pos_kr = "아이콘배너"
                else: pos_kr = "배너"
                pos_result = f"{page_kr} {pos_kr}"
        except:
            pass

    if not pos_result:
        raw = (str(raw_pos) + " " + str(urls_text)).lower()
        if not is_image: return "텍스트배너"
        if "icon" in raw or "float" in raw or "pop-layer" in raw: return "아이콘배너"

        page_kr = "리스트" if page_type == "리스트" else "본문"
        if env == "PC":
            if page_type == "본문":
                pos_result = f"{page_kr} 하단배너" if "bottom" in raw or "btm" in raw else f"{page_kr} 게시글배너"
            else:
                if "right" in raw or "wing" in raw: pos_result = f"{page_kr} 우측배너"
                elif "left" in raw: pos_result = f"{page_kr} 좌측배너"
                else: pos_result = f"{page_kr} 하단배너" if "bottom" in raw or "btm" in raw else f"{page_kr} 상단배너"
        else:
            pos_result = f"{page_kr} 하단배너" if "bottom" in raw or "btm" in raw else f"{page_kr} 게시글배너" if page_type == "본문" else f"{page_kr} 상단배너"

    # 영역명 강제 정상화
    if page_type == "리스트" and "본문" in pos_result:
        return "리스트 공지"

    return pos_result


# --- 🔗 최종 랜딩 URL 추적 ---
async def get_final_landing_url(context, redirect_url, referer_url):
    if not redirect_url or not redirect_url.startswith("http"): return redirect_url
    if "addc.dc" not in redirect_url and "netinsight" not in redirect_url: return redirect_url

    try:
        temp = await context.new_page()
        await temp.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font", "stylesheet"] else route.continue_())
        try: await temp.goto(redirect_url, referer=referer_url, wait_until="commit", timeout=5000)
        except: pass

        for _ in range(25):
            current_url = temp.url
            if "addc.dc" not in current_url and "netinsight" not in current_url and current_url != "about:blank": break
            await asyncio.sleep(0.2)

        final_url = temp.url
        await temp.close()
        return final_url
    except:
        return redirect_url


# --- 🚫 불필요 리소스 차단 ---
async def block_resources(route):
    if route.request.resource_type in ["font", "media"]: await route.abort()
    else: await route.continue_()


# --- ✅ MO 본문 짤방 iframe 로딩 완료 대기 ---
async def _wait_for_ad_frames(page, timeout_ms=7000):
    deadline = asyncio.get_event_loop().time() + timeout_ms / 1000
    ad_keywords = ["addc.dc", "netinsight", "toast"]
    while asyncio.get_event_loop().time() < deadline:
        try:
            frame_srcs = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll('iframe')).map(f => f.src || f.getAttribute('data-src') || '').filter(s => s.length > 0);
            }""")
            if any(kw in src for src in frame_srcs for kw in ad_keywords):
                await asyncio.sleep(0.8)
                return
        except: pass
        await asyncio.sleep(0.3)
    await asyncio.sleep(1.0)


# --- 🔍 광고 탐색 핵심 엔진 ---
async def capture_ads(context, page, env, gallery, page_type):
    collected = []
    seen = set()

    KST = timezone(timedelta(hours=9))
    today = datetime.now(KST).strftime("%Y-%m-%d")
    prefix = f"[{env}|{gallery[:4]}|{page_type}]"

    # 🔥 사용자 요청에 맞게 40회로 정확히 제한!
    max_valid = 40 
    max_total = 80

    valid_attempts = 0
    total_attempts = 0

    while valid_attempts < max_valid and total_attempts < max_total:
        
        # 🚨 비상 탈출 로직 (설문/유령 페이지 방어)
        if total_attempts == 15 and len(collected) == 0:
            print(f"🚨 {prefix} [비상 탈출] 15회 시도 동안 유효 광고 0개! (광고 없는 페이지로 간주하여 스킵)")
            break

        # 🔥 세션 초기화 (롤링 소재 강제 순환을 위해 10회마다 쿠키 삭제)
        if total_attempts > 0 and total_attempts % 10 == 0:
            try:
                await context.clear_cookies()
            except: pass

        total_attempts += 1
        found_dc_ad_in_this_round = False

        try:
            await page.evaluate("window.scrollTo(0, 0);")
            await page.reload(wait_until="load", timeout=12000)

            if page_type == "본문":
                if env == "MO":
                    await _wait_for_ad_frames(page, timeout_ms=7000)
                else:
                    await asyncio.sleep(2.0)

                await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.5);")
                await asyncio.sleep(0.5)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(0.5)
                await page.evaluate("window.scrollTo(0, 0);")
                await asyncio.sleep(0.3)
            else:
                await asyncio.sleep(1.5)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3);")
                await asyncio.sleep(0.5)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 1.5);")
                await asyncio.sleep(0.5)
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await asyncio.sleep(1.0)
        except:
            pass

        base_page_url = page.url.split('#')[0].split('?')[0].lower()

        for frame in page.frames:
            try:
                for ad in await frame.locator("a").all():

                    raw_href_attr = await ad.get_attribute("href") or ""
                    clean_href_attr = raw_href_attr.strip().lower()

                    if clean_href_attr == "#" or clean_href_attr.endswith("#"): continue
                    if "javascript:" in clean_href_attr and "window.open" not in clean_href_attr: continue
                    if "/board/lists" in clean_href_attr or "/mini/board/lists" in clean_href_attr: continue
                    if "dcad" in clean_href_attr: continue

                    raw_href = await ad.evaluate("""n => {
                        if (n.href && !n.href.includes('__CLICK__') && !n.href.includes('__click__') && !n.href.includes('null')) return n.href;
                        let oc = n.getAttribute('onclick');
                        if (oc) { let m = oc.match(/['"](http[^'"]+)['"]/); if (m) return m[1]; }
                        return n.href || '';
                    }""")
                    clean_href = raw_href.strip().lower()

                    if clean_href.endswith("#") or "/board/lists" in clean_href or "dcad" in clean_href: continue

                    img_src = await ad.evaluate("""n => {
                        let getValidSrc = (el) => {
                            let w = el.getAttribute('width'); let h = el.getAttribute('height');
                            if (w && parseInt(w) <= 10) return null; if (h && parseInt(h) <= 10) return null;
                            let src = el.getAttribute('data-src') || el.getAttribute('data-original') || el.src;
                            if (src && !src.includes('data:image')) return src; return null;
                        };
                        let img = n.querySelector('img');
                        if (img) { let valid = getValidSrc(img); if (valid) return valid; }
                        let bg = window.getComputedStyle(n).backgroundImage;
                        if (bg && bg !== 'none' && bg.includes('url')) { let url = bg.replace(/^url\\(['"]?/, '').replace(/['"]?\\)$/, ''); if (!url.includes('data:image')) return url; }
                        let child = n.querySelector('div, span');
                        if (child) { let cbg = window.getComputedStyle(child).backgroundImage; if (cbg && cbg !== 'none' && cbg.includes('url')) { let url = cbg.replace(/^url\\(['"]?/, '').replace(/['"]?\\)$/, ''); if (!url.includes('data:image')) return url; } }
                        return '';
                    }""")

                    raw_pos = await ad.evaluate("n => { let p = n.closest('div'); return p ? p.className : ''; }")
                    txt = await ad.inner_text() or ""
                    
                    clean_img = img_src.strip()
                    clean_txt = re.sub(r'<[^>]+>', '', txt).strip()

                    bad_img_urls = ["board/lists", "gall.dcinside.com", "m.dcinside.com"]
                    if clean_img and any(bad in clean_img.lower() for bad in bad_img_urls): clean_img = ""

                    if clean_txt: clean_txt = re.sub(r'^(AD|ad)\s*', '', clean_txt).strip()
                    if clean_txt in ["광고안내", "갤러리", "이미지 배너", "null", "dcinside.com"]: clean_txt = ""
                    if not clean_img and not clean_txt: continue

                    # 외부 네트워크 광고 필터링
                    external_ad_networks = ["google", "adsrvr", "criteo", "taboola", "doubleclick", "adnxs", "smartadserver", "naver.com", "ader.naver.com", "nclick", "kakao", "daum", "mobon", "exelbid"]
                    if any(k in clean_href for k in external_ad_networks): continue

                    junk_images = ["close", "x_btn", "traffic_", "default_banner", "noimage", "icon", "btn_ad_close"]
                    if clean_img and any(j in clean_img.lower() for j in junk_images): continue

                    is_real_ad = any(x in clean_href for x in ["addc.dc", "netinsight", "toast", "utm_source"])
                    if not is_real_ad: continue

                    found_dc_ad_in_this_round = True

                    if not raw_href.startswith("javascript") and raw_href != "#" and "__click__" not in raw_href.lower():
                        final_url = await get_final_landing_url(context, raw_href, base_page_url)
                    else: final_url = raw_href

                    clean_final = final_url.strip()
                    if clean_final.endswith("#") or "/board/lists" in clean_final or "dcad" in clean_final: continue

                    clean_final = clean_final.replace("__CLICK__", "").replace("__click__", "")
                    if not clean_final or clean_final.lower() in ["null", "#", "http://null", "https://null"]: clean_final = "랜딩 URL 없음 (클릭 이벤트)"
                    if "nstatic.dcinside.com" in clean_final: clean_final = "랜딩 URL 없음 (이미지 서버)"

                    has_img = bool(clean_img)
                    pos = get_korean_position(env, page_type, raw_pos, has_img, raw_href, clean_href + " " + clean_final)
                    ad_signature = f"{pos}|{clean_img}|{clean_final}"

                    if ad_signature not in seen:
                        seen.add(ad_signature)
                        text_val = "이미지 배너" if has_img and not clean_txt else clean_txt
                        print(f"✅ {prefix} [유효 {valid_attempts+1}/{max_valid}회차] {pos} (새로운 소재 추가)")
                        collected.append({
                            "date": today, "gallery": gallery, "env": env, "pos": pos,
                            "url": clean_final, "img": clean_img, "text": text_val
                        })
            except: continue

        if found_dc_ad_in_this_round:
            valid_attempts += 1
            if valid_attempts % 10 == 0:
                print(f"📊 {prefix} [진행률] 유효 {valid_attempts}/{max_valid}회 완료 (누적 시도: {total_attempts}, 수집 소재: {len(collected)}개)")
        else:
            print(f"⚠️ {prefix} [전체 구글광고 덮임] 카운트 미차감 (현재 유효: {valid_attempts}/{max_valid}, 누적 시도: {total_attempts})")

    print(f"🏁 {prefix} 수집 종료! 유효 {valid_attempts}/{max_valid}회, 총 시도 {total_attempts}회, 수집 소재 {len(collected)}개")
    return collected


# --- ⚡ 단일 갤러리+환경 작업 실행기 ---
async def task_runner(sem, ctx, env, tgt, queue):
    async with sem:
        await asyncio.sleep(random.uniform(0, 1.5))
        page = await ctx.new_page()
        page.on("dialog", lambda dialog: asyncio.create_task(dialog.accept()))
        await page.route("**/*", block_resources)
        try:
            target_url = tgt['pc'] if env == "PC" else tgt['mo']
            gallery_id = target_url.split("id=")[-1].split("&")[0] if "id=" in target_url else target_url.split("/")[-1]

            await page.goto(target_url, wait_until="load", timeout=15000)
            await asyncio.sleep(1.5)

            page_title, current_url = await page.title(), page.url.lower()
            keyword = tgt['name'].replace("갤러리", "").replace(" ", "").strip()

            bounce_urls = ["https://www.dcinside.com", "https://gall.dcinside.com", "https://m.dcinside.com", "https://gall.dcinside.com/m", "https://gall.dcinside.com/mini"]
            if keyword not in page_title.replace(" ", "") or current_url in bounce_urls:
                test_urls = [f"https://gall.dcinside.com/board/lists/?id={gallery_id}", f"https://gall.dcinside.com/mgallery/board/lists/?id={gallery_id}", f"https://gall.dcinside.com/mini/board/lists/?id={gallery_id}"] if env == "PC" else [f"https://m.dcinside.com/board/{gallery_id}", f"https://m.dcinside.com/mini/{gallery_id}"]
                for t_url in test_urls:
                    await page.goto(t_url, wait_until="load", timeout=12000)
                    await asyncio.sleep(1)
                    if keyword in (await page.title()).replace(" ", ""): break

            print(f"🌐 [{env}] {tgt['name']} 접속 완료. 리스트 수집 시작!")

            for item in await capture_ads(ctx, page, env, tgt['name'], "리스트"): await queue.put(item)

            # 🔥 스마트 게시글 판독기: '설문', '공지'가 아닌 "진짜 숫자로 된 일반 게시글"만 찾아 들어갑니다.
            post_href = None
            if env == "PC":
                rows = await page.locator("tr.us-post").all()
                for row in rows:
                    try:
                        num_text = await row.locator("td.gall_num").inner_text()
                        if num_text.strip().isdigit(): # 글 번호가 숫자인 것만 취급 (설문, 공지 회피)
                            a_tag = row.locator("td.gall_tit > a:not(.reply_numbox)").first
                            post_href = await a_tag.get_attribute("href")
                            if post_href: break
                    except: continue
            else:
                rows = await page.locator("ul.gall-detail-lst li").all()
                for row in rows:
                    try:
                        class_name = await row.get_attribute("class") or ""
                        if "notice" in class_name or "sp-lst" in class_name: continue
                        title_text = await row.inner_text()
                        if "설문" not in title_text and "공지" not in title_text: # 텍스트로도 설문/공지 회피
                            a_tag = row.locator("a.lt").first
                            post_href = await a_tag.get_attribute("href")
                            if post_href: break
                    except: continue

            if post_href:
                if not post_href.startswith("http"):
                    post_href = ("https://gall.dcinside.com" if env == "PC" else "https://m.dcinside.com") + post_href

                if env == "MO" and ("gall.dcinside.com" in post_href or "board/view" in post_href):
                    try:
                        from urllib.parse import urlparse, parse_qs
                        parsed = urlparse(post_href)
                        qs = parse_qs(parsed.query)
                        g_id, g_no = qs.get("id", [gallery_id])[0], qs.get("no", [""])[0]
                        post_href = f"https://m.dcinside.com/board/{g_id}/{g_no}" if g_no else f"https://m.dcinside.com/board/{g_id}"
                    except:
                        post_href = post_href.replace("gall.dcinside.com", "m.dcinside.com").replace("/board/view/?id=", "/board/")

                await page.goto(post_href, wait_until="load", timeout=15000)
                await asyncio.sleep(2.5)
                for item in await capture_ads(ctx, page, env, tgt['name'], "본문"): await queue.put(item)
            else:
                print(f"⚠️ [{env}] {tgt['name']} 일반 게시글을 찾지 못했습니다.")
        except Exception as e: print(f"⚠️ [{env}] {tgt['name']} 전체 에러: {e}")
        finally:
            try: await page.close()
            except: pass

# --- 🚀 메인 실행 ---
async def main():
    if not TARGET_GALLERIES: return
    print(f"🚀 [서버 {CHUNK_INDEX+1}/{TOTAL_CHUNKS}] 갤러리 {len(TARGET_GALLERIES)}개 담당 시작!")

    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    ws = gc.open_by_url(SHEET_URL).get_worksheet(0)

    async with async_playwright() as p:
        pc_context_opts = { "viewport": {"width": 1920, "height": 1080}, "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36" }
        mo_context_opts = { "user_agent": "Mozilla/5.0 (Linux; Android 13; SM-G991N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36", "viewport": {"width": 390, "height": 844}, "is_mobile": True, "has_touch": True }

        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-web-security"])
        pc_ctx, mo_ctx = await browser.new_context(**pc_context_opts), await browser.new_context(**mo_context_opts)

        sem, queue = asyncio.Semaphore(5), asyncio.Queue()
        uploader = asyncio.create_task(uploader_worker(queue, ws))

        tasks = [task_runner(sem, pc_ctx, "PC", t, queue) for t in TARGET_GALLERIES] + [task_runner(sem, mo_ctx, "MO", t, queue) for t in TARGET_GALLERIES]
        await asyncio.gather(*tasks)
        await browser.close()

        await queue.put(None)
        await uploader
        print(f"🎉 [서버 {CHUNK_INDEX+1}] 수집 종료!")

if __name__ == "__main__": asyncio.run(main())
