#!/usr/bin/env python3
"""
next_headless_login.py — Headless login till Next Tech.
Strategi: Navigera till SPA, vänta på OIDC → login → consent → SPA load,
sedan vänta på att SPA gör sin interna API-init (exchange OIDC → CGI session).
"""
import json, time, os, re
from playwright.sync_api import sync_playwright

CONFIG_PATH = "/opt/rm-infra/next-config.json"
CLIENT_URL = "https://project.next-tech.com/301821/client"
COOKIE_FILE = "/opt/rm-infra/next-session.json"

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def login():
    cfg = load_config()
    username = cfg["username"]
    credential = cfg["credential"]

    print(f"[{time.strftime('%H:%M:%S')}] Startar headless login som {username}...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()

        # Fånga alla API-anrop
        api_responses = []
        def on_response(response):
            if 'me.cgi' in response.url:
                api_responses.append(f"{response.status} {response.url[:100]}")

        page.on("response", on_response)

        # 1. Navigera
        print(f"[{time.strftime('%H:%M:%S')}] Navigerar till SPA...")
        page.goto(CLIENT_URL, timeout=30000)

        # 2. Kolla om vi hamnar på login
        time.sleep(3)
        current = page.url

        if "login.next-tech.com" in current:
            print(f"[{time.strftime('%H:%M:%S')}] Login-sida — fyller i credentials...")

            # Vänta på formulär
            page.wait_for_selector('input[name="username"]', timeout=10000)

            # Ändra type och fyll i
            page.evaluate('''(creds) => {
                const input = document.querySelector('input[name="username"]');
                input.type = "text";
                input.value = creds.user;
                input.dispatchEvent(new Event("input", {bubbles: true}));
                input.dispatchEvent(new Event("change", {bubbles: true}));

                // Kolla om lösenordsfält finns
                const pwd = document.querySelector('input[type="password"]');
                if (pwd) {
                    pwd.value = creds.pass;
                    pwd.dispatchEvent(new Event("input", {bubbles: true}));
                    pwd.dispatchEvent(new Event("change", {bubbles: true}));
                }

                // Aktivera submit
                const btn = document.querySelector('button[type="submit"]');
                if (btn) btn.disabled = false;
            }''', {"user": username, "pass": credential})

            time.sleep(0.5)

            # Använd React setState för att kringgå validering
            page.evaluate('''(creds) => {
                // Hitta React-instansen och sätt state direkt
                const input = document.querySelector('input[name="username"]');
                const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
                nativeInputValueSetter.call(input, creds.user);
                input.dispatchEvent(new Event("input", {bubbles: true}));

                const pwd = document.querySelector('input[type="password"]');
                if (pwd) {
                    nativeInputValueSetter.call(pwd, creds.pass);
                    pwd.dispatchEvent(new Event("input", {bubbles: true}));
                }

                setTimeout(() => {
                    const btn = document.querySelector('button[type="submit"]');
                    if (btn) { btn.disabled = false; btn.click(); }
                }, 300);
            }''', {"user": username, "pass": credential})

            print(f"[{time.strftime('%H:%M:%S')}] Form submitted via React state...")
            time.sleep(5)
            print(f"[{time.strftime('%H:%M:%S')}] URL efter submit: {page.url}")

            # Om fortfarande på login — lösenord kanske separat steg
            if "login.next-tech.com" in page.url:
                pwd_input = page.query_selector('input[type="password"]')
                if pwd_input and pwd_input.is_visible():
                    print(f"[{time.strftime('%H:%M:%S')}] Lösenord separat steg")
                    page.evaluate('''(pass_val) => {
                        const pwd = document.querySelector('input[type="password"]');
                        const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
                        setter.call(pwd, pass_val);
                        pwd.dispatchEvent(new Event("input", {bubbles: true}));
                        setTimeout(() => {
                            const btn = document.querySelector('button[type="submit"]');
                            if (btn) { btn.disabled = false; btn.click(); }
                        }, 300);
                    }''', credential)
                    time.sleep(5)

            # Kolla consent
            if "consent" in page.url.lower():
                print(f"[{time.strftime('%H:%M:%S')}] Consent-screen")
                page.evaluate('''
                    const btn = document.querySelector('button[type="submit"]');
                    if (btn) btn.click();
                ''')
                time.sleep(5)

        # 3. Vänta på att SPA laddar klart
        print(f"[{time.strftime('%H:%M:%S')}] Väntar på SPA-init...")
        time.sleep(8)
        print(f"[{time.strftime('%H:%M:%S')}] URL: {page.url}")
        print(f"[{time.strftime('%H:%M:%S')}] CGI-anrop: {api_responses}")

        # 4. Testa API ordentligt
        for attempt in range(3):
            test = page.evaluate("""
                async () => {
                    try {
                        const r = await fetch('/301821/cgi/me.cgi/data/store/ProjectListStore', {
                            headers: {'Accept': 'application/json'},
                            credentials: 'include'
                        });
                        const text = await r.text();
                        if (text.startsWith('<!DOCTYPE') || text.startsWith('<html')) {
                            return {html: true, status: r.status};
                        }
                        try {
                            const data = JSON.parse(text);
                            const rows = data.data || data.rows || data;
                            return {ok: true, count: Array.isArray(rows) ? rows.length : 'unknown', sample: text.substring(0, 300)};
                        } catch(e) {
                            return {text: text.substring(0, 300)};
                        }
                    } catch(e) { return {error: e.message}; }
                }
            """)
            print(f"[{time.strftime('%H:%M:%S')}] API attempt {attempt+1}: {json.dumps(test, ensure_ascii=False)[:300]}")

            if test.get('ok'):
                break
            time.sleep(3)

        if not test.get('ok'):
            print("API returnerar inte JSON — session inte etablerad")
            page.screenshot(path="/tmp/next_login_debug.png")

            # Visa alla cookies
            cookies = context.cookies()
            print(f"Cookies ({len(cookies)}):")
            for c in cookies:
                print(f"  {c['domain']} : {c['name']} = {c['value'][:40]}")

            browser.close()
            return False

        # 5. Spara
        cookies = context.cookies()
        cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies
                                if 'project.next-tech.com' in c.get('domain', ''))

        with open(COOKIE_FILE, "w") as f:
            json.dump(cookies, f, indent=2)
        os.chmod(COOKIE_FILE, 0o600)

        cfg["session_cookie"] = cookie_str
        cfg["session_updated"] = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(CONFIG_PATH, "w") as f:
            json.dump(cfg, f, indent=2)
        os.chmod(CONFIG_PATH, 0o600)

        print(f"[{time.strftime('%H:%M:%S')}] Session sparad — {test.get('count', '?')} projekt hämtade!")
        browser.close()
        return True

if __name__ == "__main__":
    success = login()
    exit(0 if success else 1)
