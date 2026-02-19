from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE_DIR = str(Path("data/autorus_profile").resolve())

def main():
    Path("data").mkdir(exist_ok=True)

    with sync_playwright() as p:
        # persistent profile
        context = p.chromium.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=True,   # нужен GUI только один раз (True)
            locale="ru-RU",
        )
        page = context.new_page()
        page.goto("https://b2b.autorus.ru", wait_until="domcontentloaded")
        print("Войди в аккаунт вручную в открывшемся окне.")
        input("Когда увидишь цены (не гостевой режим) — нажми Enter здесь...")
        context.close()

if __name__ == "__main__":
    main()
