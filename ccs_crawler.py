#!/usr/bin/env python3
"""
Automated crawler for CCS website product collateral.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    JavascriptException,
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
)
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:  # pragma: no cover - optional dependency
    ChromeDriverManager = None  # type: ignore

BASE_URL = "https://www.ccs-grp.com"
LOGIN_URL = f"{BASE_URL}/login/"
SERIES_INDEX_URL = f"{BASE_URL}/products/series/"


SELECTORS = {
    "login_username": "form#CustomerLoginForm input[name='Customer[login]']",
    "login_password": "form#CustomerLoginForm input[name='Customer[customerPassword]']",
    "login_button": "form#CustomerLoginForm button[type='submit']",
    "series_list": "div.seriesList a.seriesList__item",
    "series_product_table": "#proDetailBlock table tbody tr",
    "product_code_cell": "th.model-detail h5 a",
    "product_download_icon": "td.button a",
    "download_cart_panel": "#download-menu",
    "download_button": "#download-menu a[href='/mypage/download/']",
    "download_confirm_button": "#DownloadForm_finish2",
    "step_popup_iframe": "iframe[src*='cadenas']",
    "step_add_format_button": "button[data-action='addFormats']",
    "step_format_checkbox": "input[type='checkbox'][value*='STEP AP214']",
    "step_start_btn": "button[data-action='startGeneration']",
    "step_download_btn": "a[data-action='download']",
    "portal_download_cad": "button[data-action='downloadCad']",
    "portal_format_settings": "button[data-action='formatSettings']",
}


DOWNLOAD_KEYWORDS = [
    ("manual", "_Manual", ".pdf"),
    ("catalog", "_Catalog", ".pdf"),
    ("dimension", "_Dimension", ".pdf"),
    ("dimension drawing", "_Dimension", ".pdf"),
    ("dxf", "_DXF", ".zip"),
    ("step", "_STEP", ".zip"),
    ("datasheet", "_Datasheet", ".pdf"),
    ("data sheet", "_Datasheet", ".pdf"),
]


PDF_MAGIC = b"%PDF"


class DownloadTimeout(RuntimeError):
    pass


@dataclass
class ProductContext:
    code: str
    series_name: str
    series_url: str
    product_url: str


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CCS product collateral crawler")
    parser.add_argument("--username", default="Samir@MachineVisionDirect.com", help="CCS login email")
    parser.add_argument("--password", default="MVDLogin1!", help="CCS login password")
    parser.add_argument("--output-root", default="/Users/apple/Documents/Crwal/output_clean", help="Directory where products are stored")
    parser.add_argument("--series-url", action="append", help="Series URL to crawl (repeatable)")
    parser.add_argument("--series-file", default="series_urls.txt", help="Text/CSV file containing series URLs (one per line)")
    parser.add_argument("--product-file", help="CSV with explicit product URLs or model IDs")
    parser.add_argument("--max-products", type=int, default=0, help="Process only first N products")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even if folder exists")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless")
    parser.add_argument("--chromedriver", help="Path to ChromeDriver binary")
    parser.add_argument("--sleep", type=float, default=0.4, help="Base sleep between interactions")
    parser.add_argument("--download-timeout", type=int, default=300, help="Seconds to wait for download")
    parser.add_argument("--clear-cache", action="store_true", help="Start with clean Chrome profile")
    parser.add_argument("--debug", action="store_true", help="Verbose Selenium logging")
    parser.add_argument("--keep-downloads", action="store_true", help="Do not delete temporary Chrome download folder")
    parser.add_argument("--dump-headers", action="store_true", help="Print download table headers for debugging")
    return parser.parse_args(argv)


def _build_options(download_dir: Path, args: argparse.Namespace, headless_mode: str = "new") -> Options:
    options = Options()
    if args.headless:
        if headless_mode == "legacy":
            options.add_argument("--headless")
        else:
            options.add_argument("--headless=new")
    else:
        print("Running in visible mode - watch Chrome window")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--disable-features=BlockThirdPartyCookies")
    options.add_argument("--allow-third-party-cookies=1")
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "profile.block_third_party_cookies": False,
        "profile.cookie_controls_mode": 0,
    }
    options.add_experimental_option("prefs", prefs)
    if args.clear_cache:
        user_data = tempfile.mkdtemp(prefix="ccs-chrome-")
        options.add_argument(f"--user-data-dir={user_data}")
    service: Optional[Service] = None
    if args.chromedriver:
        service = Service(args.chromedriver)
    elif ChromeDriverManager is not None:
        service = Service(ChromeDriverManager().install())
    return options, service


def create_driver(download_dir: Path, args: argparse.Namespace) -> Chrome:
    options, service = _build_options(download_dir, args, headless_mode="new")
    try:
        driver = webdriver.Chrome(service=service, options=options)
    except WebDriverException as exc:
        if args.headless and "unrecognized chrome option: headless=new" in str(exc).lower():
            options, service = _build_options(download_dir, args, headless_mode="legacy")
            driver = webdriver.Chrome(service=service, options=options)
        else:
            raise
    driver.set_page_load_timeout(60)
    return driver


def human_sleep(base: float):
    time.sleep(max(base * 0.5, 0.05))


def wait_for(driver: Chrome, condition, timeout: int = 20):
    return WebDriverWait(driver, timeout).until(condition)


def read_series_from_file(path: Path) -> List[str]:
    urls: List[str] = []
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() in (".csv", ".tsv"):
        with path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                urls.append(row[0].strip())
    else:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                urls.append(line)
    return urls


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def sanitize_filename(name: str) -> str:
    name = name.replace("/", "-").replace("\\", "-").replace(":", "-")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def fetch_with_cookies(url: str, driver: Chrome, target_path: Path, timeout: int = 30):
    session = requests.Session()
    for cookie in driver.get_cookies():
        session.cookies.set(cookie["name"], cookie["value"])
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    target_path.write_bytes(resp.content)


def is_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            head = fh.read(4)
        return head == PDF_MAGIC
    except Exception:
        return False


class CCSCrawler:
    def __init__(self, driver: Chrome, args: argparse.Namespace, download_dir: Path):
        self.driver = driver
        self.args = args
        self.temp_download_dir = download_dir
        self.keep_downloads = args.keep_downloads
        self.output_root = Path(args.output_root).expanduser().resolve()
        ensure_dir(self.output_root)
        self.processed_products: List[ProductContext] = []

    def close(self):
        with contextlib.suppress(Exception):
            self.driver.quit()
        if not self.keep_downloads:
            shutil.rmtree(self.temp_download_dir, ignore_errors=True)

    def login(self):
        self.driver.get(LOGIN_URL)
        wait_for(self.driver, EC.visibility_of_element_located((By.CSS_SELECTOR, SELECTORS["login_username"])))
        self.dismiss_trust_banner()
        self.driver.find_element(By.CSS_SELECTOR, SELECTORS["login_username"]).send_keys(self.args.username)
        self.driver.find_element(By.CSS_SELECTOR, SELECTORS["login_password"]).send_keys(self.args.password)
        human_sleep(self.args.sleep)
        # Try clicking login button with JavaScript to bypass overlay issues
        login_button = self.driver.find_element(By.CSS_SELECTOR, SELECTORS["login_button"])
        self.driver.execute_script("arguments[0].click();", login_button)
        wait_for(self.driver, EC.presence_of_element_located((By.CSS_SELECTOR, "header, nav")))

    def collect_series_urls(self, overrides: Optional[List[str]] = None) -> List[str]:
        urls: List[str] = []
        if overrides:
            urls.extend(overrides)
        else:
            self.driver.get(SERIES_INDEX_URL)
            wait_for(self.driver, EC.presence_of_all_elements_located((By.CSS_SELECTOR, SELECTORS["series_list"])))
            anchors = self.driver.find_elements(By.CSS_SELECTOR, SELECTORS["series_list"])
            for anchor in anchors:
                href = anchor.get_attribute("href")
                if href:
                    urls.append(href)
        return urls

    def collect_products_from_series(self, series_url: str) -> List[ProductContext]:
        self.driver.get(series_url)
        human_sleep(self.args.sleep)
        wait_for(self.driver, EC.presence_of_all_elements_located((By.CSS_SELECTOR, SELECTORS["series_product_table"])))
        rows = self.driver.find_elements(By.CSS_SELECTOR, SELECTORS["series_product_table"])
        series_name = self.driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
        products: List[ProductContext] = []
        for row in rows:
            try:
                link = row.find_element(By.CSS_SELECTOR, SELECTORS["product_code_cell"])
                product_code = sanitize_filename(link.text.strip())
                product_url = link.get_attribute("href") or series_url
                products.append(ProductContext(product_code, series_name, series_url, product_url))
            except NoSuchElementException:
                continue
        return products

    def crawl(self, product_filters: Optional[Iterable[str]] = None):
        filters = set(product_filters or [])
        series_overrides: List[str] = []
        if self.args.series_url:
            series_overrides.extend(self.args.series_url)
        if self.args.series_file:
            series_overrides.extend(read_series_from_file(Path(self.args.series_file)))
        series_urls = self.collect_series_urls(series_overrides or None)
        progress = tqdm(series_urls, desc="Series", unit="series")
        processed_count = 0
        for series_url in progress:
            products = self.collect_products_from_series(series_url)
            if filters:
                products = [p for p in products if p.code in filters or p.product_url in filters]
            for product in tqdm(products, desc="Products", unit="product", leave=False):
                if self.args.max_products and processed_count >= self.args.max_products:
                    return
                try:
                    if self.process_product(product):
                        processed_count += 1
                    self.processed_products.append(product)
                except Exception as e:
                    print(f"Error processing {product.code}: {e}")
                    continue

    def process_product(self, context: ProductContext) -> bool:
        dest_dir = self.output_root / context.code
        if dest_dir.exists() and not self.args.overwrite:
            return False
        ensure_dir(dest_dir)
        ensure_dir(dest_dir / "Images")
        # Try up to two attempts to gather required artifacts
        for attempt in range(2):
            self.driver.get(context.series_url)
            human_sleep(self.args.sleep)
            self.clear_download_cart()
            self.driver.get(context.series_url)
            human_sleep(self.args.sleep)
            row = self.find_product_row(context.code)
            if row is None:
                print(f"Warning: Could not find product row for {context.code}, skipping...")
                self.cleanup_dest_dir(dest_dir)
                return False
            with contextlib.suppress(Exception):
                self.download_product_image(context, dest_dir, row)
            with contextlib.suppress(Exception):
                self.download_product_manual(context, dest_dir)
            self.driver.get(context.series_url)
            human_sleep(self.args.sleep)
            row = self.find_product_row(context.code)
            if row is None:
                print(f"Warning: Could not find product row for {context.code} on second attempt, skipping...")
                self.cleanup_dest_dir(dest_dir)
                return False
            self.clear_download_dir()
            downloads = self.collect_required_documents(context, row)
            self.transform_downloads(context, dest_dir, downloads)
            if self._required_files_present(dest_dir):
                break
            # If required still missing, small delay and retry building cart
            human_sleep(max(self.args.sleep, 0.5))
        # Mark as complete only if required files present
        if self._required_files_present(dest_dir):
            try:
                (dest_dir / ".complete").write_text("ok", encoding="utf-8")
            except Exception:
                pass
        return True
        self.cleanup_dest_dir(dest_dir)
        return False

    def download_product_image(self, context: ProductContext, dest_dir: Path, row):
        img_el = row.find_element(By.CSS_SELECTOR, "div.model-thumbnail img")
        src = img_el.get_attribute("data-src") or img_el.get_attribute("src")
        if not src:
            return
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = BASE_URL + src
        image_ext = Path(src).suffix or ".png"
        image_path = dest_dir / "Images" / f"{context.code}{image_ext}"
        fetch_with_cookies(src, self.driver, image_path)

    def download_product_manual(self, context: ProductContext, dest_dir: Path):
        manual_path = dest_dir / f"{context.code}_Manual.pdf"
        if manual_path.exists() and not self.args.overwrite:
            return
        self.driver.get(context.product_url)
        human_sleep(self.args.sleep)
        manual_links = self.driver.find_elements(
            By.XPATH,
            "//a[contains(translate(normalize-space(string(.)), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'manual')]",
        )
        for link in manual_links:
            href = link.get_attribute("href")
            if not href or ".pdf" not in href.lower():
                continue
            fetch_with_cookies(href, self.driver, manual_path)
            return

    def clear_download_dir(self):
        for path in self.temp_download_dir.glob("*"):
            if path.is_file():
                path.unlink(missing_ok=True)
            else:
                shutil.rmtree(path, ignore_errors=True)

    def cleanup_dest_dir(self, dest_dir: Path):
        if dest_dir.exists():
            shutil.rmtree(dest_dir, ignore_errors=True)

    def clear_download_cart(self):
        try:
            self.driver.get(f"{BASE_URL}/mypage/download/")
            human_sleep(self.args.sleep)
            wait_for(
                self.driver,
                EC.presence_of_element_located((By.CSS_SELECTOR, "form#DownloadForm")),
                timeout=10,
            )
        except TimeoutException:
            return
        while True:
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "#cart_header .btn-style08 a")
            if not buttons:
                break
            self.safe_click(buttons[0])
            human_sleep(max(self.args.sleep / 2, 0.3))

    def collect_required_documents(self, context: ProductContext, row) -> List[Path]:
        downloads: List[Path] = []
        with contextlib.suppress(Exception):
            step_path = self.fetch_step_file(context, row)
            if step_path:
                downloads.append(step_path)
        self.add_documents_to_cart(context, row)
        human_sleep(self.args.sleep)
        downloads.extend(self.trigger_batch_download(context))
        self.driver.get(context.series_url)
        human_sleep(self.args.sleep)
        return downloads

    def find_product_row(self, product_code: str):
        rows = self.driver.find_elements(By.CSS_SELECTOR, SELECTORS["series_product_table"])
        for row in rows:
            try:
                label = row.find_element(By.CSS_SELECTOR, "th.model-detail h5").text.strip()
            except NoSuchElementException:
                continue
            if product_code in label:
                return row
        return None

    def add_documents_to_cart(self, context: ProductContext, row):
        # Click only the required cells by fuzzy-matching header labels.
        headers = self.driver.find_elements(By.CSS_SELECTOR, "#proDetailBlock table thead th")
        header_labels: List[str] = [h.text.strip().lower() for h in headers]
        if self.args.dump_headers:
            print(f"[DEBUG] headers for {context.code}: {header_labels}")
        include_any = [
            "catalog",
            "pdf drawing",
            "dxf",
            "data sheet",
            "datasheet",
            "manual",
        ]
        exclude_any = [
            "warranty",
            "environmental",
            "handling precautions",
        ]
        cells = row.find_elements(By.CSS_SELECTOR, "th, td.button")
        for idx, label in enumerate(header_labels):
            if any(word in label for word in exclude_any):
                continue
            if not any(word in label for word in include_any):
                continue
            if idx >= len(cells):
                continue
            links = cells[idx].find_elements(By.TAG_NAME, "a")
            if not links:
                continue
            if self.safe_click(links[0]):
                human_sleep(max(self.args.sleep / 2, 0.2))

    def safe_click(self, element):
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        except JavascriptException:
            pass
        try:
            element.click()
            return True
        except (ElementClickInterceptedException, StaleElementReferenceException):
            try:
                self.driver.execute_script("arguments[0].click();", element)
                return True
            except Exception:
                return False

    def trigger_batch_download(self, context: ProductContext) -> List[Path]:
        # Navigate directly to the consolidated download page to avoid
        # timing issues with the floating cart panel visibility.
        self.driver.get(f"{BASE_URL}/mypage/download/")
        wait_for(
            self.driver,
            EC.presence_of_element_located((By.CSS_SELECTOR, "form#DownloadForm")),
            timeout=30,
        )
        self.dismiss_trust_banner()
        ignore_existing = {p.name for p in self.temp_download_dir.glob("*") if p.is_file()}
        self.safe_click(self.driver.find_element(By.CSS_SELECTOR, SELECTORS["download_confirm_button"]))
        archive = self.wait_for_download("batch", self.args.download_timeout, ignore_existing=ignore_existing)
        return [archive]

    def wait_for_download(self, label: str, timeout: int, ignore_existing: Optional[set] = None) -> Path:
        start = time.time()
        existing = set(ignore_existing or [])
        while time.time() - start < timeout:
            cr_downloads = list(self.temp_download_dir.glob("*.crdownload"))
            if cr_downloads:
                time.sleep(1)
                continue
            files = [
                p
                for p in self.temp_download_dir.glob("*")
                if p.is_file() and p.name not in existing
            ]
            if files:
                return sorted(files, key=lambda p: p.stat().st_mtime)[-1]
            time.sleep(1)
        raise DownloadTimeout(f"Timed out waiting for {label} download")

    def fetch_step_file(self, context: ProductContext, row) -> Optional[Path]:
        step_links = row.find_elements(By.CSS_SELECTOR, "td.button a[href*='display3dcad']")
        if not step_links:
            return None
        step_button = step_links[0]
        self.dismiss_trust_banner()
        current_tabs = self.driver.window_handles
        self.safe_click(step_button)
        human_sleep(self.args.sleep)
        try:
            WebDriverWait(self.driver, 10).until(lambda d: len(d.window_handles) > len(current_tabs))
        except TimeoutException:
            return None
        new_tabs = [handle for handle in self.driver.window_handles if handle not in current_tabs]
        if not new_tabs:
            return None
        step_tab = new_tabs[0]
        main_tab = self.driver.current_window_handle
        self.driver.switch_to.window(step_tab)
        try:
            self.prepare_cad_portal(context)
            self.trigger_cad_download(context)
            ignore_existing = {p.name for p in self.temp_download_dir.glob("*") if p.is_file()}
            step_path = self.wait_for_download("STEP", self.args.download_timeout, ignore_existing=ignore_existing)
        finally:
            self.driver.close()
            self.driver.switch_to.window(main_tab)
        return step_path

    def dismiss_trust_banner(self):
        selectors = [
            ".T360Banner_Accept",
            ".T360Banner_Reject",
            ".T360Button-Primary",
            ".trust360-privacy-button",
            ".T360Card",
            ".T360Banner_Card",
            ".T360PurposeOverviewBanner_Card",
        ]
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                wait_for(
                    self.driver,
                    EC.presence_of_element_located((By.CSS_SELECTOR, ", ".join(selectors))),
                    timeout=2,
                )
            except TimeoutException:
                return
            
            # Try to click accept buttons
            clicked = False
            for css in (".T360Banner_Accept", ".T360Button-Primary", ".T360Banner_Reject"):
                with contextlib.suppress(NoSuchElementException):
                    button = self.driver.find_element(By.CSS_SELECTOR, css)
                    if button.is_displayed():
                        self.safe_click(button)
                        clicked = True
                        break
            
            # If clicking didn't work, try to hide elements
            for css in (".trust360-privacy-button", ".T360Card", ".T360Banner_Card", ".T360PurposeOverviewBanner_Card"):
                with contextlib.suppress(NoSuchElementException):
                    element = self.driver.find_element(By.CSS_SELECTOR, css)
                    self.driver.execute_script("arguments[0].style.display = 'none';", element)
                    clicked = True
            
            # Try to remove any overlay elements
            try:
                self.driver.execute_script("""
                    var elements = document.querySelectorAll('.T360Card, .T360Banner_Card, .T360PurposeOverviewBanner_Card, .trust360-privacy-button');
                    for(var i=0; i<elements.length; i++){
                        elements[i].parentNode.removeChild(elements[i]);
                    }
                """)
                clicked = True
            except Exception:
                pass
                
            if clicked:
                with contextlib.suppress(TimeoutException):
                    WebDriverWait(self.driver, 5).until_not(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".Trust360"))
                    )
                human_sleep(0.5)
                return


    def prepare_cad_portal(self, context: ProductContext):
        human_sleep(self.args.sleep)
        with contextlib.suppress(NoSuchElementException):
            workaround = self.driver.find_element(By.CSS_SELECTOR, "div.cookie-error-messages a[target='_blank']")
            href = workaround.get_attribute("href")
            if href:
                self.driver.get(href)
                human_sleep(self.args.sleep)
        with contextlib.suppress(TimeoutException):
            wait_for(self.driver, EC.element_to_be_clickable((By.CSS_SELECTOR, SELECTORS["portal_download_cad"])), 20)
            self.safe_click(self.driver.find_element(By.CSS_SELECTOR, SELECTORS["portal_download_cad"]))
        with contextlib.suppress(NoSuchElementException):
            self.safe_click(self.driver.find_element(By.CSS_SELECTOR, SELECTORS["portal_format_settings"]))
            human_sleep(self.args.sleep)
        with contextlib.suppress(NoSuchElementException):
            checkbox = self.driver.find_element(By.XPATH, "//label[contains(., 'STEP AP214')]/input")
            if not checkbox.is_selected():
                self.safe_click(checkbox)

    def trigger_cad_download(self, context: ProductContext):
        with contextlib.suppress(NoSuchElementException):
            button = self.driver.find_element(By.XPATH, "//button[contains(., 'Start generation')]")
            self.safe_click(button)
        human_sleep(self.args.sleep * 2)
        with contextlib.suppress(NoSuchElementException):
            download_btn = self.driver.find_element(By.XPATH, "//a[contains(., 'Download')]")
            self.safe_click(download_btn)

    def transform_downloads(self, context: ProductContext, dest_dir: Path, downloads: List[Path]):
        for download in downloads:
            if download.suffix.lower() == ".zip":
                self.process_zip(download, context, dest_dir)
            else:
                self.distribute_file(download, context, dest_dir)

    def _required_files_present(self, dest_dir: Path) -> bool:
        required = [
            "_Catalog.pdf",
            "_Dimension.pdf",
            "_DXF.zip",
            "_STEP.zip",
            "_Manual.pdf",
        ]
        names = {p.name for p in dest_dir.glob("*") if p.is_file()}
        product_code = dest_dir.name
        
        # Check that files exist and have proper content
        for suffix in required:
            matching_files = [name for name in names if name.endswith(suffix)]
            if not matching_files:
                if suffix == "_Manual.pdf" or suffix == "_STEP.zip":
                    # These are optional, so continue if not found
                    continue
                return False
            
            # Verify file is not empty and has expected content
            file_path = dest_dir / matching_files[0]
            if file_path.stat().st_size < 1000:  # Files should be at least 1KB
                print(f"Warning: {file_path} is too small")
                return False
                
            # For PDFs, check they have the PDF magic number
            if suffix.endswith(".pdf"):
                if not is_pdf(file_path):
                    print(f"Warning: {file_path} is not a valid PDF")
                    return False
                    
            # Make sure the filename contains the product code
            if product_code not in file_path.name:
                print(f"Warning: {file_path} doesn't match product code {product_code}")
                return False
        
        # Check for image
        image_dir = dest_dir / "Images"
        if not image_dir.exists() or not any(p.is_file() and p.stat().st_size > 1000 for p in image_dir.glob("*")):
            return False
            
        return True

    def process_zip(self, zip_path: Path, context: ProductContext, dest_dir: Path):
        with tempfile.TemporaryDirectory(prefix="ccs-unzip-") as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            shutil.unpack_archive(str(zip_path), tmp_dir)
            for file_path in tmp_dir_path.rglob("*"):
                if file_path.is_file():
                    self.distribute_file(file_path, context, dest_dir)

    def distribute_file(self, file_path: Path, context: ProductContext, dest_dir: Path):
        name_lower = file_path.name.lower()
        suffix_entry = next((entry for entry in DOWNLOAD_KEYWORDS if entry[0] in name_lower), None)
        if not suffix_entry:
            if name_lower.startswith("c_"):
                suffix_entry = ("catalog", "_Catalog", ".pdf")
            elif name_lower.startswith("d_"):
                suffix_entry = ("dimension", "_Dimension", ".pdf")
            elif name_lower.startswith("m_") and "datasheet" not in name_lower:
                suffix_entry = ("manual", "_Manual", ".pdf")
            elif "datasheet" in name_lower or "data-sheet" in name_lower:
                suffix_entry = ("datasheet", "_Datasheet", ".pdf")
            elif self._looks_like_dimension(name_lower, context):
                suffix_entry = ("dimension", "_Dimension", ".pdf")
            elif context.code.lower() in name_lower and file_path.suffix.lower() == ".pdf":
                suffix_entry = ("datasheet", "_Datasheet", ".pdf")
            elif file_path.suffix.lower() in {".stp", ".step"}:
                suffix_entry = ("step", "_STEP", ".zip")
        if not suffix_entry:
            return
        suffix = suffix_entry[1]
        target_ext = suffix_entry[2]
        if target_ext == ".pdf" and not is_pdf(file_path):
            return
        if target_ext == ".zip":
            if file_path.suffix.lower() in (".zip", ".gz", ".rar"):
                target_path = dest_dir / f"{context.code}{suffix}.zip"
                shutil.copy(file_path, target_path)
            else:
                archive_base = dest_dir / f"{context.code}{suffix}"
                with tempfile.TemporaryDirectory(prefix="ccs-pack-") as tmp_zip_dir:
                    temp_archive_dir = Path(tmp_zip_dir)
                    temp_target = temp_archive_dir / file_path.name
                    shutil.copy(file_path, temp_target)
                    shutil.make_archive(str(archive_base), "zip", tmp_zip_dir)
            return
        # PDFs and other documents copied directly
        target_path = dest_dir / f"{context.code}{suffix}{target_ext}"
        shutil.copy(file_path, target_path)

    def _looks_like_dimension(self, name_lower: str, context: ProductContext) -> bool:
        stem = name_lower.split(".")[0]
        if "dimension" in stem or "drawing" in stem:
            return True
        if stem.endswith("_e") and stem.startswith(context.code.lower()):
            return True
        return False


def load_product_filters(path: Optional[str]) -> List[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(file_path)
    products: List[str] = []
    if file_path.suffix.lower() in (".csv", ".tsv"):
        with file_path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                products.append(row[0].strip())
    else:
        products = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return products


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    product_filters = load_product_filters(args.product_file)
    download_dir = Path(tempfile.mkdtemp(prefix="ccs-chrome-download-"))
    driver = create_driver(download_dir, args)
    crawler = CCSCrawler(driver, args, download_dir)
    try:
        crawler.login()
        crawler.crawl(product_filters)
        print(f"Completed {len(crawler.processed_products)} product(s). Output -> {crawler.output_root}")
    except KeyboardInterrupt:
        print("Interrupted by user", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        if args.debug:
            raise
        return 1
    finally:
        crawler.close()
        if not args.keep_downloads:
            shutil.rmtree(download_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

