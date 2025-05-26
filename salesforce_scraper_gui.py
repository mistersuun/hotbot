"""
salesforce_scraper_gui_v2.py
────────────────────────────
• UI 100 % CustomTkinter : les identifiants, la ville et la rue sont dans la
  fenêtre principale (plus de popup).
• Téléchargement et cache des municipalités + rues (Overpass) inchangés.
• Scraping Salesforce identique, mais encapsulé dans un thread stoppable propre.
• Robustesse accrue : timeout paramétrables, logs horodatés, fermeture sûre.
"""

from __future__ import annotations

import concurrent.futures as _fut
import contextlib
import csv
import json
import pathlib
import queue
import random
import re
import threading
import time
import tkinter as tk
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path
from tkinter import messagebox  # même en CTk on garde pour le modal natif
from tkinter import filedialog
from typing import Dict, List, Optional

import customtkinter as ctk
import pandas as pd
# ── Dépendances externes ─────────────────────────────────────────────────
import requests
import undetected_chromedriver as uc
from selenium.common.exceptions import (ElementClickInterceptedException,
                                        ElementNotInteractableException,
                                        NoSuchElementException,
                                        NoSuchWindowException,
                                        StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# ── Chemins & configuration ─────────────────────────────────────────────
BASE_DIR = pathlib.Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
CITIES_CACHE = DATA_DIR / "qc_cities.json"
CONFIG_PATH = BASE_DIR / "config.json"

LOCATORS = {
    "input": (
        By.CSS_SELECTOR,
        "input[name='account.sgaAccountNumber']",
    ),  # unique name attr
    "search_btn": (
        By.XPATH,
        "//button[@data-qa='_StyledButton' and"
        "        descendant::span[normalize-space()='Rechercher']]",
    ),
    "header": (By.CSS_SELECTOR, "[data-qa='clic__Header'], .header_container___mGxJS"),
    # the little magnifying-glass that brings the search box back
    "reopen": (
        By.XPATH,
        "//*[(self::span or self::i)        "
        "   and contains(@class,'fa-search')]/ancestor::a[1]",
    ),
}

DATA_DIR.mkdir(exist_ok=True), LOG_DIR.mkdir(exist_ok=True)

import os
import shutil
import subprocess
import sys
import tkinter.filedialog as fd
from pathlib import Path


def downloads_dir() -> Path:
    if sys.platform.startswith("win"):
        return Path(os.path.expandvars(r"%USERPROFILE%\\Downloads"))
    xdg = os.getenv("XDG_DOWNLOAD_DIR")
    return Path(xdg) if xdg else Path.home() / "Downloads"


def _clean_acc(acc: str):
    return re.sub(r"\D", "", acc.strip())


def open_folder(p: Path):
    try:
        if sys.platform.startswith("win"):
            os.startfile(p)
        elif sys.platform.startswith("darwin"):
            subprocess.Popen(["open", p])
        else:
            subprocess.Popen(["xdg-open", p])
    except Exception:
        pass


DEFAULT_CFG = {
    "max_parallel_tabs": 5,
    "mfa_timeout_sec": 60,
    "overpass_timeout": 120,
    "selenium_headless": False,
}
CFG = (
    {**DEFAULT_CFG, **json.loads(CONFIG_PATH.read_text())}
    if CONFIG_PATH.exists()
    else DEFAULT_CFG
)
CONFIG_PATH.write_text(json.dumps(CFG, indent=2))

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {"User-Agent": "QC-Scraper/1.0"}


def _slug(txt: str) -> str:
    """
    Convertit `txt` en identifiant « safe » pour un nom de fichier :
    - minuscules,
    - accents supprimés,
    - caractères non alphanum → tiret bas,
    - tirets multiples réduits à un.
    """
    if not txt:
        return ""
    # normaliser / enlever accents
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()
    # remplacer tout ce qui n'est pas [a-z0-9] par _
    txt = re.sub(r"[^a-zA-Z0-9]+", "_", txt).strip("_").lower()
    return re.sub(r"_{2,}", "_", txt)  # compacter ___


def load_cities_cache(path: Path) -> dict[str, int] | None:
    """
    Lit le cache JSON en UTF-8 et renvoie le dict city→rel_id,
    ou None s'il n'existe pas / n'est pas lisible.
    """
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_cities_cache(path: Path, data: dict[str, int]) -> None:
    """
    Sauvegarde le dict city→rel_id en JSON UTF-8, sans erreur d'encodage.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def fetch_or_load_cities(path: Path) -> dict[str, int]:
    """
    Essaie de charger le cache, sinon fetch_all_cities() et sauvegarde.
    """
    from __main__ import fetch_all_cities  # ou adapte l'import

    cache = load_cities_cache(path)
    if cache is not None:
        return {c: int(r) for c, r in cache.items()}
    mapping = fetch_all_cities()
    save_cities_cache(path, mapping)
    return mapping


# ── Utilitaires Overpass ────────────────────────────────────────────────
def fetch_all_cities() -> Dict[str, int]:
    """Renvoie {nom_ville → id_relation OSM} (admin_level=8) pour le Québec."""
    query = (
        "[out:json][timeout:60];"
        'area["boundary"="administrative"]["admin_level"="4"]["name"="Québec"]->.prov;'
        'rel["boundary"="administrative"]["admin_level"="8"](area.prov);'
        "out tags;"
    )
    r = requests.post(
        OVERPASS_URL, data=query, headers=HEADERS, timeout=CFG["overpass_timeout"]
    )
    r.raise_for_status()
    mapping = {
        elt["tags"]["name"]: elt["id"]
        for elt in r.json().get("elements", [])
        if elt.get("tags", {}).get("name")
    }
    return mapping


def fetch_streets_for_city(rel_id: int) -> List[str]:
    """Renvoie la liste triée des rues (way highway name) dans une municipalité."""
    area_id = 3600000000 + rel_id
    query = (
        "[out:json][timeout:120];"
        f"area({area_id})->.a;"
        '(way["highway"]["name"](area.a););'
        "out tags;"
    )
    r = requests.post(
        OVERPASS_URL, data=query, headers=HEADERS, timeout=CFG["overpass_timeout"] + 60
    )
    r.raise_for_status()
    names = {
        elt["tags"]["name"].strip()
        for elt in r.json().get("elements", [])
        if "tags" in elt and "name" in elt["tags"]
    }
    return sorted(names)


# ── Selenium helpers ────────────────────────────────────────────────────
BASE_DIR = pathlib.Path(__file__).resolve().parent


def build_driver() -> uc.Chrome:
    opts = uc.ChromeOptions()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    if CFG["selenium_headless"]:
        opts.add_argument("--headless=new")

    # ← tell ChromeDriver exactly which chrome.exe to use:
    # opts.binary_location = str(BASE_DIR / "chrome" / "chrome.exe")

    driver = uc.Chrome(options=opts, version_main=136)
    driver.maximize_window()
    return driver


def wait_visible(drv, by, val, timeout=20):
    return WebDriverWait(drv, timeout).until(
        EC.visibility_of_element_located((by, val))
    )


def safe_find(
    drv, css_or_xpath: str, by=By.CSS_SELECTOR, timeout=15, retries=3, sleep_step=0.5
):
    """
    Essaie de localiser un élément plusieurs fois avant d'échouer.
    `css_or_xpath` : chaîne CSS (défaut) ou XPath si `by` est By.XPATH.
    """
    for attempt in range(1, retries + 1):
        try:
            return WebDriverWait(drv, timeout).until(
                EC.presence_of_element_located((by, css_or_xpath))
            )
        except (NoSuchElementException, StaleElementReferenceException):
            if attempt == retries:
                raise
            time.sleep(sleep_step * attempt)


def safe_click(elem, retries=3, sleep_step=0.3):
    """
    Clique en gérant `stale` et `intercepted`.
    """
    for attempt in range(1, retries + 1):
        try:
            elem.click()
            return
        except (
            StaleElementReferenceException,
            ElementClickInterceptedException,
            NoSuchWindowException,
        ):
            if attempt == retries:
                raise
            time.sleep(sleep_step * attempt)


# ───────────────────────────────────────────────────
class ClicDetailScraper(threading.Thread):
    """
    Lit un fichier *doors_*.json|csv, extrait la colonne « Compte client »,
    puis va sur Clic+ pour récupérer les infos détaillées de chaque compte.
    Les résultats sont exportés dans data/specifics_<prefix>_<ts>.csv
    """

    URL = "https://clicplus.int.videotron.com/vui/#/clic/infos-externes"

    def __init__(
        self,
        doors_path: Path,
        gui_q: queue.Queue,
        pause_evt: threading.Event,
        dest_dir: Path,
        clic_user: str,
        clic_pwd: str,
        csr_code: str,
    ):
        super().__init__(daemon=True)
        self.path = doors_path
        self.gui_q = gui_q
        self.pause_evt = pause_evt
        self.driver: Optional[uc.Chrome] = None
        self.clic_user = clic_user
        self.clic_pwd = clic_pwd
        self.csr_code = csr_code
        self.rows: list[dict] = []
        self.dest_dir = dest_dir
        self._stop_evt = threading.Event()

    def stop(self):
        self._stop_evt.set()

    # ---------- helpers --------------------------------------------------
    def _dbg(self, txt: str):
        self.gui_q.put(("log", txt))

    def _with_retries(self, description: str, func, *args, **kwargs):
        """Helper method for retrying operations with logging"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._dbg(
                    f"  Attempting {description} (try {attempt + 1}/{max_retries})"
                )
                result = func(*args, **kwargs)
                self._dbg(f"  ✓ {description} successful")
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    self._dbg(
                        f"  ❌ {description} failed after {max_retries} attempts: {e}"
                    )
                    raise
                self._dbg(
                    f"  ⚠ {description} failed (attempt {attempt + 1}), retrying..."
                )
                time.sleep(1)

    @staticmethod
    def _accounts_from_file(fp: Path) -> list[str]:
        if fp.suffix == ".json":
            data = json.loads(fp.read_text(encoding="utf-8"))
            return [d.get("Compte client") for d in data if d.get("Compte client")]
        else:  # csv
            with fp.open(encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                return [
                    row.get("Compte client") for row in rdr if row.get("Compte client")
                ]

    # ---------- selenium -------------------------------------------------
    def _wait(self, by, sel, to=20):
        return WebDriverWait(self.driver, to).until(
            EC.visibility_of_element_located((by, sel))
        )

    # ───── ClicDetailScraper._login_and_ready  (remplace l'ancienne version)
    def _login_and_ready(self):
        d = self.driver
        d.get(self.URL)

        # ① — Fill in Clic+ credentials (username + password)
        wait = WebDriverWait(d, 20)
        try:
            usr = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='userName']"))
            )
            usr.clear()
            usr.send_keys(self.clic_user)

            pwd = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='password']"))
            )
            pwd.clear()
            pwd.send_keys(self.clic_pwd)

            cont = wait.until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "button[data-qa='clic_infos-externes_StyledButton']",
                    )
                )
            )
            cont.click()
            self._dbg("✔ Clic+ login submitted")

            second_cont_btn = WebDriverWait(d, 30).until(
                EC.visibility_of_element_located(
                    (
                        By.XPATH,
                        "//button[@data-qa='clic_infos-externes_StyledButton']"
                        "[normalize-space(.//span)='Continuer']",
                    )
                )
            )
            # ② Make sure it's in view
            d.execute_script(
                "arguments[0].scrollIntoView({ block: 'center' });", second_cont_btn
            )
            # ③ Use JS to click it (more reliable when something intercepts Selenium's .click())
            d.execute_script("arguments[0].click();", second_cont_btn)

        except Exception as e:
            self._dbg(f"❌ Clic+ login failed: {e}")
            raise

        # ② — Now wait for the account search input to appear
        try:
            wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "input[name='account.sgaAccountNumber']")
                )
            )
            self._dbg("✔ champ compte visible")
        except TimeoutException:
            # panel closed, so open via the search icon
            search_btn = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, ".search_wrapper___39tl7 a, .fa-search")
                )
            )
            search_btn.click()
            self._dbg("✔ click Recherche")
            wait.until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "input[name='account.sgaAccountNumber']")
                )
            )
            self._dbg("✔ champ compte visible")

    def _scrape_one(self, account: str) -> Optional[dict]:
        """Return a dict of all header fields—or None if phone never appeared."""
        d = self.driver
        wait = WebDriverWait(d, 15)  # Increased timeout
        out = {"Compte client": account}
        self._dbg(f"\n🔍 Starting scrape for account: {account}")

        # ② ─ Make sure the account input is clickable (open panel if needed)
        try:
            inp = wait.until(EC.element_to_be_clickable(LOCATORS["input"]))
            self._dbg("✓ Found account input field")
        except TimeoutException:
            # panel was closed, click the magnifier to reopen
            try:
                self._dbg("⚠ Search panel closed, attempting to reopen...")
                icon = wait.until(EC.element_to_be_clickable(LOCATORS["reopen"]))
                icon.click()
                inp = wait.until(EC.element_to_be_clickable(LOCATORS["input"]))
                self._dbg("✓ Successfully reopened search panel")
            except Exception as e:
                self._dbg(f"❌ Failed to reopen search panel: {e}")
                return None

        try:
            inp.clear()
            inp.send_keys(account)
            self._dbg(f"✓ Entered account number: {account}")
        except Exception as e:
            self._dbg(f"❌ Failed to enter account number: {e}")
            return None

        # ③ ─ Click "Rechercher"
        try:
            btn = wait.until(EC.element_to_be_clickable(LOCATORS["search_btn"]))
            btn.click()
            self._dbg("✓ Clicked Rechercher button")
        except Exception as e:
            self._dbg(f"❌ Failed to click Rechercher: {e}")
            return None

        # ④ ─ Wait for the results header with retries
        max_retries = 3
        for retry in range(max_retries):
            try:
                # a) wait for the outer header container
                header = wait.until(
                    EC.visibility_of_element_located(LOCATORS["header"])
                )
                self._dbg("✓ Header container is visible")

                # b) wait for the 'Requérant' sub-block inside it
                wait.until(
                    EC.visibility_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "[data-qa='clic__Header'] [data-qa='clic__Requerant']",
                        )
                    )
                )
                self._dbg("✓ Requérant block is visible")
                break
            except Exception as e:
                if retry == max_retries - 1:
                    self._dbg(
                        f"❌ Failed to load header after {max_retries} attempts: {e}"
                    )
                    return None
                self._dbg(f"⚠ Retry {retry + 1}/{max_retries} for header...")
                time.sleep(2)

        # ⑤ ─ Parse all label/value pairs in that header
        try:
            lines = [l.strip() for l in header.text.splitlines() if l.strip()]
            self._dbg(f"Found {len(lines)//2} fields in header:")
            for i in range(0, len(lines) - 1, 2):
                key = lines[i]
                val = lines[i + 1]
                out[key] = val
                self._dbg(f"  • {key}: {val}")
        except Exception as e:
            self._dbg(f"❌ Failed to parse header fields: {e}")
            return None

        # ─▶ Ensure the phone number is present (retry multiple times if needed)
        contact_css = "[data-qa='clic__Contact']"

        def phone_loaded(driver):
            try:
                txt = driver.find_element(By.CSS_SELECTOR, contact_css).text
                self._dbg(f"Contact block text: {txt}")
            except NoSuchElementException:
                return False
            # look for a pattern like 418 588-4462 or similar
            has_phone = bool(re.search(r"\d{3}\s*\d{3}-\d{4}", txt))
            if has_phone:
                self._dbg("✓ Found phone number pattern in contact block")
            return has_phone

        # Try multiple times with increasing delays
        for retry in range(3):
            if phone_loaded(d):
                break
            self._dbg(
                f"⚠ Phone not found, waiting {5 * (retry + 1)}s (attempt {retry + 1}/3)"
            )
            time.sleep(5 * (retry + 1))
        else:
            self._dbg("❌ Phone number not found after all retries")
            return None

        # now pull out the phone (and email) from the contact block
        try:
            contact_el = d.find_element(By.CSS_SELECTOR, contact_css)
            parts = [ln.strip() for ln in contact_el.text.splitlines() if ln.strip()]
            self._dbg(f"Contact block parts: {parts}")

            # find any email‐looking part
            email = next((p for p in parts if "@" in p), "")
            if email:
                self._dbg(f"✓ Found email: {email}")
            else:
                self._dbg("⚠ No email found")

            # find any phone‐looking part (e.g. 418 588-4462 or 4185884462)
            phone = next(
                (
                    p
                    for p in reversed(parts)
                    if re.search(r"\d{3}[\s\-]?\d{3}[\s\-]?\d{4}", p)
                ),
                "",
            )
            if phone:
                self._dbg(f"✓ Found phone: {phone}")
            else:
                self._dbg("⚠ No phone found")

            out["Courriel"] = email or "N/A"
            out["Téléphone"] = phone or "N/A"
            self._dbg("✓ Added contact info to output")
        except Exception as e:
            self._dbg(f"❌ Failed to extract contact info: {e}")
            return None

        # ⑦ ─ Re-open the search panel
        try:
            reopen = wait.until(EC.element_to_be_clickable(LOCATORS["reopen"]))
            # JS click in case normal click is blocked
            d.execute_script("arguments[0].click();", reopen)
            self._dbg("✓ Reopened search panel for next iteration")
        except Exception as e:
            self._dbg(f"⚠ Failed to reopen search panel: {e}")
            # not a fatal error—interface may still work for next loop

        self._dbg(f"✓ Successfully scraped account {account}")
        return out

    def _scrape_csr(self, account: str) -> Optional[dict]:
        d = self.driver
        wait = WebDriverWait(d, 20)
        out = {"Compte client": account}
        self._dbg(f"\n🔍 Starting CSR scrape for account: {account}")

        # … after login …
        d.get("https://csr.etiya.videotron.com/private/dashboard")
        self._dbg("✓ Loaded CSR dashboard")

        # ① wait for the modal to appear (no change)
        try:
            modal = wait.until(
                EC.visibility_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "div[role='document'].modal-dialog.modal-dialog-centered.modal-sm",
                    )
                )
            )
            self._dbg("✓ Found postal code modal")
        except Exception as e:
            self._dbg(f"❌ Failed to find postal code modal: {e}")
            return None

        try:
            # ② Open postal-code combobox
            combo = self._with_retries(
                "locate postal-code combobox",
                modal.find_element,
                By.CSS_SELECTOR,
                "input[role='combobox']#postal-code",
            )
            self._dbg("✓ Found postal code combobox")

            self._with_retries(
                "click postal-code combobox",
                lambda el: d.execute_script("arguments[0].click();", el),
                combo,
            )
            self._dbg("✓ Clicked postal code combobox")

            # ③ select suggestion
            self._with_retries(
                "select postal-code suggestion",
                combo.send_keys,
                Keys.ARROW_DOWN,
                Keys.ENTER,
            )
            self._dbg("✓ Selected postal code suggestion")

            # ④ enter CSR code
            inline = self._with_retries(
                "locate CSR code input",
                modal.find_element,
                By.CSS_SELECTOR,
                "atoms-input-with-label#work-site-user-number input.form-control",
            )
            self._dbg("✓ Found CSR code input")

            self._with_retries("fill CSR code", inline.send_keys, self.csr_code)
            self._dbg(f"✓ Entered CSR code: {self.csr_code}")

            # ⑤ click Submit
            submit = self._with_retries(
                "locate Submit button",
                modal.find_element,
                By.CSS_SELECTOR,
                "button#Submit-btn",
            )
            self._dbg("✓ Found Submit button")

            self._with_retries(
                "click Submit",
                lambda btn: d.execute_script("arguments[0].click();", btn),
                submit,
            )
            self._dbg("✓ Clicked Submit")

        except Exception as e:
            self._dbg(f"❌ Failed to complete modal: {e}")
            return None

        # 4) click the "user" icon
        try:
            user_icon = self._with_retries(
                "locate CSR user icon",
                wait.until,
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "svg.icon-light.svg-size--4")
                ),
            )
            self._dbg("✓ Found user icon")

            self._with_retries("click CSR user icon", user_icon.click)
            self._dbg("✓ Clicked user icon")
        except Exception as e:
            self._dbg(f"❌ Failed to click user icon: {e}")
            return None

        # 6) enter last 7 digits into custId + ENTER
        try:
            cust = self._with_retries(
                "locate custId field",
                wait.until,
                EC.element_to_be_clickable((By.ID, "custId")),
            )
            self._dbg("✓ Found custId field")

            last_7 = account[-7:]
            self._with_retries("fill custId", cust.send_keys, last_7)
            self._dbg(f"✓ Entered last 7 digits: {last_7}")

            time.sleep(0.7)
            self._with_retries("press ENTER on custId", cust.send_keys, Keys.ENTER)
            self._dbg("✓ Pressed ENTER on custId")
        except Exception as e:
            self._dbg(f"❌ Failed to enter custId: {e}")
            return None

        # 1) wait for the collapse panel
        try:
            panel2 = self._with_retries(
                "wait for collapse panel",
                WebDriverWait(d, 30).until,
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "div[csrcollapse].collapse.show")
                ),
            )
            self._dbg("✓ Found collapse panel")

            d.execute_script("arguments[0].scrollIntoView(true);", panel2)
            d.execute_script("arguments[0].style.overflow = 'visible';", panel2)
            self._dbg("✓ Scrolled panel into view")
        except Exception as e:
            self._dbg(f"❌ Failed to find collapse panel: {e}")
            return None

        # 3) parse entries (no change)
        try:
            entries = panel2.find_elements(By.CSS_SELECTOR, "atoms-key-value")
            self._dbg(f"Found {len(entries)} key-value pairs:")

            for kv in entries:
                key_el = kv.find_element(By.CSS_SELECTOR, "li.key")
                val_el = kv.find_element(By.CSS_SELECTOR, "li.value")
                key = key_el.text.strip()
                val = val_el.get_attribute("title") or val_el.text.strip()
                out[key] = val
                self._dbg(f"  • {key}: {val}")

            self._dbg(f"✓ Successfully parsed {len(entries)} fields")
        except Exception as e:
            self._dbg(f"❌ Failed to parse entries: {e}")
            return None

        out["Téléphone"] = (
            out.get("NUMÉRO DE TÉLÉPHONE PRINCIPAL", "N/A")
            .replace("Mobile - ", "")
            .strip()
        )
        out["Courriel"] = out.get("NOM D'UTILISATEUR", "N/A")
        self._dbg(
            f"✓ Final contact info - Phone: {out['Téléphone']}, Email: {out['Courriel']}"
        )

        self._dbg(f"✓ Successfully scraped CSR data for account {account}")
        return out

    # ---------- thread main ---------------------------------------------
    def run(self):
        try:
            # ── 0) read doors_* file, keep leading zeros ───────────────────
            self._dbg(f"\n📂 Reading input file: {self.path}")
            if self.path.suffix.lower() == ".csv":
                doors_df = pd.read_csv(
                    self.path,
                    encoding="utf-8",
                    dtype={"Compte client": str},  # ← keep zeros
                )
                self._dbg(f"✓ Loaded CSV with {len(doors_df)} rows")
            elif self.path.suffix.lower() == ".json":
                doors_df = pd.DataFrame(json.loads(self.path.read_text("utf-8")))
                doors_df["Compte client"] = doors_df["Compte client"].astype(str)
                self._dbg(f"✓ Loaded JSON with {len(doors_df)} rows")
            else:
                raise ValueError("Unsupported file type")

            # Debug: Show the first few rows of the input data
            self._dbg(f"\n📊 Input data preview:")
            self._dbg(f"Columns: {', '.join(doors_df.columns)}")
            self._dbg(f"First 5 rows:\n{doors_df.head().to_string()}")

            accts = doors_df["Compte client"].dropna().astype(str).tolist()
            if not accts:
                self._dbg("❌ No accounts found in input file")
                self.gui_q.put(
                    ("error", "Le fichier ne contient aucun « Compte client ».")
                )
                return

            self._dbg(f"\n📋 Found {len(accts)} accounts to process")
            self._dbg(f"First 5 accounts: {accts[:5]}")

            # split ⇢ Clic+ vs CSR
            clic_accts = [a for a in accts if len(_clean_acc(a)) <= 8]
            csr_accts = [a for a in accts if len(_clean_acc(a)) > 8]
            self._dbg(
                f"Split accounts - Clic+: {len(clic_accts)}, CSR: {len(csr_accts)}"
            )
            if clic_accts:
                self._dbg(f"First 5 Clic+ accounts: {clic_accts[:5]}")
            if csr_accts:
                self._dbg(f"First 5 CSR accounts: {csr_accts[:5]}")

            self.driver = build_driver()
            self._dbg("✓ Initialized Chrome driver")

            # ── 1) scrape Clic+ ------------------------------------------------
            if clic_accts:
                self._dbg("\n🔄 Starting Clic+ scraping")
                self._login_and_ready()
                for idx, acc in enumerate(clic_accts, 1):
                    if self._stop_evt.is_set():
                        break
                    while self.pause_evt.is_set():
                        time.sleep(0.3)
                    info = self._scrape_one(acc)
                    if info:
                        self.rows.append(info)
                        self._dbg(f"✓ Added Clic+ data for account {acc}")
                        self._dbg(f"  • Phone: {info.get('Téléphone', 'N/A')}")
                        self._dbg(f"  • Email: {info.get('Courriel', 'N/A')}")
                    else:
                        self._dbg(f"❌ Failed to get Clic+ data for account {acc}")
                    self.gui_q.put(("detail_progress", idx, len(accts)))

            # ── 2) scrape CSR --------------------------------------------------
            if csr_accts:
                self._dbg("\n🔄 Starting CSR scraping")
                for idx, acc in enumerate(csr_accts, len(clic_accts) + 1):
                    if self._stop_evt.is_set():
                        break
                    while self.pause_evt.is_set():
                        time.sleep(0.3)
                    info = self._scrape_csr(acc)
                    if info:
                        self.rows.append(info)
                        self._dbg(f"✓ Added CSR data for account {acc}")
                        self._dbg(f"  • Phone: {info.get('Téléphone', 'N/A')}")
                        self._dbg(f"  • Email: {info.get('Courriel', 'N/A')}")
                    else:
                        self._dbg(f"❌ Failed to get CSR data for account {acc}")
                    self.gui_q.put(("detail_progress", idx, len(accts)))

            # ── 3) merge phones/emails back into doors_df ---------------------
            if not self.rows:
                self._dbg("❌ No results found during scraping")
                return

            self._dbg(f"\n🔄 Starting merge process with {len(self.rows)} results")
            specs_df = pd.DataFrame(self.rows)

            # Debug: Show the specs data
            self._dbg(f"\n📊 Specs data preview:")
            self._dbg(f"Columns: {', '.join(specs_df.columns)}")
            self._dbg(f"First 5 rows:\n{specs_df.head().to_string()}")

            # Normalize account numbers for matching
            doors_df["acct_digits"] = doors_df["Compte client"].str.replace(
                r"\D", "", regex=True
            )
            specs_df["acct_digits"] = specs_df["Compte client"].str.replace(
                r"\D", "", regex=True
            )
            self._dbg("✓ Normalized account numbers for matching")

            # Debug: Show the normalized account numbers
            self._dbg(f"\n📊 Normalized account numbers:")
            self._dbg(f"Doors accounts: {doors_df['acct_digits'].head().tolist()}")
            self._dbg(f"Specs accounts: {specs_df['acct_digits'].head().tolist()}")

            # Ensure we have the required columns
            if "Téléphone" not in specs_df.columns:
                specs_df["Téléphone"] = "N/A"
                self._dbg("⚠ Added missing Téléphone column")
            if "Courriel" not in specs_df.columns:
                specs_df["Courriel"] = "N/A"
                self._dbg("⚠ Added missing Courriel column")

            # Merge with better handling of duplicates
            merged = pd.merge(
                doors_df,
                specs_df[["acct_digits", "Téléphone", "Courriel"]],
                on="acct_digits",
                how="left",
                validate="many_to_one",
            )
            self._dbg(f"✓ Merged data - {len(merged)} rows")

            # Debug: Show the merged data
            self._dbg(f"\n📊 Merged data preview:")
            self._dbg(f"Columns: {', '.join(merged.columns)}")
            self._dbg(f"First 5 rows:\n{merged.head().to_string()}")

            # Fill missing values with N/A
            merged["Téléphone"] = merged["Téléphone"].fillna("N/A")
            merged["Courriel"] = merged["Courriel"].fillna("N/A")
            self._dbg("✓ Filled missing values with N/A")

            # optional: report still-missing numbers
            miss = merged[merged["Téléphone"] == "N/A"]
            if not miss.empty:
                try:
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    miss_path = self.dest_dir / f"missing_after_merge_{ts}.csv"
                    miss.to_csv(miss_path, index=False, encoding="utf-8")
                    self._dbg(
                        f"⚠ {len(miss)} accounts missing phone numbers → {miss_path.name}"
                    )
                except Exception as e:
                    self._dbg(f"⚠ Could not save missing accounts file: {e}")
                    self._dbg(
                        f"⚠ {len(miss)} accounts missing phone numbers (file not saved)"
                    )
            else:
                self._dbg("✓ All accounts have phone numbers!")

            # ── 4) build the 8-column template --------------------------------
            get = lambda col: merged[col] if col in merged.columns else ""
            output = pd.DataFrame(
                {
                    "ADRESSE": get("Résidence"),
                    "CLIENT": get("Client"),
                    "NUMÉRO DE TÉLÉPHONE": merged["Téléphone"],
                    "COURRIEL": merged["Courriel"],
                    "NUMÉRO DE COMPTE": merged["Compte client"],
                    "SERVICES ACTUELS": get("Services actuels"),
                    "DERNIER STATUT": get("Dernier statut"),
                    "SERVICE AVANT DEBRANCHEMENT": get("Services avant débranchement"),
                }
            )
            self._dbg("✓ Built output template")

            # Debug: Show the final output data
            self._dbg(f"\n📊 Final output preview:")
            self._dbg(f"Columns: {', '.join(output.columns)}")
            self._dbg(f"First 5 rows:\n{output.head().to_string()}")

            # ── 5) export ------------------------------------------------------
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            prefix = _slug(self.path.stem.replace("doors_", ""))
            out_xlsx = self.dest_dir / f"specifics_{prefix}_{ts}.xlsx"

            self._dbg(f"\n💾 Exporting to Excel: {out_xlsx}")
            with pd.ExcelWriter(out_xlsx, engine="openpyxl") as wr:
                output.to_excel(wr, index=False)
            self._dbg(f"✓ Successfully exported to Excel")

            self.gui_q.put(("detail_done", str(out_xlsx), len(output)))
            open_folder(self.dest_dir)
            self._dbg("✓ Process complete!")

        except Exception as e:
            self._dbg("ERROR:\n" + traceback.format_exc())

            # ALWAYS try to generate the Excel file even if there's an error
            try:
                self._dbg("⚠ Attempting to save results with template format...")

                # Read the original doors file again
                if self.path.suffix.lower() == ".csv":
                    doors_df = pd.read_csv(
                        self.path, encoding="utf-8", dtype={"Compte client": str}
                    )
                else:
                    doors_df = pd.DataFrame(json.loads(self.path.read_text("utf-8")))
                    doors_df["Compte client"] = doors_df["Compte client"].astype(str)

                # If we have any scraped data, merge it
                if hasattr(self, "rows") and self.rows:
                    specs_df = pd.DataFrame(self.rows)

                    # Normalize account numbers for matching
                    doors_df["acct_digits"] = doors_df["Compte client"].str.replace(
                        r"\D", "", regex=True
                    )
                    specs_df["acct_digits"] = specs_df["Compte client"].str.replace(
                        r"\D", "", regex=True
                    )

                    # Ensure required columns exist
                    if "Téléphone" not in specs_df.columns:
                        specs_df["Téléphone"] = "N/A"
                    if "Courriel" not in specs_df.columns:
                        specs_df["Courriel"] = "N/A"

                    # Merge the data
                    merged = pd.merge(
                        doors_df,
                        specs_df[["acct_digits", "Téléphone", "Courriel"]],
                        on="acct_digits",
                        how="left",
                    )

                    # Fill missing values
                    merged["Téléphone"] = merged["Téléphone"].fillna("N/A")
                    merged["Courriel"] = merged["Courriel"].fillna("N/A")
                else:
                    # No scraped data, use original doors data with N/A for phone/email
                    merged = doors_df.copy()
                    merged["Téléphone"] = "N/A"
                    merged["Courriel"] = "N/A"

                # Build the EXACT template format
                get = lambda col: merged[col] if col in merged.columns else "N/A"
                output = pd.DataFrame(
                    {
                        "ADRESSE": get("Résidence"),
                        "CLIENT": get("Client"),
                        "NUMÉRO DE TÉLÉPHONE": merged["Téléphone"],
                        "COURRIEL": merged["Courriel"],
                        "NUMÉRO DE COMPTE": merged["Compte client"],
                        "SERVICES ACTUELS": get("Services actuels"),
                        "DERNIER STATUT": get("Dernier statut"),
                        "SERVICE AVANT DEBRANCHEMENT": get(
                            "Services avant débranchement"
                        ),
                    }
                )

                # Replace any remaining NaN with "N/A"
                output = output.fillna("N/A")

                ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                prefix = _slug(self.path.stem.replace("doors_", ""))
                out_xlsx = self.dest_dir / f"specifics_{prefix}_{ts}.xlsx"

                with pd.ExcelWriter(out_xlsx, engine="openpyxl") as wr:
                    output.to_excel(wr, index=False)

                scraped_count = (
                    len(self.rows) if hasattr(self, "rows") and self.rows else 0
                )
                self._dbg(f"✓ Saved template-formatted results to: {out_xlsx}")
                self._dbg(f"  • Total rows: {len(output)}")
                self._dbg(f"  • Successfully scraped: {scraped_count}")
                self._dbg(f"  • Missing data filled with N/A")

                self.gui_q.put(("detail_done", str(out_xlsx), len(output)))

            except Exception as save_error:
                self._dbg(f"❌ Failed to save results: {save_error}")
                self._dbg(f"❌ Full error: {traceback.format_exc()}")

            self.gui_q.put(("error", str(e)))

        finally:
            if self.driver:
                with contextlib.suppress(Exception):
                    self._dbg("Closing Chrome driver")
                    self.driver.quit()


# ── Thread Worker ───────────────────────────────────────────────────────
class SalesforceScraper(threading.Thread):
    LOGIN_URL = "https://v.my.site.com/resi/login"

    def __init__(
        self,
        user: str,
        pwd: str,
        city: str,
        street: Optional[str],
        rta: Optional[str],  # déjà présent
        gui_q: queue.Queue,
        pause_evt: threading.Event,
        dest_dir: Path,
    ):
        super().__init__(daemon=True)

        # ── mémorisation complète ───────────────────────────
        self.user = user
        self.pwd = pwd
        self.city = city
        self.street = street
        self.rta = rta  # ← AJOUT OBLIGATOIRE
        self.dest_dir = dest_dir
        # ----------------------------------------------------

        self.gui_q = gui_q
        self.pause_evt = pause_evt
        self._stop_evt = threading.Event()
        self.driver: Optional[uc.Chrome] = None
        self.doors: List[dict] = []
        self.curr_page = 0

        ts = datetime.utcnow().strftime("%Y%m%d")
        self.log_path = LOG_DIR / f"scraper_{ts}.log"

    # ---- helpers de log -------------------------------------------------
    def _dbg(self, msg: str):
        ts = datetime.now().strftime("[%H:%M:%S] ")
        # vers GUI
        self.gui_q.put(("log", ts + msg))
        # vers fichier
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(ts + msg + "\n")

    def _safe(self, label: str, func, *args, **kwargs):
        """Exécute func en loguant début/fin/erreur."""
        self._dbg(f"⇒ {label}…")
        try:
            res = func(*args, **kwargs)
            self._dbg(f"✔ {label} OK")
            return res
        except Exception as e:
            self._dbg(f"❌ {label} FAILED → {e}")
            raise

    # --------------------------------------------------------------------
    def stop(self):
        self._stop_evt.set()

    # --------------------------------------------------------------------
    def _login(self) -> bool:
        d = self.driver
        self._dbg("Nav → login page")
        d.get(self.LOGIN_URL)

        # username
        usr = self._safe("find #username", wait_visible, d, By.ID, "username")
        usr.clear()
        usr.send_keys(self.user)

        # password
        try:
            pwd = self._safe(
                "find name='pw'", wait_visible, d, By.NAME, "pw", timeout=10
            )
        except Exception:
            pwd = self._safe("find #password", wait_visible, d, By.ID, "password")
        pwd.clear()
        pwd.send_keys(self.pwd)

        # click
        self._safe("click #Login", wait_visible, d, By.ID, "Login").click()

        # attendre phSearchInput (MFA incluse)
        try:
            self._dbg("⏳ wait phSearchInput")
            wait_visible(d, By.ID, "phSearchInput", timeout=CFG["mfa_timeout_sec"])
            self._dbg("login complete")
            return True
        except TimeoutException:
            self._dbg("MFA timeout – pause")
            self.gui_q.put(("mfa_wait",))
            self.pause_evt.set()
            while self.pause_evt.is_set() and not self._stop_evt.is_set():
                time.sleep(0.5)
            self._dbg("⏳ resuming after MFA")
            # --- si l'onglet d'origine a été fermé par Salesforce -------------
            try:
                # simple ping : « donne-moi le titre »
                _ = d.title
            except Exception:
                self._dbg("⚠ session DevTools perdue – recherche onglet survivant")
                try:
                    # se raccrocher au dernier onglet encore ouvert
                    last = d.window_handles[-1]
                    d.switch_to.window(last)
                    self._dbg(f"✔ basculé sur handle {last}")
                except Exception as e:
                    self._dbg(f"❌ impossible de récupérer la session ({e})")
                    return False  # → run() attrapera et loguera l'erreur
            wait_visible(d, By.ID, "phSearchInput")
            return not self._stop_evt.is_set()

    # --------------------------------------------------------------------
    def _search_and_filter(self):
        d = self.driver
        query = (self.street or "").replace(" ", "-") or self.city
        self._dbg(f"search → {query}")

        # 1) Entrer la recherche
        inp = WebDriverWait(d, 15).until(
            EC.visibility_of_element_located((By.ID, "phSearchInput"))
        )
        inp.clear()
        inp.send_keys(query)
        d.find_element(By.ID, "phSearchButton").click()
        time.sleep(1)

        # 2) Cliquer sur "Afficher les filtres" si dispo
        try:
            filt_btn = WebDriverWait(d, 8).until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        "#showFiltersId-Residences__c-a0r, a.customizeColumns.filterFields",
                    )
                )
            )
            filt_btn.click()
            self._dbg("✔ filtre panel ouvert")
        except TimeoutException:
            self._dbg("⚠ pas de panneau filtres détecté (UI différente ?)")

        # 3) Sélectionner "Actif = Oui"
        try:
            # attendre la visibilité du select "Actif"
            # APRÈS  (By.ID => toujours valide)
            sel_elem = safe_find(d, "00Nd0000008BIlSEAWResidences__c", by=By.ID)
            # scroll into view pour éviter "not interactable"
            d.execute_script("arguments[0].scrollIntoView(true);", sel_elem)
            # utiliser Selenium-Select
            sel = Select(sel_elem)
            sel.select_by_visible_text("Oui")
            self._dbg("✔ Actif=Oui sélectionné")
        except TimeoutException:
            self._dbg("❌ champ Actif introuvable")
        except ElementNotInteractableException as e:
            self._dbg(f"⚠ champ Actif non interactable ({e}) — retry scroll+click")
            d.execute_script("arguments[0].scrollIntoView(true);", sel_elem)
            sel = Select(sel_elem)
            sel.select_by_visible_text("Oui")
            self._dbg("✔ retry Actif=Oui ok")

        # 4) RTA filter (optionnel)
        if self.rta:
            try:
                rta_input = WebDriverWait(d, 8).until(
                    EC.visibility_of_element_located(
                        (By.ID, "00Nd0000008B6CMEA0Residences__c")
                    )
                )
                d.execute_script("arguments[0].scrollIntoView(true);", rta_input)
                rta_input.clear()
                rta_input.send_keys(self.rta)
                self._dbg(f"✔ RTA={self.rta} appliqué")
            except Exception as e:
                self._dbg(f"❌ Impossible de saisir RTA={self.rta}: {e}")

        # 4‑bis) Remplir Ville + Rue si présents -------------------------------
        try:
            # Champ VILLE – toujours renseigné
            city_inp = safe_find(
                d, "00Nd0000008ASBbEAOResidences__c", by=By.ID  # ← id du champ Ville
            )
            d.execute_script("arguments[0].scrollIntoView(true);", city_inp)
            city_inp.clear()
            city_inp.send_keys(self.city)
            self._dbg(f"✔ Ville={self.city} appliquée")

            # Champ RUE – seulement si self.street
            if self.street:
                street_inp = safe_find(
                    d, "00Nd0000008B6ClEAKResidences__c", by=By.ID  # ← id du champ Rue
                )
                d.execute_script("arguments[0].scrollIntoView(true);", street_inp)
                street_inp.clear()
                street_inp.send_keys(self.street)
                self._dbg(f"✔ Rue={self.street} appliquée")

        except Exception as e:
            self._dbg(f"❌ Impossible de saisir ville/rue : {e}")

        # 5) Cliquer sur Appliquer les filtres
        try:
            apply_btn = WebDriverWait(d, 15).until(
                EC.element_to_be_clickable((By.ID, "save_filter_Residences__c"))
            )
            d.execute_script("arguments[0].scrollIntoView(true);", apply_btn)
            apply_btn.click()
            self._dbg("✔ filtres appliqués (ID)")
        except TimeoutException:
            self._dbg("⚠ bouton apply non trouvé par ID, tentative XPath…")
            try:
                apply_btn = WebDriverWait(d, 5).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, "//input[@value='Appliquer les filtres']")
                    )
                )
                d.execute_script("arguments[0].scrollIntoView(true);", apply_btn)
                apply_btn.click()
                self._dbg("✔ filtres appliqués (XPath)")
            except Exception as e2:
                self._dbg(f"❌ échec apply fallback XPath ({e2})")
                raise

        time.sleep(5)
        # 5) Attendre la table des résultats
        WebDriverWait(d, 15).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "table.list"))
        )
        self._dbg("✔ table.list visible, prêt à parser")

    # ─── telebot/handlers/salesforce_scraper.py  (ou le fichier équivalent) ────
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    def _scrape_door(self, href: str) -> dict | None:
        """
        1. Ouvre <href> dans un NOUVEL onglet (fini les filtres qui sautent).
        2. Parse exactement comme avant avec safe_find + <td> pairs.
        3. Ferme l'onglet et revient sur la liste.
        """
        if self._stop_evt.is_set():
            return None

        d = self.driver
        wait = WebDriverWait(d, 5)
        main = d.current_window_handle

        # ── 1) nouvel onglet vierge
        d.switch_to.new_window("tab")
        d.get(href)

        try:
            # ── 2) parsing "ancien style" — on ne change rien
            tbl = safe_find(d, "#ep table.detailList")  # votre helper existant
            tds = [
                td.text.strip() or None for td in tbl.find_elements(By.TAG_NAME, "td")
            ]

            rec = {
                tds[i]: tds[i + 1]
                for i in range(0, len(tds), 2)
                if i + 1 < len(tds) and tds[i]
            }

            # ← NEW: drop any "fizz" clients
            client_val = rec.get("Client", "") or rec.get("Compte client", "")
            if "fizz" in client_val.lower():
                self._dbg(f"⚠ Skipping fizz client: {client_val}")
                return None

            self._dbg(f"✓ parsed {len(rec)} fields")
            return rec

        except Exception as e:
            self._dbg(f"❌ detail fail ({e})")
            return None

        finally:
            # ── 3) nettoyage
            try:
                d.close()  # referme l'onglet détail
            finally:
                d.switch_to.window(main)  # retourne à la liste
                # petit wait pour garantir que la table est de nouveau interactive
                wait.until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, "table.list tr.dataRow")
                    )
                )

    # ---- main thread method --------------------------------------------
    def run(self):
        try:
            self.driver = build_driver()
            if not self._login():
                return
            self._search_and_filter()

            page_no = 0
            total_pages = None
            more = True

            while more and not self._stop_evt.is_set():
                page_no += 1

                # ── (1) info plage "x‑y" ─────────────────────────────────────
                try:
                    range_text = (
                        WebDriverWait(self.driver, 5)
                        .until(
                            EC.visibility_of_element_located(
                                (By.CSS_SELECTOR, ".itemsRange")
                            )
                        )
                        .text
                    )  # ex. "(1-25)"
                except TimeoutException:
                    range_text = "(?)"

                self._dbg(f"=== PAGE {page_no} {range_text} ===")

                # ── (2) collecter tous les liens de la page ──────────────────
                links = [
                    a.get_attribute("href")
                    for a in self.driver.find_elements(
                        By.CSS_SELECTOR, "table.list tr.dataRow th a"
                    )
                ]
                if not links:  # aucune ligne => on s'arrête
                    self._dbg("🚨 aucun enregistrement trouvé, arrêt boucle")
                    break

                import re

                for href in links:
                    rec = self._scrape_door(href)
                    if not rec:
                        continue

                    # grab the raw "Compte client" value (fall back to empty string)
                    acct = rec.get("Compte client", "")
                    # strip out non‐digits and count
                    digits = re.sub(r"\D", "", acct)

                    self.doors.append(rec)

                # ── (3) progression GUI ─────────────────────────────────────
                pct = None if total_pages is None else page_no / total_pages
                self.gui_q.put(("progress", page_no, range_text, len(self.doors), pct))

                from selenium.common.exceptions import \
                    StaleElementReferenceException

                # --- (4) tenter d'avancer ------------------------------------------------
                try:
                    # garder une référence au tableau courant
                    old_tbl = self.driver.find_element(By.CSS_SELECTOR, "table.list")

                    # rendre le footer visible + récupérer le bouton flèche
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);"
                    )
                    nxt_img = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, ".pSearchShowMore a.nextArrow > img")
                        )
                    )

                    # dernière page ?
                    if "disabled" in nxt_img.get_attribute("src"):
                        total_pages = page_no
                        more = False
                    else:
                        self._dbg("click Page suivante")
                        self.driver.execute_script(
                            "arguments[0].parentElement.click()", nxt_img
                        )

                        # ❶ attendre que l'ancien tableau devienne obsolète,
                        #    puis ❷ attendre que le nouveau soit prêt
                        WebDriverWait(self.driver, 10).until(EC.staleness_of(old_tbl))
                        WebDriverWait(self.driver, 15).until(
                            EC.visibility_of_element_located(
                                (By.CSS_SELECTOR, "table.list tr.dataRow")
                            )
                        )

                except StaleElementReferenceException as e:
                    self._dbg(f"stale element récupéré → retry ({e})")
                    continue  # relance immédiatement la boucle while

                except Exception as e:
                    self._dbg(f"no next page ({e})")
                    total_pages = page_no
                    more = False

            # ── (5) EXPORTS ─────────────────────────────────────────────
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            parts = [_slug(self.city)]
            if self.street:
                parts.append(_slug(self.street))
            if self.rta:
                parts.append(_slug(self.rta))
            prefix = "_".join(parts)

            # --- 1️⃣ chemins DE DESTINATION directement dans le dossier choisi
            out_json = self.dest_dir / f"doors_{prefix}_{ts}.json"
            out_csv = self.dest_dir / f"doors_{prefix}_{ts}.csv"

            # ① JSON (toujours, même vide)
            out_json.write_text(
                json.dumps(self.doors, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            # ② CSV
            if self.doors:
                all_keys = set().union(*(rec.keys() for rec in self.doors))
                header = ["city", "street", "rta"] + sorted(all_keys)
                with out_csv.open("w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(header)
                    for rec in self.doors:
                        w.writerow(
                            [
                                self.city,
                                self.street or "",
                                self.rta or "",
                                *[rec.get(k, "") for k in sorted(all_keys)],
                            ]
                        )
            else:  # aucune porte
                with out_csv.open("w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(["city", "street", "rta"])

            # ③ notification GUI + ouverture du dossier
            self.gui_q.put(("done", str(out_json), str(out_csv), len(self.doors)))
            open_folder(self.dest_dir)
            return  # ← il ne faut plus rien après

            # même si aucune porte n'a été trouvée, écrire un fichier JSON vide
            json_path.write_text(
                json.dumps(self.doors, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            # ➊ si aucune fiche => CSV minimal + message GUI puis retour
            if not self.doors:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(["city", "street", "rta"])  # en‑tête simple
                self.gui_q.put(("done", str(json_path), str(csv_path), 0))
                return

            # ➋ construire l'ensemble complet des champs rencontrés
            all_keys = set().union(*(rec.keys() for rec in self.doors))
            header = ["city", "street", "rta"] + sorted(all_keys)

            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(header)
                for rec in self.doors:
                    w.writerow(
                        [
                            self.city,
                            self.street or "",
                            self.rta or "",
                            *[rec.get(k, "") for k in sorted(all_keys)],
                        ]
                    )

            self.gui_q.put(("done", str(json_path), str(csv_path), len(self.doors)))

        except Exception as e:
            self._dbg(f"FATAL ERROR {e}")
            self.gui_q.put(("error", str(e)))
        finally:
            # — EXPORT inconditionnel -----------------------------------------
            try:
                if self.doors:
                    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                    parts = [_slug(self.city)]
                    if self.street:
                        parts.append(_slug(self.street))
                    if self.rta:
                        parts.append(_slug(self.rta))
                    prefix = "_".join(parts)

                    out_json = self.dest_dir / f"doors_{prefix}_{ts}.json"
                    out_csv = self.dest_dir / f"doors_{prefix}_{ts}.csv"

                    out_json.write_text(
                        json.dumps(self.doors, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )

                    all_keys = set().union(*(rec.keys() for rec in self.doors))
                    header = ["city", "street", "rta"] + sorted(all_keys)
                    with out_csv.open("w", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        w.writerow(header)
                        for rec in self.doors:
                            w.writerow(
                                [
                                    self.city,
                                    self.street or "",
                                    self.rta or "",
                                    *[rec.get(k, "") for k in sorted(all_keys)],
                                ]
                            )

                    self.gui_q.put(
                        ("done", str(out_json), str(out_csv), len(self.doors))
                    )
                else:
                    self._dbg("aucune porte collectée — rien à exporter")
            except Exception as exp:
                self._dbg(f"❌ export final failed : {exp}")
            finally:
                if self.driver:
                    with contextlib.suppress(Exception):
                        self._dbg("Quitting Chrome")
                        self.driver.quit()


# ── Interface graphique ─────────────────────────────────────────────────
class ScraperGUI:
    def __init__(self):
        # Apparence
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.root = ctk.CTk()
        self.root.title("Salesforce Door-Scraper")
        self.root.geometry("980x640")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # État interne
        self.gui_q = queue.Queue()
        self.pause_evt = threading.Event()
        self.worker: Optional[SalesforceScraper] = None
        self.city2rel: Dict[str, int] = {}
        self.city2streets: Dict[str, List[str]] = {}

        # Variables liées à l'UI
        self.user_var = tk.StringVar()
        self.pwd_var = tk.StringVar()
        self.user_var = tk.StringVar(value="othmane.elfathi@videotron.com")
        self.pwd_var = tk.StringVar(value="Brick2025$")
        self.clic_user_var = tk.StringVar()
        self.clic_pwd_var = tk.StringVar()
        self.clic_user_var = tk.StringVar(value="elfathio")
        self.clic_pwd_var = tk.StringVar(value="Videotron2025$")
        self.city_var = tk.StringVar()
        self.street_var = tk.StringVar()
        self.rta_var = tk.StringVar()
        self.employee_code = tk.StringVar()
        self.employee_code = tk.StringVar(value="20459")

        # Traces pour filtrage dynamique
        self.city_var.trace_add("write", self._filter_cities)
        self.street_var.trace_add("write", self._filter_streets)

        # Construction
        self._build_widgets()
        self._load_or_fetch_cities()

        # Boucle de polling des messages du thread
        self.root.after(100, self._poll_queue)
        self.root.mainloop()

    def _log(self, txt: str):
        """Enhanced logging with timestamp and auto-scroll"""
        ts = datetime.now().strftime("[%H:%M:%S] ")
        self.log.configure(state="normal")
        self.log.insert("end", ts + txt + "\n")
        self.log.see("end")  # Auto-scroll to bottom
        self.log.configure(state="disabled")
        # Force update of the GUI
        self.root.update_idletasks()

    def _build_widgets(self):
        pad = {"padx": 8, "pady": 3}

        # — Salesforce Identifiants —
        cred_f = ctk.CTkFrame(self.root)
        cred_f.pack(pady=8, fill="x")
        for c in range(4):
            cred_f.grid_columnconfigure(c, weight=1)
        ctk.CTkLabel(cred_f, text="SF Username:").grid(
            row=0, column=0, **pad, sticky="e"
        )
        ctk.CTkEntry(cred_f, textvariable=self.user_var).grid(
            row=0, column=1, **pad, sticky="w"
        )
        ctk.CTkLabel(cred_f, text="SF Password:").grid(
            row=0, column=2, **pad, sticky="e"
        )
        ctk.CTkEntry(cred_f, textvariable=self.pwd_var, show="*").grid(
            row=0, column=3, **pad, sticky="w"
        )

        # — Clic+ Identifiants —
        clic_f = ctk.CTkFrame(self.root)
        clic_f.pack(pady=4, fill="x")
        for c in range(4):
            clic_f.grid_columnconfigure(c, weight=1)
        ctk.CTkLabel(clic_f, text="Clic+ User:").grid(
            row=0, column=0, **pad, sticky="e"
        )
        ctk.CTkEntry(clic_f, textvariable=self.clic_user_var).grid(
            row=0, column=1, **pad, sticky="w"
        )
        ctk.CTkLabel(clic_f, text="Clic+ Pass:").grid(
            row=0, column=2, **pad, sticky="e"
        )
        ctk.CTkEntry(clic_f, textvariable=self.clic_pwd_var, show="*").grid(
            row=0, column=3, **pad, sticky="w"
        )

        # — Sélecteurs Ville / Rue / RTA —
        sel_f = ctk.CTkFrame(self.root)
        sel_f.pack(pady=8, fill="x")
        for c in range(6):
            sel_f.grid_columnconfigure(c, weight=1)

        ctk.CTkLabel(sel_f, text="City:").grid(row=0, column=0, **pad, sticky="e")
        self.city_cb = ctk.CTkComboBox(
            sel_f,
            variable=self.city_var,
            values=[],
            state="normal",
            width=200,
            command=self._on_city,
        )
        self.city_cb.grid(row=0, column=1, **pad, sticky="w")

        ctk.CTkLabel(sel_f, text="Street (opt.):").grid(
            row=0, column=2, **pad, sticky="e"
        )
        self.street_cb = ctk.CTkComboBox(
            sel_f, variable=self.street_var, values=[], state="normal", width=200
        )
        self.street_cb.grid(row=0, column=3, **pad, sticky="w")

        ctk.CTkLabel(sel_f, text="RTA (opt.):").grid(row=0, column=4, **pad, sticky="e")
        ctk.CTkEntry(
            sel_f,
            textvariable=self.rta_var,
            placeholder_text="e.g. 75261762",
            width=100,
        ).grid(row=0, column=5, **pad, sticky="w")

        ctk.CTkLabel(sel_f, text="Employee code:").grid(
            row=0, column=6, **pad, sticky="e"
        )
        ctk.CTkEntry(
            sel_f,
            textvariable=self.employee_code,
            placeholder_text="e.g. 75261",
            width=100,
        ).grid(row=0, column=7, **pad, sticky="w")

        # — Boutons Start / Specifics / Pause / Stop —
        btn_f = ctk.CTkFrame(self.root)
        btn_f.pack(pady=8)
        self.full_btn = ctk.CTkButton(
            btn_f, text="✅ Full Completion", width=160, command=self._full_completion
        )
        self.get_doors_btn = ctk.CTkButton(
            btn_f, text="▶ Get Doors", width=160, command=self._start
        )
        self.get_numbers_btn = ctk.CTkButton(
            btn_f,
            text="📄 Get Numbers",
            width=160,
            state="normal",
            command=self._start_details,
        )
        self.pause_btn = ctk.CTkButton(
            btn_f, text="Pause", width=160, state="disabled", command=self._toggle_pause
        )
        self.stop_btn = ctk.CTkButton(
            btn_f, text="■ Stop", width=160, state="disabled", command=self._stop_worker
        )
        self.full_btn.grid(row=0, column=0, padx=6)
        self.get_doors_btn.grid(row=0, column=1, padx=6)
        self.get_numbers_btn.grid(row=0, column=2, padx=6)
        self.pause_btn.grid(row=0, column=3, padx=6)
        self.stop_btn.grid(row=0, column=4, padx=6)

        # — Progression & stats —
        self.prog = ctk.CTkProgressBar(self.root, width=780)
        self.prog.set(0)
        self.prog.pack(pady=6)
        stats_f = ctk.CTkFrame(self.root)
        stats_f.pack(pady=4)
        self.page_lbl = ctk.CTkLabel(stats_f, text="Page: 0")
        self.door_lbl = ctk.CTkLabel(stats_f, text="Doors: 0")
        self.page_lbl.grid(row=0, column=0, padx=10)
        self.door_lbl.grid(row=0, column=1, padx=10)

        # — Log console —
        self.log = ctk.CTkTextbox(self.root, width=800, height=340, wrap="none")
        self.log.configure(state="disabled")
        self.log.pack(pady=8)

    def _full_completion(self):
        """
        1) Ask for a folder.
        2) Run the doors‐scraper into that folder.
        3) When it finishes, automatically run the numbers‐scraper
           against the newly created doors_*.csv (or .json) in that folder.
        """
        # pick a folder
        dst = fd.askdirectory(
            title="Choose destination for doors + numbers", initialdir=downloads_dir()
        )
        if not dst:
            return

        # clear any old state
        self.destination_folder = Path(dst)
        self._log(f"▶ Starting Full Completion process in {dst}")
        self._log("Step 1: Getting doors from Salesforce...")

        # step 1: run the SalesforceScraper
        self.pause_evt.clear()
        self.worker = SalesforceScraper(
            self.user_var.get().strip(),
            self.pwd_var.get().strip(),
            self.city_var.get().strip(),
            self.street_var.get().strip().upper() or None,
            self.rta_var.get().strip() or None,
            self.gui_q,
            self.pause_evt,
            dest_dir=self.destination_folder,
        )

        if self.worker and self.worker.is_alive():
            messagebox.showwarning("Running", "Scraper already running.")
            return
        user, pwd = self.user_var.get().strip(), self.pwd_var.get().strip()
        if not user or not pwd:
            messagebox.showerror("Error", "Entrez nom d'utilisateur et mot de passe.")
            return
        city = self.city_var.get().strip()
        if not city:
            messagebox.showerror("Error", "Sélectionnez une ville d'abord.")
            return
        street = self.street_var.get().strip().upper() or None
        rta = self.rta_var.get().strip() or None

        self.worker.start()

        # disable buttons until both finish
        self.get_doors_btn.configure(state="disabled")
        self.get_numbers_btn.configure(state="disabled")
        self.full_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal")
        self.stop_btn.configure(state="normal")

        # tell _poll_queue that we're in full‐mode
        self.full_mode = True

    def _load_or_fetch_cities(self):
        self.city2rel = fetch_or_load_cities(CITIES_CACHE)
        if self.city2rel:
            self._populate_cities()
        else:
            threading.Thread(target=self._thread_fetch_cities, daemon=True).start()

    def _thread_fetch_cities(self):
        self._log("⏳ Téléchargement des municipalités…")
        try:
            self.city2rel = fetch_all_cities()
            save_cities_cache(CITIES_CACHE, self.city2rel)
            self._log(f"✅ {len(self.city2rel):,} villes chargées")
        except Exception as e:
            self._log(f"❌ Échec fetch villes: {e}")
            messagebox.showerror("Error", f"Impossible de récupérer les villes:\n{e}")
        self.root.after(0, self._populate_cities)

    def _populate_cities(self):
        vals = sorted(self.city2rel.keys())
        self.city_cb.configure(values=vals)

    def _on_city(self, *_):
        city = self.city_var.get()
        self.street_cb.set("")
        self.street_cb.configure(values=[], state="disabled")
        if city in self.city2streets:
            self.street_cb.configure(values=self.city2streets[city], state="normal")
        else:
            threading.Thread(
                target=self._thread_fetch_streets, args=(city,), daemon=True
            ).start()

    def _thread_fetch_streets(self, city: str):
        self._log(f"⏳ Récupération rues de {city}…")
        try:
            rel = self.city2rel[city]
            sts = fetch_streets_for_city(rel)
            self.city2streets[city] = sts
            self._log(f"✅ {len(sts):,} rues chargées")
        except Exception as e:
            self._log(f"❌ Échec rues: {e}")
            sts = []
        self.root.after(0, lambda: self.street_cb.configure(values=sts, state="normal"))

    def _filter_cities(self, *args):
        txt = self.city_var.get().lower()
        vals = [c for c in self.city2rel if txt in c.lower()]
        self.city_cb.configure(values=sorted(vals))

    def _filter_streets(self, *args):
        city = self.city_var.get()
        all_sts = self.city2streets.get(city, [])
        txt = self.street_var.get().lower()
        vals = [s for s in all_sts if txt in s.lower()]
        self.street_cb.configure(values=sorted(vals))

    def _start_details(self):
        doors_fp = filedialog.askopenfilename(
            title="Choose a doors file",
            initialdir=DATA_DIR,
            filetypes=[
                ("Doors exports", "doors_*.json doors_*.csv"),
                ("All files", "*.*"),
            ],
        )
        if not doors_fp:
            return

        dst_dir = fd.askdirectory(
            title="Choisir le dossier de destination", initialdir=downloads_dir()
        )
        if not dst_dir:
            return

        # Reset UI
        self.prog.set(0)
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

        self.detail_scraper = ClicDetailScraper(
            Path(doors_fp),
            self.gui_q,
            self.pause_evt,
            dest_dir=Path(dst_dir),
            clic_user=self.clic_user_var.get().strip(),
            clic_pwd=self.clic_pwd_var.get().strip(),
            csr_code=self.employee_code.get().strip(),
        )
        self._log(f"▶ Starting numbers scraping from {doors_fp}")
        self.detail_scraper.start()
        self.pause_btn.configure(state="normal")
        self.stop_btn.configure(state="normal")

    def _start(self):
        if self.worker and self.worker.is_alive():
            messagebox.showwarning("Running", "Scraper already running.")
            return
        user, pwd = self.user_var.get().strip(), self.pwd_var.get().strip()
        if not user or not pwd:
            messagebox.showerror("Error", "Entrez nom d'utilisateur et mot de passe.")
            return
        city = self.city_var.get().strip()
        if not city:
            messagebox.showerror("Error", "Sélectionnez une ville d'abord.")
            return
        street = self.street_var.get().strip().upper() or None
        rta = self.rta_var.get().strip() or None

        dst_dir = fd.askdirectory(
            title="Choisir le dossier de destination", initialdir=downloads_dir()
        )
        if not dst_dir:
            return  # utilisateur a cancel
        self.dest_dir = Path(dst_dir)

        # Reset UI
        self.page_lbl.configure(text="Page: 0")
        self.door_lbl.configure(text="Doors: 0")
        self.prog.set(0)
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

        # Lancer le thread
        self.pause_evt.clear()
        self.worker = SalesforceScraper(
            user,
            pwd,
            city,
            street,
            rta,
            self.gui_q,
            self.pause_evt,
            dest_dir=self.dest_dir,
        )
        self._log(
            f"▶ Starting doors scraping for {city}" + (f" - {street}" if street else "")
        )
        self.worker.start()

        self.get_doors_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal")
        self.stop_btn.configure(state="normal")

    def _toggle_pause(self):
        if not self.worker:
            return
        if self.pause_evt.is_set():
            self.pause_evt.clear()
            self.pause_btn.configure(text="Pause")
            self._log("▶ Resuming...")
        else:
            self.pause_evt.set()
            self.pause_btn.configure(text="Resume")
            self._log("⏸ Paused")

    def _stop_worker(self):
        if self.worker and self.worker.is_alive():
            self.worker.stop()
        if hasattr(self, "detail_scraper") and self.detail_scraper.is_alive():
            self.detail_scraper.stop()
        self._log("⏹ Stopping...")
        self.pause_evt.clear()

    def _poll_queue(self):
        try:
            while True:
                tag, *payload = self.gui_q.get_nowait()

                if tag == "log":
                    # Direct log message from worker
                    self._log(payload[0])
                elif tag == "progress":
                    page_no, range_text, doors, pct = payload
                    self.page_lbl.configure(text=f"Page: {page_no} {range_text}")
                    self.door_lbl.configure(text=f"Doors: {doors}")
                    if pct is not None:
                        self.prog.set(pct)
                elif tag == "detail_progress":
                    idx, total = payload
                    pct = idx / total
                    self.prog.set(pct)
                    self._log(f"Progress: {idx}/{total} accounts processed ({pct:.1%})")
                elif tag == "done":
                    json_path, csv_path, cnt = payload
                    self._log(f"✓ Doors scraping complete: {cnt:,} records")
                    self._log(f"  • JSON: {json_path}")
                    self._log(f"  • CSV: {csv_path}")
                    # if we're in full‐mode, immediately launch the numbers‐scraper:
                    if getattr(self, "full_mode", False):
                        # detect the doors file we just got:
                        doors_fp = Path(
                            csv_path if csv_path.endswith(".csv") else json_path
                        )
                        self._log(
                            f"\n▶ Starting Step 2: Getting numbers from {doors_fp.name}"
                        )
                        # fire up the ClicDetailScraper with the same dest folder
                        self.detail_scraper = ClicDetailScraper(
                            doors_fp,
                            self.gui_q,
                            self.pause_evt,
                            dest_dir=self.destination_folder,
                            clic_user=self.clic_user_var.get().strip(),
                            clic_pwd=self.clic_pwd_var.get().strip(),
                            csr_code=self.employee_code.get().strip(),
                        )
                        self.detail_scraper.start()
                        # clear the flag so we only chain once
                        self.full_mode = False
                    else:
                        # your normal single‐mode behavior:
                        self._reset_buttons()

                elif tag == "detail_done":
                    csv_path, nb = payload
                    self._log(f"\n✓ Numbers scraping complete: {nb} accounts processed")
                    self._log(f"  • Output: {csv_path}")
                    # full‐mode or normal, wrap up the UI:
                    self._reset_buttons()
                    messagebox.showinfo(
                        "Full Completion",
                        f"All done!\nDoors + Numbers in:\n{self.destination_folder}",
                    )
                elif tag == "error":
                    self._log(f"❌ ERROR: {payload[0]}")
                    messagebox.showerror("Error", payload[0])
                    self._reset_buttons()
                elif tag == "mfa_wait":
                    self._log("⏳ Waiting for MFA completion...")
                    messagebox.showinfo(
                        "MFA Required", "Please complete MFA in the browser window."
                    )

        except queue.Empty:
            pass

        self.root.after(150, self._poll_queue)

    def _reset_buttons(self):
        self.get_doors_btn.configure(state="normal")
        self.get_numbers_btn.configure(state="normal")
        self.full_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled")
        self.stop_btn.configure(state="disabled")
        self.prog.set(1)

    def _on_close(self):
        if self.worker and self.worker.is_alive():
            if messagebox.askyesno("Quit", "Scraper running. Stop and quit?"):
                self.worker.stop()
            else:
                return
        self.root.destroy()


# ─────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ScraperGUI()
