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
import contextlib, concurrent.futures as _fut
import json, pathlib, queue, random, threading, time
from datetime import datetime
from typing import Dict, List, Optional
import re
import unicodedata
from tkinter import filedialog
import csv

# ── Dépendances externes ─────────────────────────────────────────────────
import requests, undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException

import tkinter as tk
import customtkinter as ctk
from tkinter import messagebox      # même en CTk on garde pour le modal natif

import json
from pathlib import Path

from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException,
    ElementClickInterceptedException, NoSuchWindowException
)

# ── Chemins & configuration ─────────────────────────────────────────────
BASE_DIR      = pathlib.Path(__file__).resolve().parent
DATA_DIR      = BASE_DIR / "data"
LOG_DIR       = BASE_DIR / "logs"
CITIES_CACHE  = DATA_DIR / "qc_cities.json"
CONFIG_PATH   = BASE_DIR / "config.json"

DATA_DIR.mkdir(exist_ok=True), LOG_DIR.mkdir(exist_ok=True)

DEFAULT_CFG = {
    "max_parallel_tabs": 5,
    "mfa_timeout_sec":   60,
    "overpass_timeout":  120,
    "selenium_headless": False
}
CFG = {**DEFAULT_CFG, **json.loads(CONFIG_PATH.read_text())} if CONFIG_PATH.exists() else DEFAULT_CFG
CONFIG_PATH.write_text(json.dumps(CFG, indent=2))

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS      = {"User-Agent": "QC-Scraper/1.0"}

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
    # remplacer tout ce qui n’est pas [a-z0-9] par _
    txt = re.sub(r"[^a-zA-Z0-9]+", "_", txt).strip("_").lower()
    return re.sub(r"_{2,}", "_", txt)  # compacter ___

def load_cities_cache(path: Path) -> dict[str, int] | None:
    """
    Lit le cache JSON en UTF-8 et renvoie le dict city→rel_id,
    ou None s’il n’existe pas / n’est pas lisible.
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
    Sauvegarde le dict city→rel_id en JSON UTF-8, sans erreur d’encodage.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_or_load_cities(path: Path) -> dict[str, int]:
    """
    Essaie de charger le cache, sinon fetch_all_cities() et sauvegarde.
    """
    from __main__ import fetch_all_cities  # ou adapte l’import
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
        '[out:json][timeout:60];'
        'area["boundary"="administrative"]["admin_level"="4"]["name"="Québec"]->.prov;'
        'rel["boundary"="administrative"]["admin_level"="8"](area.prov);'
        'out tags;'
    )
    r = requests.post(OVERPASS_URL, data=query, headers=HEADERS, timeout=CFG["overpass_timeout"])
    r.raise_for_status()
    mapping = {elt["tags"]["name"]: elt["id"] for elt in r.json().get("elements", []) if elt.get("tags", {}).get("name")}
    return mapping

def fetch_streets_for_city(rel_id: int) -> List[str]:
    """Renvoie la liste triée des rues (way highway name) dans une municipalité."""
    area_id = 3600000000 + rel_id
    query = (
        '[out:json][timeout:120];'
        f'area({area_id})->.a;'
        '(way["highway"]["name"](area.a););'
        'out tags;'
    )
    r = requests.post(OVERPASS_URL, data=query, headers=HEADERS, timeout=CFG["overpass_timeout"]+60)
    r.raise_for_status()
    names = {elt["tags"]["name"].strip() for elt in r.json().get("elements", []) if "tags" in elt and "name" in elt["tags"]}
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
    opts.binary_location = str(BASE_DIR / "chrome" / "chrome.exe")

    driver = uc.Chrome(options=opts, version_main=136)
    driver.maximize_window()
    return driver

def wait_visible(drv, by, val, timeout=20):
    return WebDriverWait(drv, timeout).until(EC.visibility_of_element_located((by, val)))

def safe_find(drv, css_or_xpath: str, by=By.CSS_SELECTOR,
                timeout=15, retries=3, sleep_step=0.5):
    """
    Essaie de localiser un élément plusieurs fois avant d’échouer.
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
        except (StaleElementReferenceException,
                ElementClickInterceptedException,
                NoSuchWindowException):
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

    def __init__(self, doors_path: Path,
                 gui_q: queue.Queue, pause_evt: threading.Event):
        super().__init__(daemon=True)
        self.path      = doors_path
        self.gui_q     = gui_q
        self.pause_evt = pause_evt
        self.driver: Optional[uc.Chrome] = None
        self.rows: list[dict] = []

    # ---------- helpers --------------------------------------------------
    def _dbg(self, txt: str):
        self.gui_q.put(("log", txt))

    @staticmethod
    def _accounts_from_file(fp: Path) -> list[str]:
        if fp.suffix == ".json":
            data = json.loads(fp.read_text(encoding="utf-8"))
            return [d.get("Compte client") for d in data if d.get("Compte client")]
        else:  # csv
            with fp.open(encoding="utf-8") as f:
                rdr = csv.DictReader(f)
                return [row.get("Compte client") for row in rdr
                        if row.get("Compte client")]

    # ---------- selenium -------------------------------------------------
    def _wait(self, by, sel, to=20):
        return WebDriverWait(self.driver, to).until(
            EC.visibility_of_element_located((by, sel)))

    def _login_and_ready(self):
        d = self.driver
        d.get(self.URL)
        self._dbg("→ waiting Continuer…")
        self._wait(By.CSS_SELECTOR, "button.wrapper___2na1A").click()

        self._dbg("→ waiting Recherche icon…")
        self._wait(By.CSS_SELECTOR, ".search_wrapper___39tl7 a").click()

    def _scrape_one(self, acc: str) -> Optional[dict]:
        d = self.driver
        try:
            inp = self._wait(By.CSS_SELECTOR, "input[name='account.sgaAccountNumber']")
            inp.clear()
            inp.send_keys(acc)
            d.find_element(By.CSS_SELECTOR,
                "button.wrapper___2na1A[type='submit'] span").click()

            # attendre que le header se charge (contient le n° compte)
            self._wait(By.CSS_SELECTOR, ".header_container___mGxJS")

            # récupérer quelques champs :
            out: dict[str, str] = {"Compte client": acc}
            out["Adresse"] = d.find_element(
                By.CSS_SELECTOR, "[data-qa='clic__Address']").text
            out["Courriel"] = d.find_element(
                By.CSS_SELECTOR, "[data-qa='clic__Contact'] .email___ftlWz").text
            out["Téléphone"] = d.find_element(
                By.CSS_SELECTOR, "[data-qa='clic__Contact'] span").text
            out["Mensualité"] = d.find_element(
                By.XPATH, "//span[contains(text(),'Mensualité')]/../div/span").text
            return out
        except Exception as e:                       # compte peut être invalide
            self._dbg(f"⚠ compte {acc} : {e}")
            return None

    # ---------- thread main ---------------------------------------------
    def run(self):
        try:
            accts = self._accounts_from_file(self.path)
            if not accts:
                self.gui_q.put(("error", "Le fichier ne contient aucun « Compte client »."))
                return

            self.driver = build_driver()
            self._login_and_ready()

            for idx, acc in enumerate(accts, 1):
                if self.pause_evt.is_set():                     # pause gérée par le GUI
                    while self.pause_evt.is_set():
                        time.sleep(0.3)
                info = self._scrape_one(acc)
                if info:
                    self.rows.append(info)
                self.gui_q.put(("detail_progress", idx, len(accts)))

            # export CSV
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            prefix = _slug(self.path.stem.replace("doors_", ""))
            out = DATA_DIR / f"specifics_{prefix}_{ts}.csv"
            with out.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.rows[0].keys())
                w.writeheader()
                w.writerows(self.rows)

            self.gui_q.put(("detail_done", str(out), len(self.rows)))

        except Exception as e:
            self.gui_q.put(("error", str(e)))
        finally:
            if self.driver:
                with contextlib.suppress(Exception):
                    self.driver.quit()

# ── Thread Worker ───────────────────────────────────────────────────────
class SalesforceScraper(threading.Thread):
    LOGIN_URL = "https://v.my.site.com/resi/login"

    def __init__(
        self,
        user:   str,
        pwd:    str,
        city:   str,
        street: Optional[str],
        rta:    Optional[str],          # déjà présent
        gui_q:  queue.Queue,
        pause_evt: threading.Event
    ):
        super().__init__(daemon=True)

        # ── mémorisation complète ───────────────────────────
        self.user   = user
        self.pwd    = pwd
        self.city   = city
        self.street = street
        self.rta    = rta              # ← AJOUT OBLIGATOIRE
        # ----------------------------------------------------

        self.gui_q      = gui_q
        self.pause_evt  = pause_evt
        self._stop_evt  = threading.Event()
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
    def stop(self): self._stop_evt.set()

    # --------------------------------------------------------------------
    def _login(self) -> bool:
        d = self.driver
        self._dbg("Nav → login page")
        d.get(self.LOGIN_URL)

        # username
        usr = self._safe("find #username", wait_visible, d, By.ID, "username")
        usr.clear(); usr.send_keys(self.user)

        # password
        try:
            pwd = self._safe("find name='pw'", wait_visible, d, By.NAME, "pw", timeout=10)
        except Exception:
            pwd = self._safe("find #password", wait_visible, d, By.ID, "password")
        pwd.clear(); pwd.send_keys(self.pwd)

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

        # 2) Cliquer sur “Afficher les filtres” si dispo
        try:
            filt_btn = WebDriverWait(d, 8).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    '#showFiltersId-Residences__c-a0r, a.customizeColumns.filterFields'
                ))
            )
            filt_btn.click()
            self._dbg("✔ filtre panel ouvert")
        except TimeoutException:
            self._dbg("⚠ pas de panneau filtres détecté (UI différente ?)")

        # 3) Sélectionner “Actif = Oui”
        try:
            # attendre la visibilité du select “Actif”
            # APRÈS  (By.ID => toujours valide)
            sel_elem = safe_find(
                d,
                '00Nd0000008BIlSEAWResidences__c',
                by=By.ID
            )
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
                    EC.visibility_of_element_located((
                        By.ID, "00Nd0000008B6CMEA0Residences__c"
                    ))
                )
                d.execute_script("arguments[0].scrollIntoView(true);", rta_input)
                rta_input.clear()
                rta_input.send_keys(self.rta)
                self._dbg(f"✔ RTA={self.rta} appliqué")
            except Exception as e:
                self._dbg(f"❌ Impossible de saisir RTA={self.rta}: {e}")

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
                    EC.element_to_be_clickable((By.XPATH, "//input[@value='Appliquer les filtres']"))
                )
                d.execute_script("arguments[0].scrollIntoView(true);", apply_btn)
                apply_btn.click()
                self._dbg("✔ filtres appliqués (XPath)")
            except Exception as e2:
                self._dbg(f"❌ échec apply fallback XPath ({e2})")
                raise

        # 5) Attendre la table des résultats
        WebDriverWait(d, 15).until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, "table.list"))
        )
        self._dbg("✔ table.list visible, prêt à parser")

    # --------------------------------------------------------------------
    def _scrape_door(self, href: str):
        if self._stop_evt.is_set():
            return None

        d = self.driver
        self._dbg(f"→ open {href}")
        main = d.current_window_handle

        # Ouvre dans le même onglet (plus de fermeture d’onglet ⇒ pas de ‘no such window’)
        d.get(href)

        try:
            tbl = safe_find(d, "#ep table.detailList")
            tds = [td.text.strip() or None for td in tbl.find_elements(By.TAG_NAME, "td")]
            rec = {tds[i]: tds[i + 1] for i in range(0, len(tds), 2)
                if i + 1 < len(tds) and tds[i]}
            self._dbg(f"✓ parsed {len(rec)} fields")
            return rec

        except Exception as e:
            self._dbg(f"❌ detail fail ({e})")
            return None

        finally:
            # revient à la liste (flèche Retour) plutôt que gérer les fenêtres
            d.back()
            safe_find(d, "table.list")       # s’assurer que la liste est revenue

    # ---- main thread method --------------------------------------------
    def run(self):
        try:
            self.driver = build_driver()
            if not self._login():
                return
            self._search_and_filter()

            page_no     = 0          # page courante
            total_pages = None       # inconnu tant qu’on n’a pas vu la dernière

            more = True
            while more and not self._stop_evt.is_set():
                page_no += 1

                # ── range "(x-y)" ───────────────────────────────────────────
                try:
                    range_text = WebDriverWait(self.driver, 5).until(
                        EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, ".itemsRange"))
                    ).text            # ex : "(1-25)"
                except TimeoutException:
                    range_text = "(?)"

                self._dbg(f"=== PAGE {page_no} {range_text} ===")

                # ── liens de la page ────────────────────────────────────────
                links = [a.get_attribute("href") for a in
                        self.driver.find_elements(
                            By.CSS_SELECTOR, "table.list tr.dataRow th a")]

                for href in links:
                    rec = self._scrape_door(href)
                    if rec:
                        self.doors.append(rec)

                # ── % d’avancement ─────────────────────────────────────────
                pct = None if total_pages is None else page_no / total_pages
                self.gui_q.put(("progress", page_no, range_text,
                                len(self.doors), pct))

                # ── page suivante ? ────────────────────────────────────────
                try:
                    # s’assurer que le footer est rendu
                    self.driver.execute_script(
                        "window.scrollTo(0, document.body.scrollHeight);")
                    nxt_img = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR,
                            ".pSearchShowMore a.nextArrow > img"))
                    )
                    if "disabled" in nxt_img.get_attribute("src"):
                        total_pages = page_no
                        more = False
                    else:
                        self._dbg("click Page suivante")
                        self.driver.execute_script(
                            "arguments[0].parentElement.click()", nxt_img)
                        WebDriverWait(self.driver, 15).until(
                            EC.visibility_of_element_located(
                                (By.CSS_SELECTOR, "table.list")))
                except Exception as e:
                    self._dbg(f"no next page ({e})")
                    total_pages = page_no
                    more = False

            # ── export JSON + CSV ──────────────────────────────────────────
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")

            # prefix = city[_street][_rta]
            parts = [_slug(self.city)]
            if self.street:
                parts.append(_slug(self.street))
            if self.rta:
                parts.append(_slug(self.rta))
            prefix = "_".join(parts)

            json_path = DATA_DIR / f"doors_{prefix}_{ts}.json"
            csv_path  = DATA_DIR / f"doors_{prefix}_{ts}.csv"

            json_path.write_text(
                json.dumps(self.doors, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

            import csv
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                header = ["city", "street", "rta"] + list(self.doors[0].keys())
                w.writerow(header)
                for rec in self.doors:
                    w.writerow([
                        self.city,
                        self.street or "",
                        self.rta    or ""
                    ] + [rec.get(k, "") for k in header[3:]])

            self.gui_q.put(("done", str(json_path), str(csv_path), len(self.doors)))


        except Exception as e:
            self._dbg(f"FATAL ERROR {e}")
            self.gui_q.put(("error", str(e)))
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
        self.root.geometry("830x640")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # État interne
        self.gui_q     = queue.Queue()
        self.pause_evt = threading.Event()
        self.worker: Optional[SalesforceScraper] = None
        self.city2rel: Dict[str, int] = {}
        self.city2streets: Dict[str, List[str]] = {}

        # Variables liées à l’UI
        self.user_var   = tk.StringVar()
        self.pwd_var    = tk.StringVar()
        self.city_var   = tk.StringVar()
        self.street_var = tk.StringVar()
        self.rta_var    = tk.StringVar()

        # Traces pour filtrage dynamique
        self.city_var.trace_add("write", self._filter_cities)
        self.street_var.trace_add("write", self._filter_streets)

        # Construction
        self._build_widgets()
        self._load_or_fetch_cities()

        # Boucle de polling des messages du thread
        self.root.after(100, self._poll_queue)
        self.root.mainloop()

    def _build_widgets(self):
        pad = {"padx": 8, "pady": 3}

        # — Identifiants —
        cred_f = ctk.CTkFrame(self.root)
        cred_f.pack(pady=8, fill="x")
        for c in range(4):
            cred_f.grid_columnconfigure(c, weight=1)
        ctk.CTkLabel(cred_f, text="Username:").grid(row=0, column=0, **pad, sticky="e")
        ctk.CTkEntry(cred_f, textvariable=self.user_var).grid(row=0, column=1, **pad, sticky="w")
        ctk.CTkLabel(cred_f, text="Password:").grid(row=0, column=2, **pad, sticky="e")
        ctk.CTkEntry(cred_f, textvariable=self.pwd_var, show="*").grid(row=0, column=3, **pad, sticky="w")

        # — Sélecteurs Ville / Rue / RTA —
        sel_f = ctk.CTkFrame(self.root)
        sel_f.pack(pady=8, fill="x")
        for c in range(6):
            sel_f.grid_columnconfigure(c, weight=1)

        ctk.CTkLabel(sel_f, text="City:").grid(row=0, column=0, **pad, sticky="e")
        self.city_cb = ctk.CTkComboBox(
            sel_f, variable=self.city_var, values=[],
            state="normal", width=200, command=self._on_city
        )
        self.city_cb.grid(row=0, column=1, **pad, sticky="w")

        ctk.CTkLabel(sel_f, text="Street (opt.):").grid(row=0, column=2, **pad, sticky="e")
        self.street_cb = ctk.CTkComboBox(
            sel_f, variable=self.street_var, values=[],
            state="normal", width=200
        )
        self.street_cb.grid(row=0, column=3, **pad, sticky="w")

        ctk.CTkLabel(sel_f, text="RTA (opt.):").grid(row=0, column=4, **pad, sticky="e")
        ctk.CTkEntry(
            sel_f, textvariable=self.rta_var,
            placeholder_text="e.g. 75261762", width=200
        ).grid(row=0, column=5, **pad, sticky="w")

        # — Boutons Start / Specifics / Pause / Stop —
        btn_f = ctk.CTkFrame(self.root)
        btn_f.pack(pady=8)
        self.start_btn = ctk.CTkButton(btn_f, text="▶ Start", width=160, command=self._start)
        self.detail_btn = ctk.CTkButton(btn_f, text="📄 Get specifics", width=160, state="normal", command=self._start_details)
        self.pause_btn = ctk.CTkButton(btn_f, text="Pause", width=160, state="disabled", command=self._toggle_pause)
        self.stop_btn  = ctk.CTkButton(btn_f, text="■ Stop",  width=160, state="disabled", command=self._stop_worker)
        self.start_btn.grid(row=0, column=0, padx=6)
        self.detail_btn.grid(row=0, column=1, padx=6)
        self.pause_btn.grid(row=0, column=2, padx=6)
        self.stop_btn .grid(row=0, column=3, padx=6)

        # — Progression & stats —
        self.prog      = ctk.CTkProgressBar(self.root, width=780)
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
            threading.Thread(target=self._thread_fetch_streets, args=(city,), daemon=True).start()

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
        """Choisit un fichier doors_*.json/csv et lance ClicDetailScraper."""
        fp = filedialog.askopenfilename(
            title="Choose a doors file",
            initialdir=DATA_DIR,
            filetypes=[("Doors exports", "doors_*.json doors_*.csv"),
                    ("All files", "*.*")]
        )
        if not fp:
            return

        self._log(f"▶ Specifics : {fp}")
        self.detail_scraper = ClicDetailScraper(
            Path(fp), self.gui_q, self.pause_evt
        )
        self.detail_scraper.start()

    def _log(self, txt: str):
        ts = datetime.now().strftime("[%H:%M:%S] ")
        self.log.configure(state="normal")
        self.log.insert("end", ts + txt + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _start(self):
        if self.worker and self.worker.is_alive():
            messagebox.showwarning("Running", "Scraper already running.")
            return
        user, pwd = self.user_var.get().strip(), self.pwd_var.get().strip()
        if not user or not pwd:
            messagebox.showerror("Error", "Entrez nom d’utilisateur et mot de passe.")
            return
        city = self.city_var.get().strip()
        if not city:
            messagebox.showerror("Error", "Sélectionnez une ville d’abord.")
            return
        street = self.street_var.get().strip().upper() or None
        rta    = self.rta_var.get().strip() or None

        # Reset UI
        self.page_lbl.configure(text="Page: 0")
        self.door_lbl.configure(text="Doors: 0")
        self.prog.set(0)
        self.log.configure(state="normal")
        self.log.delete("1.0","end")
        self.log.configure(state="disabled")

        # Lancer le thread
        self.pause_evt.clear()
        self.worker = SalesforceScraper(
            user, pwd, city, street, rta,
            self.gui_q, self.pause_evt
        )
        self.worker.start()

        self.start_btn.configure(state="disabled")
        self.pause_btn.configure(state="normal")
        self.stop_btn .configure(state="normal")

    def _toggle_pause(self):
        if not self.worker:
            return
        if self.pause_evt.is_set():
            self.pause_evt.clear()
            self.pause_btn.configure(text="Pause")
            self._log("▶ Reprise")
        else:
            self.pause_evt.set()
            self.pause_btn.configure(text="Resume")
            self._log("⏸ Pause demandée")

    def _stop_worker(self):
        if self.worker and self.worker.is_alive():
            self.worker.stop()
            self._log("⏹ Arrêt demandé…")
        self.pause_evt.clear()

    def _poll_queue(self):
        try:
            while True:
                tag, *payload = self.gui_q.get_nowait()

                if tag == "log":
                    self._log(payload[0])

                elif tag == "progress":
                    pg, rng, doors, pct = payload
                    self.page_lbl.configure(text=f"Page {pg} {rng}")
                    self.door_lbl.configure(text=f"Doors: {doors:,}")
                    self.prog.set(pct if pct is not None else -1)

                elif tag == "done":
                    json_path, csv_path, cnt = payload
                    self._log(f"✓ Terminé : {cnt:,} portes")
                    self._log(f"JSON → {json_path}")
                    self._log(f"CSV  → {csv_path}")
                    messagebox.showinfo(
                        "Done",
                        f"{cnt:,} portes exportées\n{json_path}\n{csv_path}"
                    )
                    self._reset_buttons()

                elif tag == "error":
                    self._log(f"❌ ERREUR : {payload[0]}")
                    messagebox.showerror("Error", payload[0])
                    self._reset_buttons()
                
                elif tag == "detail_progress":
                    done, total = payload
                    self.page_lbl.configure(text=f"Compte {done}/{total}")
                    # barre indéterminée ici :
                    self.prog.set(done / total)

                elif tag == "detail_done":
                    csv_path, nb = payload
                    self._log(f"✓ Specifics terminé : {nb} comptes")
                    self._log(f"CSV  → {csv_path}")
                    messagebox.showinfo(
                        "Done", f"{nb} comptes exportés dans\n{csv_path}"
                    )

        except queue.Empty:
            pass

        self.root.after(150, self._poll_queue)

    def _reset_buttons(self):
        self.start_btn.configure(state="normal")
        self.pause_btn.configure(state="disabled")
        self.stop_btn .configure(state="disabled")
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
