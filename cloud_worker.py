"""
Cloud-Native High-Concurrency Extraction Worker (GitHub Actions Matrix Swarm).
- 35 parallel HTTP/2 TLS workers via curl_cffi (Chrome 124 / Safari 15.5 rotation).
- Zero-DOM JSON hydration & regex extraction.
- Deterministic shard slicing (--shard-id / --total-shards) for 20-runner parallel execution.
- Adaptive HTTP 429 exponential backoff.
- Self-contained WAL-checkpointed SQLite shard output ready for rclone MD5-verified upload.
"""

import argparse
import concurrent.futures
import json
import logging
import random
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from bs4 import BeautifulSoup
from curl_cffi import requests

from cloud_schema import init_shard_db, normalize_item, upsert_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cloud_worker")

TLS_PROFILES = ["chrome124", "chrome120", "safari15_5"]
thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        profile = random.choice(TLS_PROFILES)
        thread_local.session = requests.Session(impersonate=profile)
        thread_local.profile = profile
    return thread_local.session


def fetch_with_backoff(url: str, headers: Dict[str, str], max_retries: int = 3) -> Any:
    s = get_session()
    for attempt in range(max_retries):
        try:
            r = s.get(url, headers=headers, impersonate=thread_local.profile, timeout=14)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                sleep_s = (2 ** attempt) + random.uniform(0.3, 1.2)
                time.sleep(sleep_s)
                # Rotate TLS profile on backoff
                thread_local.profile = random.choice(TLS_PROFILES)
                thread_local.session = requests.Session(impersonate=thread_local.profile)
                s = thread_local.session
                continue
            return None
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


# ----------------- Partition Matrices ----------------- #

SPAIN_CITIES = [
    "madrid", "barcelona", "valencia", "sevilla", "zaragoza", "malaga", "murcia",
    "palma-de-mallorca", "las-palmas-de-gran-canaria", "bilbao", "alicante-alacant",
    "cordoba", "valladolid", "vigo", "gijon", "hospitalet-de-llobregat", "vitoria-gasteiz",
    "la-coruna", "elche-elx", "granada", "terrassa", "badalona", "oviedo", "sabadell",
    "cartagena", "jerez-de-la-frontera", "mostoles", "santa-cruz-de-tenerife",
    "alcala-de-henares", "fuenlabrada", "pamplona-iruna", "almeria", "leganes",
    "san-sebastian-donostia", "santander", "castellon-de-la-plana", "burgos", "albacete",
    "alcorcon", "getafe", "salamanca", "logrono", "huelva", "badajoz", "tarragona", "leon",
    "lleida", "marbella", "torrevieja", "estepona", "fuengirola", "mijas", "benidorm",
    "torremolinos", "benalmadena", "orihuela", "denia", "javea-xabia", "calpe-calp",
    "altea", "santa-pola", "roquetas-de-mar", "velez-malaga", "nerja",
    "chiclana-de-la-frontera", "el-puerto-de-santa-maria", "sanlucar-de-barrameda",
    "salou", "cambrils", "lloret-de-mar", "blanes", "sitges", "castelldefels", "gava",
    "sant-cugat-del-valles", "mataro", "vilanova-i-la-geltru", "manresa", "rubi",
    "viladecans", "aranjuez", "parla", "alcobendas", "las-rozas-de-madrid",
    "pozuelo-de-alarcon", "majadahonda", "rivas-vaciamadrid", "valdemoro", "torrejon-de-ardoz",
    "boadilla-del-monte", "colmenar-viejo", "tres-cantos", "san-sebastian-de-los-reyes",
    "gandia", "sagunto-sagunt", "torrent", "paterna", "el-ejido", "motril", "almunecar",
    "marbella", "san-pedro-de-alcantara", "sotogrande", "tarifa", "algeciras", "cadiz",
    "ibiza-eivissa", "manacor", "calvia", "llucmajor", "arona", "adeje", "puerto-de-la-cruz",
    "telde", "san-bartolome-de-tirajana", "mogan", "arrecife", "puerto-del-rosario",
]

GERMANY_CITIES = [
    "berlin", "muenchen", "hamburg", "koeln", "frankfurt-am-main", "stuttgart",
    "duesseldorf", "leipzig", "dortmund", "essen", "bremen", "dresden", "hannover",
    "nuernberg", "duisburg", "bochum", "wuppertal", "bielefeld", "bonn", "muenster",
    "karlsruhe", "mannheim", "augsburg", "wiesbaden", "gelsenkirchen", "moenchengladbach",
    "braunschweig", "chemnitz", "kiel", "aachen", "halle-saale", "magdeburg",
    "freiburg-im-breisgau", "krefeld", "mainz", "luebeck", "erfurt", "rostock", "kassel",
    "potsdam", "hagen", "saarbruecken", "hamm", "muelheim-an-der-ruhr", "osnabrueck",
    "solingen", "ludwigshafen", "leverkusen", "oldenburg", "neuss", "heidelberg",
    "paderborn", "darmstadt", "regensburg", "wuerzburg", "ingolstadt", "heilbronn", "ulm",
    "wolfsburg", "goettingen", "pforzheim", "offenbach-am-main", "bottrop", "fuerth",
    "remscheid", "reutlingen", "bremerhaven", "koblenz", "bergisch-gladbach", "erlangen",
    "moers", "trier", "siegen", "hildesheim", "salzgitter", "cottbus", "kaiserslautern",
    "guetersloh", "schwerin", "witten", "gera", "iserlohn", "zwickau", "dueren",
    "ratingen", "luenen", "hanau", "marl", "velbert", "esslingen-am-neckar", "flensburg",
    "tubingen", "ludwigsburg", "konstanz", "worms", "marburg", "bamberg", "bayreuth",
    "passau", "landshut", "rosenheim", "kempten-allgaeu", "aschaffenburg", "deutschland",
]

ITALY_PROVINCES = [
    ("lombardia", "milano"), ("lombardia", "brescia"), ("lombardia", "bergamo"),
    ("lombardia", "monza-della-brianza"), ("lombardia", "como"), ("lombardia", "varese"),
    ("lombardia", "pavia"), ("lombardia", "cremona"), ("lombardia", "mantova"),
    ("lazio", "roma"), ("lazio", "latina"), ("lazio", "frosinone"), ("lazio", "viterbo"),
    ("piemonte", "torino"), ("piemonte", "cuneo"), ("piemonte", "alessandria"), ("piemonte", "novara"),
    ("campania", "napoli"), ("campania", "salerno"), ("campania", "caserta"), ("campania", "avellino"),
    ("veneto", "verona"), ("veneto", "padova"), ("veneto", "vicenza"), ("veneto", "treviso"), ("veneto", "venezia"),
    ("emilia-romagna", "bologna"), ("emilia-romagna", "modena"), ("emilia-romagna", "reggio-emilia"),
    ("emilia-romagna", "parma"), ("emilia-romagna", "ravenna"), ("emilia-romagna", "rimini"), ("emilia-romagna", "ferrara"),
    ("toscana", "firenze"), ("toscana", "pisa"), ("toscana", "lucca"), ("toscana", "arezzo"),
    ("toscana", "livorno"), ("toscana", "pistoia"), ("toscana", "siena"), ("toscana", "prato"),
    ("sicilia", "palermo"), ("sicilia", "catania"), ("sicilia", "messina"), ("sicilia", "trapani"), ("sicilia", "siracusa"),
    ("puglia", "bari"), ("puglia", "lecce"), ("puglia", "taranto"), ("puglia", "foggia"), ("puglia", "brindisi"),
    ("calabria", "reggio-calabria"), ("calabria", "cosenza"), ("calabria", "catanzaro"),
    ("sardegna", "cagliari"), ("sardegna", "sassari"), ("sardegna", "nuoro"),
    ("liguria", "genova"), ("liguria", "savona"), ("liguria", "la-spezia"), ("liguria", "imperia"),
    ("marche", "ancona"), ("marche", "pesaro-urbino"), ("marche", "macerata"),
    ("abruzzo", "pescara"), ("abruzzo", "chieti"), ("abruzzo", "teramo"), ("abruzzo", "l-aquila"),
    ("friuli-venezia-giulia", "udine"), ("friuli-venezia-giulia", "trieste"),
    ("trentino-alto-adige", "trento"), ("trentino-alto-adige", "bolzano"),
    ("umbria", "perugia"), ("umbria", "terni"),
]

POLAND_REGIONS = [
    "mazowieckie", "malopolskie", "slaskie", "dolnoslaskie", "wielkopolskie", "pomorskie",
    "lodzkie", "kujawsko-pomorskie", "lubelskie", "podkarpackie", "zachodniopomorskie",
    "warminsko-mazurskie", "swietokrzyskie", "podlaskie", "opolskie", "lubuskie",
    "warszawa", "krakow", "wroclaw", "poznan", "gdansk", "lodz", "katowice", "szczecin",
    "lublin", "bydgoszcz", "bialystok", "gdynia", "czestochowa", "radom", "torun",
    "rzeszow", "kielce", "gliwice", "olsztyn", "bielsko-biala", "opole", "zielona-gora",
]

NORWAY_FYLKER = [
    ("0.20061", "Oslo"), ("0.22030", "Viken"), ("0.22034", "Innlandet"),
    ("0.22038", "Vestfold og Telemark"), ("0.22042", "Agder"), ("0.20012", "Rogaland"),
    ("0.22046", "Vestland"), ("0.20015", "Møre og Romsdal"), ("0.20016", "Trøndelag"),
    ("0.20018", "Nordland"), ("0.22054", "Troms og Finnmark"),
]

PORTUGAL_DISTRICTS = [
    "lisboa", "porto", "faro", "braga", "setubal", "coimbra", "aveiro", "leiria",
    "santarem", "viana-do-castelo", "funchal-ilha-da-madeira", "viseu", "evora",
    "castelo-branco", "guarda", "beja", "braganca", "portalegre", "ilha-de-sao-miguel",
]

DUTCH_CITIES = [
    "amsterdam", "rotterdam", "den-haag", "utrecht", "eindhoven", "tilburg", "groningen",
    "almere", "breda", "nijmegen", "enschede", "haarlem", "arnhem", "amersfoort", "zaanstad",
    "apeldoorn", "den-bosch", "hoofddorp", "maastricht", "leiden", "dordrecht", "zoetermeer",
    "zwolle", "deventer", "delft", "alkmaar", "hilversum", "venlo", "heerlen", "gouda",
    "leeuwarden", "roosendaal", "helmond", "oss", "schiedam", "spijkenisse", "purmerend",
    "vlaardingen", "alphen-aan-den-rijn", "bergen-op-zoom",
]


# ----------------- Portal Extractors ----------------- #

def extract_fotocasa(params: Tuple[str, str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    city, cat_slug, pq, page = params
    q = f"?{pq}" if pq else ""
    url = f"https://www.fotocasa.es/es/{cat_slug}/{city}/todas-las-zonas/l/{page}{q}"
    headers = {"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    items, orgs = [], []
    for sc in soup.find_all("script"):
        if sc.string and '"initialSearch"' in sc.string:
            try:
                d = json.loads(sc.string)
                reales = d.get("initialSearch", {}).get("result", {}).get("realEstates", [])
                purpose = "for_rent" if "alquiler" in cat_slug else "for_sale"
                for re_item in reales:
                    fid = str(re_item.get("id") or "")
                    if not fid:
                        continue
                    addr = re_item.get("address") or {}
                    muni = addr.get("municipality") or addr.get("city") or city.capitalize()
                    prov = addr.get("province") or city.capitalize()
                    feats = {f.get("key"): f.get("value") for f in re_item.get("features", []) if isinstance(f, dict)}
                    price_str = re_item.get("price") or ""
                    m_pr = re.search(r"([\d\.]+)", price_str)
                    price_clean = m_pr.group(1).replace(".", "") if m_pr else None
                    adv = re_item.get("advertiser") or {}
                    agency = adv.get("name") if isinstance(adv, dict) else None
                    raw = {
                        "item_id": f"fotocasa_es_{fid}",
                        "portal_item_id": fid,
                        "url": f"https://www.fotocasa.es/es/{cat_slug}/{muni}/{fid}/d",
                        "title": f"Inmueble en {muni}, {prov}",
                        "purpose": purpose,
                        "category": "residential" if "viviendas" in cat_slug else "commercial",
                        "bedrooms": feats.get("rooms"),
                        "bathrooms": feats.get("bathrooms"),
                        "area_sqm": feats.get("surface"),
                        "price_local": price_clean,
                        "state_or_province": prov,
                        "city": muni,
                        "postal_code": addr.get("zipCode"),
                        "full_address": f"{muni}, {prov}, España",
                        "brokerage_name": agency,
                    }
                    items.append(normalize_item(raw, "ES", "fotocasa_es"))
                    if agency:
                        orgs.append({
                            "org_id": f"fotocasa_org_{adv.get('id', hash(agency) & 0xffffffff)}",
                            "name": agency,
                            "portal_source": "fotocasa_es",
                            "country_code": "ES",
                            "address": f"{muni}, {prov}",
                        })
            except Exception:
                pass
            break
    return items, orgs


def extract_immowelt(params: Tuple[str, str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    city, cat_slug, pq, page = params
    q = f"&{pq}" if pq else ""
    url = f"https://www.immowelt.de/suche/{city}/{cat_slug}?sp={page}{q}"
    headers = {"Accept-Language": "de-DE,de;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    items, seen = [], set()
    for a_link in soup.find_all("a", href=True):
        href = a_link["href"]
        if "/expose/" not in href:
            continue
        m = re.search(r"/expose/([a-zA-Z0-9\-]+)", href)
        if not m:
            continue
        iid = m.group(1)
        if iid in seen:
            continue
        seen.add(iid)
        card = a_link.find_parent("div")
        txt = card.get_text(" ", strip=True) if card else ""
        pm = re.search(r"([\d\.]+)\s*€", txt)
        am = re.search(r"([\d\.,]+)\s*m²", txt)
        rm = re.search(r"(\d+[\.,]?\d*)\s*Zi", txt)
        lm = re.search(r"(\d{5})\s+([A-Za-zäöüÄÖÜß\s\-]+)", txt)
        loc_city = lm.group(2).strip() if lm else city.capitalize()
        raw = {
            "item_id": f"immowelt_de_{iid}",
            "portal_item_id": iid,
            "url": f"https://www.immowelt.de/expose/{iid}",
            "title": f"Immobilie in {loc_city}",
            "purpose": "for_rent" if "mieten" in cat_slug else "for_sale",
            "category": "residential",
            "unit_subtype": "apartment" if "wohnungen" in cat_slug else "house",
            "bedrooms": rm.group(1) if rm else None,
            "area_sqm": am.group(1).replace(",", ".") if am else None,
            "price_local": pm.group(1).replace(".", "") if pm else None,
            "state_or_province": loc_city,
            "city": loc_city,
            "postal_code": lm.group(1) if lm else None,
            "full_address": f"{lm.group(1)} {loc_city}, Deutschland" if lm else f"{loc_city}, Deutschland",
        }
        items.append(normalize_item(raw, "DE", "immowelt_de"))
    return items, []


def extract_subito(params: Tuple[str, str, str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    reg, metro, cat, pq, page = params
    q = f"&{pq}" if pq else ""
    metro_slug = f"/{metro}" if metro else ""
    url = f"https://www.subito.it/annunci-{reg}/{cat}/immobili{metro_slug}/?o={page}{q}"
    headers = {"Accept-Language": "it-IT,it;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    sc = soup.find("script", id="__NEXT_DATA__")
    if not sc or not sc.string:
        return [], []
    items, orgs = [], []
    try:
        data = json.loads(sc.string)
        items_data = data.get("props", {}).get("pageProps", {}).get("initialState", {}).get("items", {})
        ads = items_data.get("list", []) if isinstance(items_data, dict) else (items_data if isinstance(items_data, list) else [])
        purpose = "for_sale" if "vendita" in cat else ("for_rent" if "affitto" in cat else "commercial")
        for ad in ads:
            obj = ad.get("item", ad)
            urn = str(obj.get("urn") or obj.get("id") or "")
            if not urn:
                continue
            cid = urn.split(":")[-1]
            geo = obj.get("geo") or {}
            city_name = (geo.get("city") or {}).get("value") or metro.capitalize()
            reg_name = (geo.get("region") or {}).get("value") or reg.capitalize()
            feats = {f.get("uri"): f.get("values", [{}])[0].get("value") for f in obj.get("features", []) if isinstance(f, dict)}
            adv = obj.get("advertiser") or {}
            agency = adv.get("name")
            raw = {
                "item_id": f"subito_it_{cid}",
                "portal_item_id": cid,
                "url": (obj.get("urls") or {}).get("default") or f"https://www.subito.it/annunci-italia/{cid}.htm",
                "title": obj.get("subject") or "Immobile in Italia",
                "purpose": purpose,
                "category": "residential" if cat in ("vendita", "affitto") else "commercial",
                "bedrooms": feats.get("rooms"),
                "bathrooms": feats.get("bathrooms"),
                "area_sqm": feats.get("size"),
                "price_local": feats.get("price"),
                "state_or_province": reg_name,
                "city": city_name,
                "full_address": f"{city_name}, {reg_name}, Italia",
                "agent_name": agency,
                "agent_phone": adv.get("phone"),
                "brokerage_name": agency,
            }
            items.append(normalize_item(raw, "IT", "subito_it"))
            if agency:
                orgs.append({
                    "org_id": f"subito_org_{adv.get('userId', hash(agency) & 0xffffffff)}",
                    "name": agency,
                    "portal_source": "subito_it",
                    "country_code": "IT",
                    "phone": adv.get("phone"),
                    "address": f"{city_name}, {reg_name}",
                })
    except Exception:
        pass
    return items, orgs


def extract_otodom(params: Tuple[str, str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    loc, cat_slug, pq, page = params
    q = f"&{pq}" if pq else ""
    url = f"https://www.otodom.pl/pl/wyniki/{cat_slug}/{loc}?limit=36&page={page}{q}"
    headers = {"Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    sc = soup.find("script", id="__NEXT_DATA__")
    if not sc or not sc.string:
        return [], []
    items, orgs = [], []
    try:
        data = json.loads(sc.string)
        arr = data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])
        purpose = "for_rent" if "wynajem" in cat_slug else "for_sale"
        for row in arr:
            oid = row.get("id")
            if not oid:
                continue
            agency = row.get("agency") or {}
            ag_name = agency.get("name")
            price_obj = row.get("totalPrice") or {}
            loc_obj = row.get("location") or {}
            addr = loc_obj.get("address") or {}
            city_v = (addr.get("city") or {}).get("name") if isinstance(addr.get("city"), dict) else loc.capitalize()
            prov_v = (addr.get("province") or {}).get("name") if isinstance(addr.get("province"), dict) else ""
            coords = loc_obj.get("coordinates") or {}
            raw = {
                "item_id": f"otodom_pl_{oid}",
                "portal_item_id": str(oid),
                "url": f"https://www.otodom.pl/pl/oferta/{row.get('slug', oid)}",
                "title": row.get("title") or f"Nieruchomość {city_v}",
                "purpose": purpose,
                "unit_subtype": "apartment" if "mieszkanie" in cat_slug else "house",
                "bedrooms": row.get("roomsNumber"),
                "area_sqm": row.get("areaInSquareMeters"),
                "price_local": price_obj.get("value"),
                "currency": price_obj.get("currency", "PLN"),
                "state_or_province": prov_v,
                "city": city_v,
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
                "brokerage_name": ag_name,
            }
            items.append(normalize_item(raw, "PL", "otodom_pl"))
            if ag_name:
                orgs.append({
                    "org_id": f"otodom_org_{agency.get('id', hash(ag_name) & 0xffffffff)}",
                    "name": ag_name,
                    "portal_source": "otodom_pl",
                    "country_code": "PL",
                    "address": prov_v or city_v,
                })
    except Exception:
        pass
    return items, orgs


def extract_finn(params: Tuple[str, str, str, int, int]) -> Tuple[List[Dict], List[Dict]]:
    loc_id, fylke, section, pt, page = params
    url = f"https://www.finn.no/realestate/{section}/search.html?location={loc_id}&property_type={pt}&page={page}"
    headers = {"Accept-Language": "nb-NO,nb;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    items, orgs = [], []
    for art in soup.find_all("article", class_=lambda c: c and "sf-search-ad" in c):
        link = art.select_one("a")
        href = link.get("href") if link else None
        if not href or "finnkode=" not in href:
            continue
        fk = href.split("finnkode=")[-1].split("&")[0]
        txt = art.get_text(" | ", strip=True)
        mp = re.search(r"([\d\s]+)\s*kr", txt)
        ma = re.search(r"(\d+)\s*m²", txt)
        mr = re.search(r"(\d+)\s*soverom", txt, re.I)
        title = (art.select_one("h2") or link).get_text(strip=True)
        raw = {
            "item_id": f"finn_no_{fk}",
            "portal_item_id": fk,
            "url": href if href.startswith("http") else f"https://www.finn.no{href}",
            "title": title,
            "purpose": "for_rent" if section == "lettings" else "for_sale",
            "bedrooms": mr.group(1) if mr else None,
            "area_sqm": ma.group(1) if ma else None,
            "price_local": mp.group(1).replace(" ", "") if mp else None,
            "currency": "NOK",
            "state_or_province": fylke,
            "city": fylke,
            "full_address": f"{title}, {fylke}, Norge",
        }
        items.append(normalize_item(raw, "NO", "finn_no"))
    return items, orgs


def extract_pararius(params: Tuple[str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    city, cat, page = params
    suf = f"/page-{page}" if page > 1 else ""
    url = f"https://www.pararius.nl/{cat}/{city}{suf}"
    headers = {"Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    items, orgs = [], []
    purpose = "for_sale" if "koop" in cat else "for_rent"
    for li in soup.select(".search-list__item--listing"):
        wc = li.select_one("wc-listing-search-item")
        lid = wc.get("data-id") if wc else None
        link = li.select_one("a[href*='-te-']")
        if not link or not link.get("href"):
            continue
        href = link["href"]
        key = lid or href.split("/")[-1]
        title = link.get("aria-label") or link.get_text(strip=True)
        p_el = li.select_one(".listing-search-item__price")
        mp = re.search(r"€\s*([\d\.,]+)", p_el.get_text(strip=True)) if p_el else None
        a_el = li.select_one(".illustrated-features__item--surface-area")
        ma = re.search(r"(\d+)\s*m²", a_el.get_text(strip=True)) if a_el else None
        raw = {
            "item_id": f"pararius_nl_{key}",
            "portal_item_id": str(key),
            "url": f"https://www.pararius.nl{href}" if href.startswith("/") else href,
            "title": title,
            "purpose": purpose,
            "area_sqm": ma.group(1) if ma else None,
            "price_local": mp.group(1).replace(".", "").replace(",", ".") if mp else None,
            "state_or_province": city.capitalize(),
            "city": city.capitalize(),
            "full_address": f"{title}, {city.capitalize()}, Nederland",
        }
        items.append(normalize_item(raw, "NL", "pararius_nl"))
    return items, orgs


def extract_imovirtual(params: Tuple[str, str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    dist, cat_slug, pq, page = params
    q = f"&{pq}" if pq else ""
    url = f"https://www.imovirtual.com/pt/resultados/{cat_slug}/{dist}?limit=36&page={page}{q}"
    headers = {"Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8"}
    r = fetch_with_backoff(url, headers)
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "lxml")
    sc = soup.find("script", id="__NEXT_DATA__")
    if not sc or not sc.string:
        return [], []
    items, orgs = [], []
    try:
        data = json.loads(sc.string)
        arr = data.get("props", {}).get("pageProps", {}).get("data", {}).get("searchAds", {}).get("items", [])
        purpose = "for_rent" if "arrendar" in cat_slug else "for_sale"
        for row in arr:
            oid = row.get("id")
            if not oid:
                continue
            price_obj = row.get("totalPrice") or {}
            raw = {
                "item_id": f"imovirtual_pt_{oid}",
                "portal_item_id": str(oid),
                "url": f"https://www.imovirtual.com/pt/anuncio/{row.get('slug', oid)}",
                "title": row.get("title") or f"Imóvel em {dist.capitalize()}",
                "purpose": purpose,
                "bedrooms": row.get("roomsNumber"),
                "area_sqm": row.get("areaInSquareMeters"),
                "price_local": price_obj.get("value"),
                "state_or_province": dist.capitalize(),
                "city": dist.capitalize(),
            }
            items.append(normalize_item(raw, "PT", "imovirtual_pt"))
    except Exception:
        pass
    return items, orgs


# ----------------- Task Queue Builder ----------------- #

def build_tasks_for_portal(portal: str) -> List[Any]:
    tasks: List[Any] = []
    if portal == "fotocasa_es":
        pqs = ["", "maxPrice=200000", "minPrice=200000&maxPrice=450000", "minPrice=450000"]
        for city in SPAIN_CITIES:
            for cat in ["comprar/viviendas", "alquiler/viviendas", "comprar/locales-y-oficinas"]:
                for pq in pqs:
                    for p in range(1, 31):
                        tasks.append((city, cat, pq, p))
    elif portal == "immowelt_de":
        pqs = ["", "pma=200000", "pmi=200000&pma=450000", "pmi=450000"]
        for city in GERMANY_CITIES:
            for cat in ["wohnungen/kaufen", "haeuser/kaufen", "wohnungen/mieten", "haeuser/mieten"]:
                for pq in pqs:
                    for p in range(1, 26):
                        tasks.append((city, cat, pq, p))
    elif portal == "subito_it":
        pqs = ["", "pe=150000", "ps=150000&pe=350000", "ps=350000"]
        for reg, metro in ITALY_PROVINCES:
            for cat in ["vendita", "affitto", "terreni", "locali-commerciali"]:
                for pq in pqs:
                    for p in range(1, 21):
                        tasks.append((reg, metro, cat, pq, p))
    elif portal == "otodom_pl":
        pqs = ["", "priceMax=450000", "priceMin=450000&priceMax=900000", "priceMin=900000"]
        for loc in POLAND_REGIONS:
            for cat in ["sprzedaz/mieszkanie", "sprzedaz/dom", "wynajem/mieszkanie", "sprzedaz/dzialka"]:
                for pq in pqs:
                    for p in range(1, 26):
                        tasks.append((loc, cat, pq, p))
    elif portal == "finn_no":
        for loc_id, fylke in NORWAY_FYLKER:
            for section in ["homes", "lettings", "leisure-sale", "plots"]:
                for pt in [1, 2, 3, 4]:
                    for p in range(1, 36):
                        tasks.append((loc_id, fylke, section, pt, p))
    elif portal == "pararius_nl":
        for city in DUTCH_CITIES:
            for cat in ["koopwoningen", "huurwoningen"]:
                for p in range(1, 31):
                    tasks.append((city, cat, p))
    elif portal == "imovirtual_pt":
        pqs = ["", "search%5Bfilter_float_price%3Ato%5D=250000", "search%5Bfilter_float_price%3Afrom%5D=250000"]
        for dist in PORTUGAL_DISTRICTS:
            for cat in ["comprar/apartamento", "comprar/moradia", "arrendar/apartamento", "comprar/terreno"]:
                for pq in pqs:
                    for p in range(1, 26):
                        tasks.append((dist, cat, pq, p))
    return tasks


EXTRACTOR_MAP = {
    "fotocasa_es": extract_fotocasa,
    "immowelt_de": extract_immowelt,
    "subito_it": extract_subito,
    "otodom_pl": extract_otodom,
    "finn_no": extract_finn,
    "pararius_nl": extract_pararius,
    "imovirtual_pt": extract_imovirtual,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal", required=True, choices=list(EXTRACTOR_MAP.keys()))
    parser.add_argument("--shard-id", type=int, default=0)
    parser.add_argument("--total-shards", type=int, default=1)
    parser.add_argument("--workers", type=int, default=35)
    parser.add_argument("--max-tasks", type=int, default=0, help="Optional cap for smoke tests")
    parser.add_argument("--output", required=True, help="Output SQLite shard path")
    args = parser.parse_args()

    all_tasks = build_tasks_for_portal(args.portal)
    shard_tasks = [t for i, t in enumerate(all_tasks) if i % args.total_shards == args.shard_id]
    if args.max_tasks > 0:
        shard_tasks = shard_tasks[: args.max_tasks]

    logger.info(
        f"Cloud Worker [{args.portal}] Shard {args.shard_id}/{args.total_shards}: "
        f"{len(shard_tasks):,} tasks queued across {args.workers} workers."
    )

    out_path = Path(args.output)
    conn = init_shard_db(out_path)
    fn = EXTRACTOR_MAP[args.portal]

    t0 = time.time()
    ingested_total = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(fn, task) for task in shard_tasks]
        for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                items, orgs = fut.result()
                if items or orgs:
                    ingested_total += upsert_batch(conn, items, orgs)
            except Exception as e:
                logger.debug(f"Task exception: {e}")

            if idx % 250 == 0:
                elapsed = max(time.time() - t0, 0.1)
                logger.info(
                    f"[{args.portal} S{args.shard_id}] Progress {idx:,}/{len(shard_tasks):,} | "
                    f"Ingested: {ingested_total:,} | Speed: {ingested_total / elapsed:.1f} items/s"
                )

    # Checkpoint WAL so the .sqlite shard is 100% self-contained
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM items")
    unique_count = cur.fetchone()[0]
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.close()

    elapsed = max(time.time() - t0, 0.1)
    logger.info(
        f"=== SHARD COMPLETE [{args.portal} S{args.shard_id}] Unique Items: {unique_count:,} | "
        f"Elapsed: {elapsed:.1f}s | Output: {out_path} ==="
    )


if __name__ == "__main__":
    main()
