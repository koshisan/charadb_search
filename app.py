import streamlit as st
import psycopg2
import os
import json
import datetime
import time
import math
import re
import socket
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from image_server import start_image_server
import extra_streamlit_components as stx
import urllib.request
import urllib.error
import streamlit.components.v1 as components

# Importieren der Konfiguration aus config.py
# Importieren der Konfiguration aus config.py
try:
    import config
    IMAGE_ROOT = config.IMAGE_ROOT
    DB_CONFIG = config.DB_CONFIG
    # Optional config
    IMAGE_SERVER_BASE_URL = getattr(config, "IMAGE_SERVER_BASE_URL", None)
except ImportError:
    st.error("Konfigurationsdatei 'config.py' nicht gefunden oder fehlerhaft. Bitte erstelle sie basierend auf dem Beispiel.")
    st.stop()

# --- SETUP & STYLES ---
st.set_page_config(layout="wide", page_title="Char Archive Ultimate", page_icon="🗃️")

# Handle Session State Initialization
if "page" not in st.session_state:
    st.session_state.page = 0
if "p_jump" not in st.session_state:
    st.session_state.p_jump = 1
if "p_jump_b" not in st.session_state:
    st.session_state.p_jump_b = 1

# COOKIE MANAGEMENT - Unified approach
# Use a static, unique key for the manager
cookie_manager = stx.CookieManager(key="charasearch_ultimate_cookie_manager")
cookies = cookie_manager.get_all()

# Defaults
DEFAULT_SETTINGS = {
    "limit": 24,
    "selected_sources": ["chub", "risuai"],
    "selected_fields": ["tags"],
    "token_range": [0, 8000],
    "unlimited": False
}

# 1. Initialize session state with defaults (if not set)
for key, val in DEFAULT_SETTINGS.items():
    if key not in st.session_state:
        st.session_state[key] = val

# 2. Sync Logic (Browser -> session_state)
# We wait patiently for the unified 'app_settings' cookie to arrive.
if not st.session_state.get("cookies_initialized", False):
    # Track when we started waiting
    if "sync_start_time" not in st.session_state:
        st.session_state.sync_start_time = time.time()
    
    elapsed = time.time() - st.session_state.sync_start_time

    if cookies and "app_settings" in cookies:
        try:
            raw_val = cookies["app_settings"]
            saved = raw_val if isinstance(raw_val, dict) else json.loads(raw_val)
            
            if saved:
                for k in DEFAULT_SETTINGS:
                    if k in saved:
                        st.session_state[k] = saved[k]
                
                st.session_state.cookies_initialized = True
                st.session_state["debug_sync_msg"] = f"Successfully synced after {elapsed:.2f}s."
                st.rerun()
        except Exception as e:
            st.session_state.cookies_initialized = True
            st.session_state["debug_sync_msg"] = f"Sync failed: {str(e)}"
    else:
        # User requested a 20s wait period
        if elapsed < 20.0:
            # Show a subtle loading indicator if it takes more than 1s
            if elapsed > 1.0:
                st.info(f"Warte auf Einstellungen... ({int(elapsed)}s / 20s)")
            
            time.sleep(0.1) # Prevent CPU hammering
            st.rerun()
        else:
            st.session_state.cookies_initialized = True
            st.session_state["debug_sync_msg"] = f"No settings found after {elapsed:.1f}s wait."

# Check Query Params for Tag Search (Click on Badge)
if "q" in st.query_params:
    st.session_state.search_input = st.query_params["q"]
    st.session_state.page = 0
    st.session_state.p_jump = 1
    st.session_state.p_jump_b = 1

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
    /* Global Styles & Reset */
    :root {
        --bg-main: #0f1115;
        --card-bg: rgba(26, 28, 35, 0.7);
        --accent: #ff4b4b;
        --accent-glow: rgba(255, 75, 75, 0.3);
        --text-main: #e0e0e0;
        --text-dim: #9ca3af;
        --border-color: rgba(255, 255, 255, 0.1);
        --glass-border: rgba(255, 255, 255, 0.05);
    }

    .main {
        background-color: var(--bg-main);
        font-family: 'Inter', system-ui, -apple-system, sans-serif;
    }

    .block-container {
        padding-top: 3rem;
        max-width: 95rem;
    }
    
    /* Card Container - Glassmorphism */
    .char-card {
        background: var(--card-bg);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid var(--glass-border);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.4);
        transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
    }
    .char-card:hover {
        border-color: var(--accent-glow);
        transform: translateY(-2px);
        box-shadow: 0 10px 40px 0 rgba(255, 75, 75, 0.1);
    }
    
    /* Typography */
    .char-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #fff;
        margin-bottom: 4px;
        letter-spacing: -0.02em;
    }
    .char-author {
        font-size: 0.85rem;
        color: var(--text-dim);
        font-weight: 500;
        letter-spacing: 0.01em;
    }
    .char-tagline {
        font-style: italic;
        color: #cbd5e1;
        border-left: 2px solid var(--accent);
        padding-left: 12px;
        margin: 12px 0;
        font-size: 0.95rem;
    }
    
    /* Badges / Tags */
    .tag-badge {
        background: rgba(45, 48, 58, 0.6);
        color: #d1d5db !important;
        padding: 4px 10px;
        border-radius: 6px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        font-size: 0.72rem;
        margin-right: 6px;
        margin-bottom: 6px;
        display: inline-block;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        text-decoration: none;
        transition: all 0.2s ease;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    .tag-badge:hover {
        background: var(--accent);
        border-color: var(--accent);
        color: #fff !important;
        transform: scale(1.05);
    }
    
    /* Images */
    .char-image-container img {
        border-radius: 10px;
        width: 100%;
        object-fit: cover;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    /* Form & Inputs */
    [data-testid="stForm"] {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
    }
    
    /* Hide empty form containers that create phantom elements */
    [data-testid="stForm"]:empty,
    [data-testid="stForm"] > div:empty {
        display: none !important;
        height: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    
    /* Buttons */
    div.stButton > button {
        background: rgba(45, 48, 58, 0.8);
        color: #fff;
        border: 1px solid var(--glass-border);
        border-radius: 8px;
        font-weight: 600;
        padding: 0.5rem 1rem;
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        background: var(--accent);
        border-color: var(--accent);
        box-shadow: 0 0 15px var(--accent-glow);
    }
    
    /* Search Form Submit Button Specifics */
    #search_form div.stButton > button {
        margin-top: 1.5rem !important;
    }

    /* HTML Preview Box - Sleeker & Matching Theme */
    .char-preview-box {
        max-height: 400px;
        overflow-y: auto;
        padding: 16px;
        background: rgba(0, 0, 0, 0.2);
        border: 1px solid var(--glass-border);
        border-radius: 10px;
        margin-top: 12px;
        margin-bottom: 12px;
    }
    .char-preview-box::-webkit-scrollbar {
        width: 5px;
    }
    .char-preview-box::-webkit-scrollbar-thumb {
        background: #333;
        border-radius: 10px;
    }
    .char-preview-box::-webkit-scrollbar-thumb:hover {
        background: var(--accent);
    }
    
    /* Pagination Styles */
    .pag-label {
        font-size: 0.8rem;
        color: var(--text-dim);
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Download Buttons Custom styling for integration */
    .stDownloadButton > button {
        width: 100%;
        font-size: 0.8rem !important;
        margin-bottom: 6px !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Token Badge */
    .token-badge {
        background: rgba(255, 75, 75, 0.15);
        color: var(--accent);
        border: 1px solid var(--accent-glow);
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 700;
        margin-left: 8px;
        display: inline-block;
        vertical-align: middle;
    }

    /* Inline Tag Details */
    .tags-container {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0; /* Gaps are handled by margins in .tag-badge */
    }
    .tag-details {
        display: contents; /* Makes children participate in the outer flex box */
    }
    .tag-details summary {
        list-style: none;
        display: inline-block;
        outline: none;
        cursor: pointer;
    }
    .tag-details summary::-webkit-details-marker {
        display: none;
    }
    .tag-details[open] summary {
        display: none;
    }

    /* Copy Link Button Styling */
    .copy-link-btn {
        background: rgba(45, 48, 58, 0.8) !important;
        color: #fff !important;
        border: 1px solid var(--glass-border) !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        padding: 0.5rem 1rem !important;
        width: 100% !important;
        min-height: 35px !important;
        font-size: 0.8rem !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
    }
    .copy-link-btn:hover {
        background: var(--accent) !important;
        border-color: var(--accent) !important;
        box-shadow: 0 0 10px var(--accent-glow) !important;
    }
    
    /* Image Overlay & Badges */
    .image-wrapper {
        position: relative;
        display: inline-block;
        width: 100%;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    .image-wrapper img {
        width: 100%;
        display: block;
        object-fit: cover;
    }
    .badge-overlay {
        position: absolute;
        top: 8px;
        right: 8px;
        display: flex;
        flex-direction: column;
        gap: 4px;
        z-index: 10;
        pointer-events: none;
    }
    .safety-badge {
        background: rgba(0, 0, 0, 0.85);
        color: #fff;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        border: 1px solid rgba(255, 75, 75, 0.4);
        box-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }
    .safety-badge.loli {
        background: rgba(255, 0, 0, 0.9);
        border-color: #ff0000;
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(255, 0, 0, 0.7); }
        70% { box-shadow: 0 0 0 10px rgba(255, 0, 0, 0); }
        100% { box-shadow: 0 0 0 0 rgba(255, 0, 0, 0); }
    }
</style>
""", unsafe_allow_html=True)

# --- FUNKTIONEN ---

def clean_html(html_str):
    """Deaktiviert Autoplay in Audio/Video Tags"""
    if not html_str: return ""
    # Ersetze autoplay (case-insensitive) durch data-autoplay
    cleaned = re.sub(r'\bautoplay\b', 'data-autoplay', html_str, flags=re.IGNORECASE)
    return cleaned

def render_preview_html(content):
    """Rendert HTML-Content in einem isolierten Iframe (st.components.v1.html) um Style-Bleeding zu verhindern."""
    if not content: return
    
    # CSS für das Innere des Iframes - passend zum App-Style
    iframe_css = """
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
    <style>
        body {
            background: transparent;
            color: #d1d5db;
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
            font-size: 0.9rem;
            line-height: 1.6;
            margin: 0;
            padding: 0;
        }
        .preview-content {
            padding: 2px;
        }
        ::-webkit-scrollbar {
            width: 5px;
        }
        ::-webkit-scrollbar-thumb {
            background: #333;
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #ff4b4b;
        }
    </style>
    <script>
        function copyToClipboard(text, btn) {
            const originalText = btn.innerText;
            function showSuccess(msg) {
                btn.innerText = "✅ " + msg;
                btn.style.borderColor = "#4CAF50";
                setTimeout(() => {
                    btn.innerText = originalText;
                    btn.style.borderColor = "";
                }, 2000);
            }

            if (navigator.clipboard && window.isSecureContext) {
                navigator.clipboard.writeText(text).then(() => showSuccess("KOPIERT")).catch(e => fallbackCopy(text));
            } else {
                fallbackCopy(text);
            }

            function fallbackCopy(text) {
                try {
                    const textArea = document.createElement("textarea");
                    textArea.value = text;
                    textArea.style.position = "fixed";
                    textArea.style.left = "-9999px";
                    textArea.style.top = "0";
                    document.body.appendChild(textArea);
                    textArea.focus();
                    textArea.select();
                    const successful = document.execCommand('copy');
                    document.body.removeChild(textArea);
                    if (successful) showSuccess("KOPIERT");
                    else showSuccess("FEHLER");
                } catch (err) {
                    showSuccess("FEHLER");
                }
            }
        }
    </script>
    """
    
    full_html = f"""
    {iframe_css}
    <div class="preview-content">
        {content}
    </div>
    """
    
    # Berechne ungefähre Höhe basierend auf Content-Länge (Streamlit Iframes brauchen feste Höhe oder scrollen)
    # Wir nutzen ein div mit overflow im app-css, aber das iframe selbst braucht auch Platz.
    import streamlit.components.v1 as components
    components.html(full_html, height=300, scrolling=True)

# start_image_server is now imported from image_server.py

def change_page(new_page):
    """Callback für Paginierung - Synchronisiert alle States"""
    st.session_state.page = new_page
    st.session_state.p_jump = new_page + 1

@st.cache_resource
def get_db_connection():
    """Hält die DB-Verbindung offen, damit es schneller geht"""
    return psycopg2.connect(**DB_CONFIG)

@st.cache_data(show_spinner=False, ttl=600)
def run_query_cached(sql, params):
    """Führt die Query aus und cached das Ergebnis für 10 Minuten"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_image_path(image_hash, debug=False):
    """Findet das Bild im Sharding-Dschungel (nun auch rekursiv)"""
    if not image_hash: return None, []
    
    extensions = [".png", ".webp", ".jpg", ".jpeg", ""]
    candidates = []
    
    # Paths to check
    checks = []
    
    # 0. User Specific: Split Filename Sharding (hashed-data/a/b/c/defg...)
    # Hash: 2b2b9b... -> Path: hashed-data/2/b/2/b9b...
    if len(image_hash) > 3:
        checks.append(os.path.join(IMAGE_ROOT, "hashed-data", image_hash[0], image_hash[1], image_hash[2], image_hash[3:]))

    # 1. Nested Sharding (hashed-data/a/b/abcde...)
    if len(image_hash) >= 2:
        checks.append(os.path.join(IMAGE_ROOT, "hashed-data", image_hash[0], image_hash[1], image_hash))

    # 2. Simple Sharding (hashed-data/a/abcde...)
    if len(image_hash) >= 1:
        checks.append(os.path.join(IMAGE_ROOT, "hashed-data", image_hash[0], image_hash))
        
    # 3. Flat (hashed-data/abcde...)
    checks.append(os.path.join(IMAGE_ROOT, "hashed-data", image_hash))
    
    # Generate Candidate List (with Exts) for Debugging
    for c in checks:
        for ext in extensions:
            candidates.append(c + ext)
            
    # Real Search
    for path_base in checks:
        # Check Direct File + Ext
        for ext in extensions:
            p = path_base + ext
            if os.path.exists(p) and os.path.isfile(p):
                return p, candidates

        # Check Directory (Deep Search) - Fallback
        if os.path.exists(path_base) and os.path.isdir(path_base):
            if debug: candidates.append(f"[DIR FOUND] {path_base} -> Scanning...")
            for root, _, files in os.walk(path_base):
                for f in files:
                    if f.lower().endswith(tuple([".png", ".webp", ".jpg", ".jpeg"])):
                        found = os.path.join(root, f)
                        if debug: candidates.append(f"[DEEP MATCH] {found}")
                        return found, candidates
            
    return None, candidates

def get_safety_badges(metadata):
    """Extrahiert Safety-Badges aus Metadata"""
    if not metadata: return []
    
    badges = []
    safety = metadata.get('safety', {})
    
    # 1. Bad Shit
    bad = safety.get('bad_shit', {})
    if bad.get('loli'): badges.append({'label': 'LOLI', 'class': 'loli'})
    
    # 2. Categories
    cats = safety.get('categories', {})
    # Map key -> Label
    cat_map = {
        'sexual': 'Sexual',
        'violence': 'Violence',
        'hate': 'Hate',
        'self_harm': 'Self Harm',
        'harassment': 'Harass',
        'sexual_minors': 'Minors',
        'violence_graphic': 'Gore'
    }
    
    for key, label in cat_map.items():
        if cats.get(key): # If value is not None/False/Empty
            badges.append({'label': label, 'class': ''})
            
    # 3. Legacy / Tags
    tags = metadata.get('tags', []) or []
    if isinstance(tags, str): tags = [tags]
    # Simple check for explicit NSFW tag if no detailed safety
    if not badges and (metadata.get('nsfw') or any(str(t).lower() in ['nsfw', 'r-18', 'x-rated'] for t in tags)):
        badges.append({'label': 'NSFW', 'class': ''})
        
    return badges

def format_tags(tags_data):
    """Parsed Tags und gibt sie als Liste zurück"""
    if not tags_data: return []
    tags = []
    if isinstance(tags_data, list):
        tags = tags_data
    elif isinstance(tags_data, str):
        try:
            tags = json.loads(tags_data)
        except:
            if ',' in tags_data:
                tags = tags_data.split(',')
            else:
                tags = [tags_data]
    
    # Clean up tags (remove quotes, whitespace)
    return [str(t).strip().replace('"', '').replace("'", "") for t in tags if str(t).strip()]

def render_badges(tags_list):
    html_str = ""
    for t in tags_list: 
        # Link to ?q=TAGNAME to trigger search on reload
        html_str += f'<a href="?q={t}" target="_self" class="tag-badge">{t}</a>'
    return html_str

# --- HAUPTBEREICH ---

# Start Image Server
img_server_url = start_image_server(IMAGE_ROOT)

# Use markdown instead of st.title to avoid phantom container
# Position this absolutely at the very top of the app
st.markdown('<div id="top-marker" style="position: absolute; top: 0; left: 0; height: 1px; width: 1px; z-index: -1;"></div>', unsafe_allow_html=True)
st.markdown("# 🗃️ Character Archive: Local Edition")

# Init Session State for Pagination
if 'page' not in st.session_state: st.session_state.page = 0
if 'last_query' not in st.session_state: st.session_state.last_query = ""

# Sidebar
with st.sidebar:
    # Search Form at the top
    st.header("🔍 Suche")
    with st.form("search_form"):
        search_query = st.text_input("Suchbegriff", placeholder="Suchbegriff eingeben...", value=st.session_state.get('search_input', ""), label_visibility="collapsed")
        search_btn = st.form_submit_button("Suche", width="stretch")
        if search_btn:
            st.session_state.search_input = search_query
            st.session_state.page = 0
            st.session_state.p_jump = 1
            st.session_state.p_jump_b = 1
            st.rerun()
    
    st.divider()
    st.header("Filter & Einstellungen")
    
    source_map = {
        "chub": "Chub.ai",
        "risuai": "RisuAI",
        "char_tavern": "Tavern",
        "generic": "Generic",
        "chub_lorebook": "Lorebooks",
        "booru": "Booru",
        "nyaime": "Nyaime",
        "webring": "Webring"
    }
    
    # Results limit slider
    st.slider("Anzahl Ergebnisse", 10, 250, key="limit")
    
    st.divider()
    
    # Using DIRECT BINDING to st.session_state keys (keys match DEFAULT_SETTINGS)
    st.multiselect(
        "Quellen", 
        options=list(source_map.keys()), 
        format_func=lambda x: source_map[x],
        key="selected_sources"
    )
    
    search_options = {
        "name": "Name",
        "tags": "Tags",
        "description": "Beschreibung / Summary",
        "creator_notes": "Creator Notes",
        "first_mes": "First Message",
        "scenario": "Scenario",
        "author": "Autor"
    }
    
    st.multiselect(
        "Suche in...",
        options=list(search_options.keys()),
        format_func=lambda x: search_options[x],
        key="selected_fields"
    )
    
    st.selectbox("Sortierung", [
        "Neueste zuerst", 
        "Älteste zuerst", 
        "Name (A-Z)", 
        "Token Count (Viel)", 
        "Token Count (Wenig)"
    ], key="sort_option")

    st.divider()
    st.write("📊 Token-Filter")
    # We use a helper for the list->tuple conversion to satisfy slider
    st.slider("Token-Bereich", 0, 16000, key="token_range_ui", step=100, 
              value=tuple(st.session_state.token_range))
    st.checkbox("Nach oben offen", key="unlimited")
    
    # Sync the UI-only token range back to the main state
    st.session_state.token_range = list(st.session_state.token_range_ui)

    if st.button("Einstellungen als Standard speichern", width="stretch", key="save_settings_btn"):
        # Gather all current session_state values
        settings_to_save = {k: st.session_state.get(k) for k in DEFAULT_SETTINGS}
        
        cookie_manager.set("app_settings", json.dumps(settings_to_save), 
                          expires_at=datetime.datetime.now() + datetime.timedelta(days=365))
        
        st.session_state.cookies_initialized = True
        st.success("Erfolgreich gespeichert! Die Seite lädt in Kürze neu...")
        time.sleep(1.2) # Longer wait to ensure cookie is set in browser
        st.rerun()

    st.divider()
    debug_mode = st.checkbox("Debug-Modus", value=False)
    if debug_mode:
        st.write("--- DEBUG PERSISTENCE ---")
        st.write("Initialized FLAG:", st.session_state.get("cookies_initialized"))
        st.write("Sync Msg:", st.session_state.get("debug_sync_msg", "None"))
        st.write("Sync Waits:", st.session_state.get("sync_waits"))
        st.write("Current Session State:", {k: st.session_state.get(k) for k in DEFAULT_SETTINGS})
        st.write("Image Server Status:", img_server_url)
        st.write("Cookies Raw:", cookies)
    explain_mode = False
    if debug_mode:
        st.info(f"Root: `{IMAGE_ROOT}`")
        explain_mode = st.checkbox("Zeige Query Plan (EXPLAIN ANALYZE)", value=False)

def get_json_field(path_list):
    """Helper für SQL JSON Access"""
    # path_list = ['definition', 'data', 'description'] -> definition->'data'->>'description'
    if not path_list: return ""
    
    col = path_list[0]
    rest = path_list[1:]
    
    sql_frag = col
    for i, key in enumerate(rest):
        arrow = "->>" if i == len(rest) - 1 else "->"
        sql_frag += f"{arrow}'{key}'"
    return sql_frag

def build_search_conditions(fields_to_search):
    """Baut die WHERE Conditions basierend auf fields_to_search"""
    if not fields_to_search: return "1=1"
    
    conditions = []
    
    # For Tags, we use Regex with boundaries (\y) by default for precision
    tag_op = "~*"
    tag_fmt = lambda col: f"{col} {tag_op} %s"
    
    # For other fields, we use standard ILIKE for flexibility
    def_fmt = lambda col: f"{col} ILIKE %s"
    
    if "name" in fields_to_search:
        conditions.append(def_fmt("name"))
        
    if "author" in fields_to_search:
        conditions.append(def_fmt("author"))
        
    if "tags" in fields_to_search:
        # Use Regex boundaries specifically for tags to avoid 'ntr' in 'country'
        conditions.append(tag_fmt("metadata->>'tags'"))
        conditions.append(tag_fmt("definition->>'tags'"))
        conditions.append(tag_fmt("definition->'data'->>'tags'"))
        
    if "description" in fields_to_search:
        conditions.append(def_fmt("definition->>'description'"))
        conditions.append(def_fmt("definition->'data'->>'description'"))
    
    if "creator_notes" in fields_to_search:
        conditions.append(def_fmt("definition->>'creator_notes'"))
        conditions.append(def_fmt("definition->'data'->>'creator_notes'"))
        
    if "first_mes" in fields_to_search:
        conditions.append(def_fmt("definition->>'first_message'"))
        conditions.append(def_fmt("definition->'data'->>'first_message'"))
        
    if "scenario" in fields_to_search:
        conditions.append(def_fmt("definition->>'scenario'"))
        conditions.append(def_fmt("definition->'data'->>'scenario'"))
        
    return " OR ".join(conditions)

def extract_card_data(definition):
    """Python-seitiges Parsen der JSON Definition"""
    data = {}
    
    def get_val(keys):
        # Rekursiver Lookup
        curr = definition
        for k in keys:
            if isinstance(curr, dict) and k in curr:
                curr = curr[k]
            else:
                return None
        return curr if isinstance(curr, str) else None

    # Priority Lists for fields
    # Description
    desc = get_val(['data', 'description']) or get_val(['description']) or get_val(['personality'])
    data['description'] = desc
    
    # Creator Notes
    notes = get_val(['data', 'creator_notes']) or get_val(['creator_notes'])
    data['creator_notes'] = notes
    
    # First Mes
    first = get_val(['data', 'first_mes']) or get_val(['first_mes'])
    data['first_mes'] = first
    
    # Scenario
    scen = get_val(['data', 'scenario']) or get_val(['scenario'])
    data['scenario'] = scen
    
    return data

if st.session_state.get('search_input') and st.session_state.selected_sources and st.session_state.selected_fields:
    # --- ROBUST VARIABLE ALIASING ---
    # We map state to locals to prevent any remaining NameErrors and keep logic clean
    selected_sources = st.session_state.selected_sources
    selected_fields = st.session_state.selected_fields
    limit = st.session_state.limit
    sort_option = st.session_state.sort_option
    token_range = st.session_state.token_range
    unlimited_tokens = st.session_state.unlimited
    
    search_query = st.session_state.search_input
    conn = get_db_connection()
    cur = conn.cursor()
    
    sql_parts = []
    params = []
    
    where_clause = build_search_conditions(st.session_state.selected_fields)
    
    # We need to wrap TAG and NON-TAG params differently
    tag_param = f"\\y{search_query}\\y"
    def_param = f"%{search_query}%"
    
    # Identify which field each %s belongs to
    chub_params = []
    if "name" in selected_fields: chub_params.append(def_param)
    if "author" in selected_fields: chub_params.append(def_param)
    if "tags" in selected_fields: 
        chub_params.extend([tag_param] * 3) # metadata, definition, data->tags
    if "description" in selected_fields: chub_params.extend([def_param] * 2)
    if "creator_notes" in selected_fields: chub_params.extend([def_param] * 2)
    if "first_mes" in selected_fields: chub_params.extend([def_param] * 2)
    if "scenario" in selected_fields: chub_params.extend([def_param] * 2)

    # Standard Fields + Full Definition
    # 1. Name, 2. Image, 3. Source, 4. Metadata, 5. Added, 6. Author, 7. Tagline, 8. Definition, 9. tokens_count
    
    # helper for tokens_count expression
    tokens_expr = "COALESCE((metadata->>'totalTokens')::int, (metadata->>'total_token_count')::int, (definition->'data'->>'total_token_count')::int, 0)"
    base_select = f"SELECT name, image_hash, '{{src}}', metadata, added, author, {{tagline_expr}}, definition, {tokens_expr} as tokens_count FROM {{table}}"

    # 1. CHUB
    if "chub" in st.session_state.selected_sources:
        q = base_select.format(src="chub", tagline_expr="NULL", table="chub_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # 2. RISU
    if "risuai" in st.session_state.selected_sources:
        q = base_select.format(src="risuai", tagline_expr="NULL", table="risuai_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # 3. TAVERN
    if "char_tavern" in st.session_state.selected_sources:
        q = base_select.format(src="tavern", tagline_expr="NULL", table="char_tavern_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # 4. GENERIC
    if "generic" in st.session_state.selected_sources:
        q = base_select.format(src="generic", tagline_expr="tagline", table="generic_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)
    
    # 5. LOREBOOKS
    if "chub_lorebook" in st.session_state.selected_sources:
        q = base_select.format(src="lorebook", tagline_expr="NULL", table="chub_lorebook_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # 6. BOORU
    if "booru" in st.session_state.selected_sources:
        booru_conds = []
        # Booru uses Regex boundaries for Tags by default
        if "name" in st.session_state.selected_fields: booru_conds.append("name ILIKE %s")
        if "author" in st.session_state.selected_fields: booru_conds.append("author ILIKE %s")
        if "description" in st.session_state.selected_fields: booru_conds.append("summary ILIKE %s")
        if "tags" in st.session_state.selected_fields: booru_conds.append("array_to_string(tags, ',') ~* %s")
        
        if not booru_conds: booru_str = "FALSE"
        else: booru_str = " OR ".join(booru_conds)
        
        # Build Booru Params
        booru_params = []
        if "name" in st.session_state.selected_fields: booru_params.append(def_param)
        if "author" in st.session_state.selected_fields: booru_params.append(def_param)
        if "description" in st.session_state.selected_fields: booru_params.append(def_param)
        if "tags" in st.session_state.selected_fields: booru_params.append(tag_param)
        
        sql_parts.append(f"""
            SELECT name, image_hash, 'booru', jsonb_build_object('tags', tags, 'totalTokens', 0), added, author, tagline, 
            jsonb_build_object('description', summary) as definition, 0 as tokens_count
            FROM booru_character_def 
            WHERE {booru_str}
        """)
        params.extend(booru_params)

    # 7. NYAIME
    if "nyaime" in st.session_state.selected_sources:
        q = base_select.format(src="nyaime", tagline_expr="NULL", table="nyaime_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # 8. WEBRING
    if "webring" in selected_sources:
        q = base_select.format(src="webring", tagline_expr="tagline", table="webring_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend(chub_params)

    # --- EXECUTE ---
    if sql_parts:
        # Wrap everything in a subquery so we can use complex ORDER BY and WHERE with UNION
        combined_sql = " UNION ALL ".join(sql_parts)
        
        # Determine Token Range Filter
        min_tokens, max_tokens = token_range
        if not unlimited_tokens:
            range_cond = f"WHERE tokens_count BETWEEN {min_tokens} AND {max_tokens}"
        else:
            range_cond = f"WHERE tokens_count >= {min_tokens}"

        # Prepare full results query with total count
        full_count_sql = f"SELECT *, COUNT(*) OVER() as full_count FROM ({combined_sql}) AS search_results {range_cond}"
        full_sql = full_count_sql
        
        # Sortierung
        s_opt = st.session_state.sort_option
        if s_opt == "Neueste zuerst": full_sql += " ORDER BY added DESC NULLS LAST"
        elif s_opt == "Älteste zuerst": full_sql += " ORDER BY added ASC NULLS LAST"
        elif s_opt == "Token Count (Viel)": 
            # Nutze tokens_count column
            full_sql += " ORDER BY tokens_count DESC"
        elif s_opt == "Token Count (Wenig)": 
            # Unbekannte (0) ans Ende
            full_sql += " ORDER BY CASE WHEN tokens_count = 0 THEN 9999999 ELSE tokens_count END ASC"
        else: full_sql += " ORDER BY name ASC"
        
        # Pagination Calculation
        offset = st.session_state.page * st.session_state.limit
        
        full_sql += f" LIMIT {st.session_state.limit} OFFSET {offset}"
        
        try:
            # DEBUG: EXPLAIN MODE
            if debug_mode:
                # Show raw SQL
                st.caption("🛠️ Generated SQL:")
                st.code(cur.mogrify(full_sql, tuple(params)).decode('utf-8'), language="sql")
                
            if explain_mode:
                explain_sql = "EXPLAIN ANALYZE " + full_sql
                with st.expander("🔍 Database Query Plan", expanded=True):
                    try:
                        cur.execute(explain_sql, tuple(params))
                        plan = cur.fetchall()
                        plan_str = "\n".join([row[0] for row in plan])
                        st.code(plan_str, language="sql")
                    except Exception as ex:
                        st.error(f"Explain fehlgeschlagen: {ex}")

            with st.spinner(f"Lade Seite {st.session_state.page + 1}..."):
                start_time = time.time()
                # Nutze cached query um Doppel-Runs bei Download zu vermeiden
                rows = run_query_cached(full_sql, tuple(params))
                end_time = time.time()
                
                # Spinner ends
                total_matches = rows[0][-1] if rows else 0
                total_pages = math.ceil(total_matches / limit) if total_matches > 0 else 1
            
            # Mark this position as scroll target for page changes
            # We use a simple JS injection to force scroll to top
            # STRAEGY CHANGE: Use scrollIntoView on the #top-marker element we created earlier
            js = f"""
            <script>
                // Page: {st.session_state.page} - {time.time()}
                try {{
                    // 1. Try scrolling the view container (Streamlit specific)
                    var viewContainer = window.parent.document.querySelector('[data-testid="stAppViewContainer"]');
                    if (viewContainer) {{
                        viewContainer.scrollTop = 0;
                        console.log("Scrolled view container to 0");
                    }}
                    
                    // 2. Also try scrolling to the marker as backup
                    var marker = window.parent.document.getElementById("top-marker");
                    if (marker) {{
                        marker.scrollIntoView({{behavior: "auto", block: "start"}});
                        console.log("Scrolled to marker");
                    }} 
                }} catch (e) {{
                    console.log("Scroll failed: " + e);
                }}
            </script>
            """
            components.html(js, height=0, width=0)
            
            # --- RENDER RESULTS IN GRID ---
            # Use 2-column rows for perfectly aligned starting heights
            for i in range(0, len(rows), 2):
                grid_cols = st.columns(2, gap="medium")
                
                # Check two indices: i and i+1
                for j in [0, 1]:
                    idx = i + j
                    if idx >= len(rows):
                        break
                        
                    with grid_cols[j]:
                        row = rows[idx]
                        name, img_hash, src, metadata, added, author, tagline, definition, tokens_count, full_count = row
                        
                        # --- DATA PREP ---
                        real_path, checked_paths = get_image_path(img_hash, debug=debug_mode)
                        card_data = extract_card_data(definition) if definition else {}
                        
                        summary_text = card_data.get('creator_notes') or tagline or ""
                        if not summary_text and card_data.get('description'):
                            desc = card_data['description'].split('\n')[0]
                            summary_text = desc[:200] + "..." if len(desc) > 200 else desc

                        # CLASSIC SPLIT: Image/Buttons Left (C1), Info/Tags Right (C2)
                        c1, c2 = st.columns([2, 4.5])
                        
                        with c1:
                            # 1. URL Resolution
                            # 1. URL Resolution
                            # PRIORITY 1: Configured Base URL
                            if IMAGE_SERVER_BASE_URL:
                                srv_url = IMAGE_SERVER_BASE_URL
                            # PRIORITY 2: Auto-detected URL from verify_server functionality
                            elif img_server_url and not img_server_url.startswith("Error"):
                                srv_url = img_server_url
                            # PRIORITY 3: Fallback logic
                            else:
                                srv_url = f"http://{socket.gethostname()}:{8505}"
                            direct_url = None
                            
                            if real_path:
                                rel_path = os.path.relpath(real_path, IMAGE_ROOT).replace("\\", "/")
                                direct_url = f"{srv_url}/{rel_path}"
                                # Ensure extension for server (it strips it, but usually browsers like extensions)
                                if not direct_url.lower().endswith((".png", ".webp", ".jpg", ".jpeg")):
                                    direct_url += ".png"
                            
                            # 2. Render Image with Overlay
                            if direct_url:
                                badges = get_safety_badges(metadata)
                                
                                img_html = f'<div class="image-wrapper"><img src="{direct_url}" />'
                                
                                if badges:
                                    badge_html = ""
                                    for b in badges:
                                        badge_html += f'<div class="safety-badge {b["class"]}">{b["label"]}</div>'
                                    
                                    img_html += f'<div class="badge-overlay">{badge_html}</div>'
                                
                                img_html += '</div>'
                                
                                st.markdown(img_html, unsafe_allow_html=True)
                            elif real_path:
                                st.image(real_path, width='stretch')
                            else:
                                st.markdown(f"🖼️ *Bild fehlt*\n\n`{img_hash[:6]}`")
                            
                            # Row 1: Downloads
                            b1, b2 = st.columns(2, gap="small")
                            if real_path and direct_url:
                                # Fetch from server to get embedded metadata
                                file_data = None
                                try:
                                    with urllib.request.urlopen(direct_url) as response:
                                        file_data = response.read()
                                except:
                                    # Fallback to local file (no metadata but better than nothing)
                                    with open(real_path, "rb") as f:
                                        file_data = f.read()
                                
                                if file_data:
                                    with b1: st.download_button("💾 PNG", file_data, file_name=f"{name}.png", key=f"dl_{img_hash}_{idx}")
                            
                            if definition:
                                json_str = json.dumps(definition, indent=2, ensure_ascii=False)
                                with b2: st.download_button("💾 JSON", json_str, file_name=f"{name}.json", key=f"dl_json_{img_hash}_{idx}")

                            # Row 2: SillyTavern Link (using st.code for reliable copy)
                            if direct_url:
                                
                                # Use st.expander + st.code for built-in copy functionality
                                with st.expander("🔗 SillyTavern Import Link"):
                                    st.code(direct_url, language="text")

                        with c2:
                            # HEADER: Title & Tokens
                            tokens_label = f"{tokens_count} T" if tokens_count > 0 else "0 T"
                            token_badge = f"<span class='token-badge'>{tokens_label}</span>"
                            st.markdown(f"<div class='char-title' style='font-size: 1.15rem;'>{name} {token_badge}</div>", unsafe_allow_html=True)
                            if author:
                                st.markdown(f"<div class='char-author'>by {author}</div>", unsafe_allow_html=True)
                            
                            # SUMMARY BOX
                            if summary_text:
                                safe_summary = clean_html(summary_text)
                                st.markdown("<div class='char-preview-box' style='max-height: 200px; font-size: 0.85rem;'>", unsafe_allow_html=True)
                                render_preview_html(safe_summary)
                                st.markdown("</div>", unsafe_allow_html=True)

                            # TAGS & DETAILS
                            tags_raw = metadata.get('tags') if metadata else []
                            tags_list = format_tags(tags_raw)
                            if not tags_list and definition:
                                dev_tags = definition.get('data', {}).get('tags') or definition.get('tags') if isinstance(definition, dict) else None
                                if dev_tags: tags_list = format_tags(dev_tags)

                            if tags_list:
                                disp_lim = 8
                                m_tags = tags_list[:disp_lim]
                                if search_query and any(search_query.lower() == t.lower() for t in tags_list):
                                    m_tag = next(t for t in tags_list if t.lower() == search_query.lower())
                                    if m_tag not in m_tags: m_tags[-1] = m_tag
                                
                                tags_html = f'<div class="tags-container" style="margin-bottom: 15px;">{render_badges(m_tags)}'
                                if len(tags_list) > disp_lim:
                                    r_tags = [t for t in tags_list if t not in m_tags]
                                    tags_html += f'<details class="tag-details"><summary class="tag-badge">+{len(r_tags)} weitere</summary>{render_badges(r_tags)}</details>'
                                tags_html += '</div>'
                                st.markdown(tags_html, unsafe_allow_html=True)

                            with st.expander("📝 Details"):
                                # Use tabs for clean detail view
                                content_map = {}
                                if card_data.get('description'): content_map["Desc"] = card_data['description']
                                if card_data.get('first_mes'): content_map["First"] = card_data['first_mes']
                                tab_names = list(content_map.keys()) + ["Info", "Raw"]
                                t_rows = st.tabs(tab_names)
                                t_idx = 0
                                for k in content_map:
                                    with t_rows[t_idx]: st.markdown(content_map[k])
                                    t_idx += 1
                                with t_rows[t_idx]:
                                    st.table({"Added": added.strftime("%Y-%m-%d") if added else "?", "Source": src})
                                    t_idx += 1
                                with t_rows[t_idx]: st.json(metadata)
                        
                        # Add visual separator between cards
                        st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)
            
            # --- PAGINATION CONTROLS (Bottom) ---
            try:
                col_b_res, col_b_prev, col_b_page, col_b_next = st.columns([3, 0.6, 1.2, 0.6], vertical_alignment="center")
            except:
                col_b_res, col_b_prev, col_b_page, col_b_next = st.columns([3, 0.6, 1.2, 0.6])
            
            with col_b_res:
                st.markdown(f'<p class="pag-label">S. {st.session_state.page+1} / {total_pages}</p>', unsafe_allow_html=True)
            with col_b_prev:
                 if st.session_state.page > 0:
                     if st.button("⬅️", key="prev_bottom"):
                         change_page(st.session_state.page - 1)
                         st.rerun()
            with col_b_page:
                if total_pages > 1:
                    st.number_input("Seite", 1, total_pages, key="p_jump_b", label_visibility="collapsed", on_change=lambda: change_page(st.session_state.p_jump_b - 1))
            with col_b_next:
                 if st.session_state.page < total_pages - 1:
                     if st.button("➡️", key="next_bottom"):
                         change_page(st.session_state.page + 1)
                         st.rerun()

        except Exception as e:
            st.error(f"Fehler: {e}")
            if debug_mode: st.code(full_sql)

    cur.close()

elif not st.session_state.selected_sources:
    st.warning("Wähle eine Quelle.")
else:
    st.info("Suche starten...")
