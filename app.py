import streamlit as st
import psycopg2
import os
import json
import datetime
import time
import math
import re

# Importieren der Konfiguration aus config.py
try:
    from config import IMAGE_ROOT, DB_CONFIG
except ImportError:
    st.error("Konfigurationsdatei 'config.py' nicht gefunden. Bitte erstelle sie basierend auf dem Beispiel.")
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

# Check Query Params for Tag Search (Click on Badge)
if "q" in st.query_params:
    st.session_state.search_input = st.query_params["q"]
    st.session_state.page = 0
    st.session_state.p_jump = 1
    st.session_state.p_jump_b = 1

st.markdown("""
<style>
    /* Global Styles */
    .block-container {
        padding-top: 2rem;
    }
    
    /* Card Container */
    .char-card {
        background-color: #1e1e1e;
        border: 1px solid #333;
        border-radius: 10px;
        padding: 15px;
        margin-bottom: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    
    /* Typography */
    .char-title {
        font-size: 1.4em;
        font-weight: bold;
        color: #fff;
        margin-bottom: 0px;
    }
    .char-author {
        font-size: 0.9em;
        color: #aaa;
        margin-bottom: 10px;
    }
    .char-tagline {
        font-style: italic;
        color: #ddd;
        border-left: 3px solid #ff4b4b;
        padding-left: 10px;
        margin: 10px 0;
    }
    
    /* Badges */
    .tag-badge {
        background-color: #262730;
        color: #e0e0e0 !important;
        padding: 2px 8px;
        border-radius: 4px;
        border: 1px solid #4a4a4a;
        font-size: 0.75em;
        margin-right: 4px;
        margin-bottom: 4px;
        display: inline-block;
        font-family: monospace;
        text-decoration: none;
    }
    .tag-badge:hover {
        background-color: #3e404b;
        border-color: #ff4b4b;
        color: #fff !important;
    }
    
    /* Images */
    .char-image-container img {
        border-radius: 8px;
        width: 100%;
        object-fit: cover;
    }

    /* Search Button Alignment - Final Fix */
    [data-testid="stForm"] {
        border: none !important;
        padding: 0 !important;
    }
    /* Ensure the column content is centered/bottom aligned */
    div.stButton > button {
        margin-top: 1.5rem !important;
        width: 100%;
        height: 100%;
    }

    /* HTML Preview Box - Improved visibility & size */
    .char-preview-box {
        max-height: 500px;
        overflow-y: auto;
        padding: 15px;
        background: rgba(128, 128, 128, 0.08);
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 8px;
        font-size: 0.95em;
        line-height: 1.6;
        margin-top: 10px;
        margin-bottom: 15px;
    }
    .char-preview-box::-webkit-scrollbar {
        width: 6px;
    }
    .char-preview-box::-webkit-scrollbar-thumb {
        background: #555;
        border-radius: 3px;
    }
    .char-preview-box::-webkit-scrollbar-track {
        background: transparent;
    }
    
    /* Pagination Layout Alignment */
    .pag-label {
        display: flex;
        align-items: center;
        height: 100%;
        margin: 0 !important;
        font-size: 0.85rem;
        color: #888;
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

def change_page(new_page):
    """Callback für Paginierung - Synchronisiert alle States"""
    st.session_state.page = new_page
    st.session_state.p_jump = new_page + 1
    st.session_state.p_jump_b = new_page + 1

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
    for t in tags_list[:20]: 
        # Link to ?q=TAGNAME to trigger search on reload
        enc_tag = t # URL encoding happens automatically by browser usually, but simple is fine
        html_str += f'<a href="?q={enc_tag}" target="_self" class="tag-badge">{t}</a>'
    if len(tags_list) > 20:
        html_str += f'<span class="tag-badge" style="cursor:default">+{len(tags_list)-20} more</span>'
    return html_str

# --- HAUPTBEREICH ---

st.title("🗃️ Character Archive: Local Edition")

# Init Session State for Pagination
if 'page' not in st.session_state: st.session_state.page = 0
if 'last_query' not in st.session_state: st.session_state.last_query = ""

# Sidebar
with st.sidebar:
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
    
    # Results limit slider - now max 250
    limit = st.slider("Anzahl Ergebnisse", 10, 250, 20)
    
    st.divider()
    
    # Use a Form for sidebar settings to avoid immediate re-run
    with st.form("settings_form"):
        # 1. Source Selection
        selected_sources = st.multiselect(
            "Quellen", 
            options=list(source_map.keys()), 
            format_func=lambda x: source_map[x],
            default=["chub", "risuai", "char_tavern", "chub_lorebook"]
        )
        
        # 2. Search Field Selection
        search_options = {
            "name": "Name",
            "tags": "Tags",
            "description": "Beschreibung / Summary",
            "creator_notes": "Creator Notes",
            "first_mes": "First Message",
            "scenario": "Scenario",
            "author": "Autor"
        }
        
        selected_fields = st.multiselect(
            "Suche in...",
            options=list(search_options.keys()),
            format_func=lambda x: search_options[x],
            default=["name", "tags", "description", "creator_notes"]
        )
        
        sort_option = st.selectbox("Sortierung", [
            "Neueste zuerst", 
            "Älteste zuerst", 
            "Name (A-Z)", 
            "Token Count (Viel)", 
            "Token Count (Wenig)"
        ])

        st.divider()
        st.write("📊 Token-Filter")
        token_range = st.slider("Token-Bereich", 0, 16000, (0, 8000), step=100)
        unlimited_tokens = st.checkbox("Nach oben offen", value=False)
        
        apply_btn = st.form_submit_button("Einstellungen anwenden")

    st.divider()
    debug_mode = st.checkbox("Debug-Modus", value=False)
    explain_mode = False
    if debug_mode:
        st.info(f"Root: `{IMAGE_ROOT}`")
        explain_mode = st.checkbox("Zeige Query Plan (EXPLAIN ANALYZE)", value=False)

# Search Input Form
with st.form("search_form"):
    # Using vertical_alignment="bottom" for modern Streamlit, or fallback layout
    try:
        col_input, col_submit = st.columns([5, 1], vertical_alignment="bottom")
    except:
        col_input, col_submit = st.columns([5, 1])
    
    with col_input:
        search_query = st.text_input("🔍 Suche...", placeholder="Suchbegriff eingeben...", value=st.session_state.get('search_input', ""))
    with col_submit:
        search_btn = st.form_submit_button("Suche", use_container_width=True)
        if search_btn:
            st.session_state.search_input = search_query
            st.session_state.page = 0
            # Reset jump widgets
            st.session_state.p_jump = 1
            st.session_state.p_jump_b = 1
            st.rerun()

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

def build_search_conditions(query_param_name):
    """Baut die WHERE Conditions basierend auf selected_fields"""
    if not selected_fields: return "1=1" # Fallback
    
    conditions = []
    
    # Mapping Field -> SQL Expression(s) (COALESCE Logic handled via OR checking mostly)
    
    if "name" in selected_fields:
        conditions.append("name ILIKE %s")
        
    if "author" in selected_fields:
        # Indexed: idx_chub_author_trgm -> author
        conditions.append("author ILIKE %s")
        
    if "tags" in selected_fields:
        # Check metadata tags (standard) and definition tags
        conditions.append("metadata->>'tags' ILIKE %s")
        conditions.append("definition->>'tags' ILIKE %s")
        conditions.append("definition->'data'->>'tags' ILIKE %s")
        
    # Complex Fields (check V1 and V2 locations)
    
    # Description
    if "description" in selected_fields:
        # Indexed: idx_chub_desc_trgm -> definition->>'description' (for V1 Cards)
        conditions.append("definition->>'description' ILIKE %s")
        conditions.append("definition->'data'->>'description' ILIKE %s")
        # conditions.append("definition->>'personality' ILIKE %s")
    
    # Creator Notes
    if "creator_notes" in selected_fields:
        # Indexed: idx_chub_notes_trgm -> definition->>'creator_notes'
        conditions.append("definition->>'creator_notes' ILIKE %s")
        conditions.append("definition->'data'->>'creator_notes' ILIKE %s")
        # Legacy/Other
        # conditions.append("definition->>'notes' ILIKE %s")
        
    # First Mes
    if "first_mes" in selected_fields:
        # Indexed: idx_chub_firstmes_trgm -> definition->>'first_message'
        # CHECK: User index uses 'first_message', my code used 'first_mes'
        conditions.append("definition->>'first_message' ILIKE %s")
        conditions.append("definition->'data'->>'first_message' ILIKE %s")
        # Fallback to 'first_mes' just in case
        conditions.append("definition->>'first_mes' ILIKE %s")
        conditions.append("definition->'data'->>'first_mes' ILIKE %s")
        
    # Scenario
    if "scenario" in selected_fields:
        # Indexed: idx_chub_scenario_trgm -> definition->>'scenario'
        conditions.append("definition->>'scenario' ILIKE %s")
        conditions.append("definition->'data'->>'scenario' ILIKE %s")
        
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

if search_query and selected_sources and selected_fields:
    conn = get_db_connection()
    cur = conn.cursor()
    
    sql_parts = []
    params = []
    
    where_clause = build_search_conditions("search_query")
    # Count how many placeholders (%) are in the where_clause
    num_params = where_clause.count("%s")
    
    # Standard Fields + Full Definition
    # 1. Name, 2. Image, 3. Source, 4. Metadata, 5. Added, 6. Author, 7. Tagline, 8. Definition, 9. tokens_count
    
    # helper for tokens_count expression
    tokens_expr = "COALESCE((metadata->>'totalTokens')::int, (metadata->>'total_token_count')::int, (definition->'data'->>'total_token_count')::int, 0)"
    base_select = f"SELECT name, image_hash, '{{src}}', metadata, added, author, {{tagline_expr}}, definition, {tokens_expr} as tokens_count FROM {{table}}"
    
    # 1. CHUB
    if "chub" in selected_sources:
        q = base_select.format(src="chub", tagline_expr="NULL", table="chub_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

    # 2. RISU
    if "risuai" in selected_sources:
        q = base_select.format(src="risuai", tagline_expr="NULL", table="risuai_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

    # 3. TAVERN
    if "char_tavern" in selected_sources:
        q = base_select.format(src="tavern", tagline_expr="NULL", table="char_tavern_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

    # 4. GENERIC
    if "generic" in selected_sources:
        q = base_select.format(src="generic", tagline_expr="tagline", table="generic_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)
    
    # 5. LOREBOOKS
    if "chub_lorebook" in selected_sources:
        q = base_select.format(src="lorebook", tagline_expr="NULL", table="chub_lorebook_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

    # 6. BOORU - Special Handling (Summary as Description substitute, tags array)
    if "booru" in selected_sources:
        # Booru hat kein 'definition' JSONB wie die anderen, sondern festes Schema. 
        # Wir simulieren ein definition object für consistency
        # Tagline ist vorhanden.
        
        # Mapping für Search Logic ist trickier hier. Wir vereinfachen:
        # Booru hat: name, summary, tagline, author, tags
        booru_conds = []
        if "name" in selected_fields: booru_conds.append("name ILIKE %s")
        if "author" in selected_fields: booru_conds.append("author ILIKE %s")
        if "description" in selected_fields: booru_conds.append("summary ILIKE %s") # Summary = Desc
        if "tags" in selected_fields: booru_conds.append("array_to_string(tags, ',') ILIKE %s")
        if "scenario" in selected_fields: booru_conds.append("FALSE") # Gibts nicht
        
        if not booru_conds: booru_str = "FALSE"
        else: booru_str = " OR ".join(booru_conds)
        
        num_booru = booru_str.count("%s")
        
        sql_parts.append(f"""
            SELECT name, image_hash, 'booru', jsonb_build_object('tags', tags, 'totalTokens', 0), added, author, tagline, 
            jsonb_build_object('description', summary) as definition, 0 as tokens_count
            FROM booru_character_def 
            WHERE {booru_str}
        """)
        params.extend([f'%{search_query}%'] * num_booru)

    # 7. NYAIME
    if "nyaime" in selected_sources:
        q = base_select.format(src="nyaime", tagline_expr="NULL", table="nyaime_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

    # 8. WEBRING
    if "webring" in selected_sources:
        q = base_select.format(src="webring", tagline_expr="tagline", table="webring_character_def")
        q += f" WHERE {where_clause}"
        sql_parts.append(q)
        params.extend([f'%{search_query}%'] * num_params)

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
        if sort_option == "Neueste zuerst": full_sql += " ORDER BY added DESC NULLS LAST"
        elif sort_option == "Älteste zuerst": full_sql += " ORDER BY added ASC NULLS LAST"
        elif sort_option == "Token Count (Viel)": 
            # Nutze tokens_count column
            full_sql += " ORDER BY tokens_count DESC"
        elif sort_option == "Token Count (Wenig)": 
            # Unbekannte (0) ans Ende
            full_sql += " ORDER BY CASE WHEN tokens_count = 0 THEN 9999999 ELSE tokens_count END ASC"
        else: full_sql += " ORDER BY name ASC"
        
        # Pagination Calculation
        offset = st.session_state.page * limit
        
        full_sql += f" LIMIT {limit} OFFSET {offset}"
        
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
                
                # Extract total count from window function
                total_matches = rows[0][-1] if rows else 0
                total_pages = math.ceil(total_matches / limit) if total_matches > 0 else 1
                
                st.info(f"Suche ausgeführt in {end_time - start_time:.4f} Sekunden. (Insgesamt {total_matches} Treffer)")
            
            # --- PAGINATION CONTROLS (Top) ---
            try:
                col_res, col_prev, col_page, col_next = st.columns([3, 0.6, 1.2, 0.6], vertical_alignment="center")
            except:
                col_res, col_prev, col_page, col_next = st.columns([3, 0.6, 1.2, 0.6])
            
            with col_res:
                st.markdown(f'<p class="pag-label">S. {st.session_state.page+1} / {total_pages} ({total_matches} Treffer)</p>', unsafe_allow_html=True)
            with col_prev:
                if st.session_state.page > 0:
                     st.button("⬅️", key="prev_top", on_click=change_page, args=(st.session_state.page - 1,))
            with col_page:
                if total_pages > 1:
                    st.number_input("Seite", 1, total_pages, key="p_jump", label_visibility="collapsed", on_change=lambda: change_page(st.session_state.p_jump - 1))
            with col_next:
                 if st.session_state.page < total_pages - 1:
                     st.button("➡️", key="next_top", on_click=change_page, args=(st.session_state.page + 1,))
            
            for i, row in enumerate(rows):
                name, img_hash, src, metadata, added, author, tagline, definition, tokens_count, full_count = row
                
                # --- DATA PREP ---
                real_path, checked_paths = get_image_path(img_hash, debug=debug_mode)
                
                # Extract fields from definition
                card_data = extract_card_data(definition) if definition else {}
                
                # Tokens
                tokens_val = tokens_count
                tokens_label = f"{tokens_val} Tokens" if tokens_val > 0 else "Tokens Unbekannt"
                token_bg = "#ff4b4b" if tokens_val > 0 else "#666"
                token_html = f"<span style='background:{token_bg}; color:#fff; padding:2px 8px; border-radius:4px; font-size:0.85em; margin-left:10px; font-weight:bold;'>{tokens_label}</span>"
                tags_raw = metadata.get('tags') if metadata else []
                tags_list = format_tags(tags_raw)
                
                if not tags_list and definition:
                     # Fallback: Check definition -> data -> tags
                     def_tags = None
                     if isinstance(definition, dict):
                         def_tags = definition.get('data', {}).get('tags') or definition.get('tags')
                     
                     if def_tags:
                         tags_list = format_tags(def_tags)

                # Summary Text (Creator Notes preferred, else Tagline, else Desc Truncated)
                summary_text = card_data.get('creator_notes') or ""
                if not summary_text: summary_text = tagline or ""
                # Fallback to Desc First Sentence
                if not summary_text and card_data.get('description'):
                    desc = card_data['description']
                    first_sentence = desc.split('\n')[0]
                    if len(first_sentence) < 200:
                         summary_text = first_sentence
                    else:
                        summary_text = first_sentence[:200] + "..."
                
                # --- UI CARD ---
                with st.container():
                    col1, col2 = st.columns([1, 4])
                    
                    with col1:
                        if real_path:
                            st.image(real_path, use_container_width=True)
                        else:
                            st.markdown(f"🖼️ *Bild fehlt*\n\n`{img_hash[:6]}`")
                            
                        if real_path:
                             with open(real_path, "rb") as f:
                                st.download_button("💾 PNG", f, file_name=f"{name}.png", key=f"dl_{img_hash}_{i}")

                    with col2:
                        # HEADER
                        author_html = f"<span class='char-author'>by {author}</span>" if author else ""
                        
                        st.markdown(f"<div class='char-title'>{name} &nbsp; {author_html} {token_html}</div>", unsafe_allow_html=True)
                        
                        # TAGS directly in Overview (Render limited amount)
                        if tags_list:
                            st.markdown(render_badges(tags_list[:12]), unsafe_allow_html=True)
                        elif debug_mode:
                            st.caption("⚠️ Keine Tags gefunden (Check Raw Definition)")

                        
                        # SUMMARY (Creator Notes / Tagline / HTML Preview)
                        if summary_text:
                            # Wrap in scrollable box and allow HTML, but CLEAN it from autoplay
                            safe_summary = clean_html(summary_text)
                            st.markdown(f"<div class='char-preview-box'>{safe_summary}</div>", unsafe_allow_html=True)
                        
                        # EXPANDER
                        with st.expander("📝 Details anzeigen"):
                            
                            tabs = []
                            tab_names = []
                            
                            # Logik: Nur Tabs erstellen für Content der da ist
                            content_map = {}
                            
                            if card_data.get('description'): 
                                content_map["Beschreibung"] = card_data['description']
                            if card_data.get('first_mes'): 
                                content_map["First Message"] = card_data['first_mes']
                            if card_data.get('scenario'): 
                                content_map["Scenario"] = card_data['scenario']
                                
                            # Immer da: Node Info, Metadata
                            
                            if content_map:
                                tab_names.extend(content_map.keys())
                            
                            tab_names.extend(["Node Info", "Raw Metadata"])
                            
                            current_tabs = st.tabs(tab_names)
                            
                            # Content füllen
                            idx = 0
                            
                            # 1. Content Text Tabs
                            for key in content_map:
                                with current_tabs[idx]:
                                    st.markdown(content_map[key])
                                idx += 1
                                
                            # 2. Node Info
                            with current_tabs[idx]:
                                info_data = {
                                    "Added": added.strftime("%Y-%m-%d %H:%M") if added else "Unknown",
                                    "Source": src,
                                }
                                if metadata:
                                    if 'total_token_count' in metadata: info_data['Tokens'] = metadata['total_token_count']
                                    if 'star_count' in metadata: info_data['Stars'] = metadata['star_count']
                                    if 'download_count' in metadata: info_data['Downloads'] = metadata['download_count']
                                    if 'date_last_updated' in metadata: info_data['Updated'] = metadata['date_last_updated']
                                st.table(info_data)
                                idx += 1
                                
                            # 3. Raw Metadata (+ Definition Debug)
                            with current_tabs[idx]:
                                col_meta, col_def = st.columns(2)
                                with col_meta:
                                    st.caption("Metadata")
                                    st.json(metadata)
                                with col_def:
                                    st.caption("Definition (JSON)")
                                    st.json(definition)
                                    if debug_mode: 
                                        st.write("Paths:", checked_paths)

                    st.markdown("---")
            
            # --- PAGINATION CONTROLS (Bottom) ---
            try:
                col_b_res, col_b_prev, col_b_page, col_b_next = st.columns([3, 0.6, 1.2, 0.6], vertical_alignment="center")
            except:
                col_b_res, col_b_prev, col_b_page, col_b_next = st.columns([3, 0.6, 1.2, 0.6])
            
            with col_b_res:
                st.markdown(f'<p class="pag-label">S. {st.session_state.page+1} / {total_pages}</p>', unsafe_allow_html=True)
            with col_b_prev:
                 if st.session_state.page > 0:
                     st.button("⬅️", key="prev_bottom", on_click=change_page, args=(st.session_state.page - 1,))
            with col_b_page:
                if total_pages > 1:
                    st.number_input("Seite", 1, total_pages, key="p_jump_b", label_visibility="collapsed", on_change=lambda: change_page(st.session_state.p_jump_b - 1))
            with col_b_next:
                 if st.session_state.page < total_pages - 1:
                     st.button("➡️", key="next_bottom", on_click=change_page, args=(st.session_state.page + 1,))

        except Exception as e:
            st.error(f"Fehler: {e}")
            if debug_mode: st.code(full_sql)

    cur.close()

elif not selected_sources:
    st.warning("Wähle eine Quelle.")
else:
    st.info("Suche starten...")
