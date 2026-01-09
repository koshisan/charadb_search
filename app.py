import streamlit as st
import psycopg2
import os
import json
import datetime

# Importieren der Konfiguration aus config.py
try:
    from config import IMAGE_ROOT, DB_CONFIG
except ImportError:
    st.error("Konfigurationsdatei 'config.py' nicht gefunden. Bitte erstelle sie basierend auf dem Beispiel.")
    st.stop()

# --- SETUP & STYLES ---
st.set_page_config(layout="wide", page_title="Char Archive Ultimate", page_icon="🗃️")

# Check Query Params for Tag Search (Click on Badge)
if "q" in st.query_params:
    st.session_state.search_input = st.query_params["q"]
    st.query_params.clear()

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
</style>
""", unsafe_allow_html=True)

# --- FUNKTIONEN ---

@st.cache_resource
def get_db_connection():
    """Hält die DB-Verbindung offen, damit es schneller geht"""
    return psycopg2.connect(**DB_CONFIG)

def get_image_path(image_hash, debug=False):
    """Findet das Bild im Sharding-Dschungel"""
    if not image_hash: return None, []
    
    extensions = ["", ".png", ".webp", ".jpg", ".jpeg"]
    candidates = []
    
    # 1. Nested Sharding (hashed-data/a/b/abcde...)
    if len(image_hash) >= 2:
        base = os.path.join(IMAGE_ROOT, "hashed-data", image_hash[0], image_hash[1], image_hash)
        candidates.extend([base + ext for ext in extensions])

    # 2. Simple Sharding (hashed-data/a/abcde...)
    if len(image_hash) >= 1:
        base = os.path.join(IMAGE_ROOT, "hashed-data", image_hash[0], image_hash)
        candidates.extend([base + ext for ext in extensions])
        
    # 3. Flat (hashed-data/abcde...)
    base = os.path.join(IMAGE_ROOT, "hashed-data", image_hash)
    candidates.extend([base + ext for ext in extensions])
    
    for path in candidates:
        if os.path.exists(path) and os.path.isfile(path):
            return path, candidates
            
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
            tags = tags_data.split(',')
    return [str(t).strip().replace('"', '') for t in tags if str(t).strip()]

def render_badges(tags_list):
    html_str = ""
    for t in tags_list[:20]: 
        # Link to ?q=TAGNAME to trigger search on reload
        html_str += f'<a href="?q={t}" target="_self" class="tag-badge">{t}</a>'
    if len(tags_list) > 20:
        html_str += f'<span class="tag-badge" style="cursor:default">+{len(tags_list)-20} more</span>'
    return html_str

# --- HAUPTBEREICH ---

st.title("🗃️ Character Archive: Local Edition")

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
    
    # 1. Source Selection
    selected_sources = st.multiselect(
        "Quellen", 
        options=list(source_map.keys()), 
        format_func=lambda x: source_map[x],
        default=["chub", "risuai", "char_tavern", "chub_lorebook"]
    )
    
    st.divider()
    
    # 2. Search Field Selection
    st.subheader("Suchfelder")
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
    
    st.divider()
    
    sort_option = st.selectbox("Sortierung", ["Neueste zuerst", "Älteste zuerst", "Name (A-Z)"])
    limit = st.slider("Anzahl Ergebnisse", 10, 100, 20)
    
    st.divider()
    debug_mode = st.checkbox("Debug-Modus", value=False)
    if debug_mode:
        st.info(f"Root: `{IMAGE_ROOT}`")

# Search
search_query = st.text_input("🔍 Suche...", placeholder="Suchbegriff eingeben...")

# --- LOGIK ---

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
        conditions.append("author ILIKE %s")
        
    if "tags" in selected_fields:
        # Check metadata tags (standard) and definition tags (sometimes)
        conditions.append("metadata->>'tags' ILIKE %s")
        
    # Complex Fields (check V1 and V2 locations)
    
    # Description
    if "description" in selected_fields:
        conditions.append("definition->>'description' ILIKE %s")
        conditions.append("definition->'data'->>'description' ILIKE %s")
        # Legacy/Other
        conditions.append("definition->>'personality' ILIKE %s")
    
    # Creator Notes
    if "creator_notes" in selected_fields:
        conditions.append("definition->>'creator_notes' ILIKE %s")
        conditions.append("definition->'data'->>'creator_notes' ILIKE %s")
        
    # First Mes
    if "first_mes" in selected_fields:
        conditions.append("definition->>'first_mes' ILIKE %s")
        conditions.append("definition->'data'->>'first_mes' ILIKE %s")
        
    # Scenario
    if "scenario" in selected_fields:
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
    # 1. Name, 2. Image, 3. Source, 4. Metadata, 5. Added, 6. Author, 7. Tagline, 8. Definition
    
    base_select = "SELECT name, image_hash, '{src}', metadata, added, author, {tagline_expr}, definition FROM {table}"
    
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
            SELECT name, image_hash, 'booru', jsonb_build_object('tags', tags), added, author, tagline, 
            jsonb_build_object('description', summary) as definition
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
        full_sql = " UNION ALL ".join(sql_parts)
        
        if sort_option == "Neueste zuerst": full_sql += " ORDER BY added DESC"
        elif sort_option == "Älteste zuerst": full_sql += " ORDER BY added ASC"
        else: full_sql += " ORDER BY name ASC"
        
        full_sql += f" LIMIT {limit}"
        
        try:
            with st.spinner("Lade Daten..."):
                cur.execute(full_sql, tuple(params))
                rows = cur.fetchall()
            
            st.caption(f"{len(rows)} Ergebnisse")
            
            for row in rows:
                name, img_hash, src, metadata, added, author, tagline, definition = row
                
                # --- DATA PREP ---
                real_path, checked_paths = get_image_path(img_hash, debug=debug_mode)
                
                # Extract fields from definition
                card_data = extract_card_data(definition) if definition else {}
                
                # Tags extraction (Metadata preferred, fallback to definition)
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
                                st.download_button("💾 PNG", f, file_name=f"{name}.png", key=f"dl_{img_hash}")

                    with col2:
                        # HEADER
                        author_html = f"<span class='char-author'>by {author}</span>" if author else ""
                        st.markdown(f"<div class='char-title'>{name} &nbsp; {author_html}</div>", unsafe_allow_html=True)
                        
                        # TAGS directly in Overview (Render limited amount)
                        if tags_list:
                            st.markdown(render_badges(tags_list[:12]), unsafe_allow_html=True)
                        elif debug_mode:
                            st.caption("⚠️ Keine Tags gefunden (Check Raw Definition)")

                        
                        # SUMMARY (Creator Notes / Tagline)
                        if summary_text:
                            st.info(summary_text) # Info box for summary looks decent
                        
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

        except Exception as e:
            st.error(f"Fehler: {e}")
            if debug_mode: st.code(full_sql)

    cur.close()

elif not selected_sources:
    st.warning("Wähle eine Quelle.")
else:
    st.info("Suche starten...")


