import http.server
import socketserver
import threading
import os
import io
import json
import base64
import psycopg2
import socket
from http import HTTPStatus
from PIL import Image, PngImagePlugin

# Try to import config, assuming this file is in the same directory as config.py
try:
    from config import IMAGE_ROOT, DB_CONFIG
except ImportError:
    print("Error: config.py not found.")
    IMAGE_ROOT = "."
    DB_CONFIG = {}

class ImageRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # We need to pass the directory explicitly to SimpleHTTPRequestHandler
        # But we also intercept requests before they hit the default handler logic for PNGs
        super().__init__(*args, directory=IMAGE_ROOT, **kwargs)

    def do_GET(self):
        # Decode path to handle special characters if any
        path = self.path
        
        # We only care about PNGs that surely need metadata
        # The URL structure is expected to be: /hashed-data/e/b/0/eb0c83ae....png
        # or simplified versions supported by app.py logic.
        if path.lower().endswith(".png"):
            try:
                self.serve_image_with_metadata(path)
                return
            except Exception as e:
                # If something goes wrong in the smart serving, 
                # we return an error as requested by the user.
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Error serving image: {str(e)}")
                return

        # For non-png or other requests, fall back to default behavior (serving raw files)
        # However, the user request specifically talked about the "additional webserver" for files.
        # If we want to be strict, we can keep using default for everything else (e.g. .jpg)
        return super().do_GET()

    def serve_image_with_metadata(self, path):
        # 1. Reconstruct Hash from Path
        # Example: /hashed-data/e/b/0/c83ae23e0e416d7a35ff7e6bdf8af.png
        # We need to be careful about the mount point. 
        # path starts with /. 
        
        # Remove leading slash
        clean_path = path.lstrip('/')
        
        # User confirmed all files on disk are extensionless
        if clean_path.lower().endswith(".png"):
            clean_path = os.path.splitext(clean_path)[0]

        # Check if file exists on disk first
        full_path = os.path.join(IMAGE_ROOT, clean_path)
        if not os.path.exists(full_path):
            self.send_error(HTTPStatus.NOT_FOUND, "File not found on disk")
            return

        # Try to parse the hash from the path components
        # We assume the standard structure created by the app
        parts = clean_path.split('/')
        
        image_hash = None
        
        # Strategy: Look for the 'hashed-data' segment and parse relative to it
        if "hashed-data" in parts:
            idx = parts.index("hashed-data")
            # The parts after hashed-data are the sharding + filename
            # /hashed-data/e/b/0/c83ae....png  -> ['e', 'b', '0', 'c83ae....png']
            # Reconstructing hash: e + b + 0 + stem(c83ae...)
            
            # We need at least the filename to get the rest of the hash
            remainder = parts[idx+1:]
            if len(remainder) >= 1:
                filename = remainder[-1]
                stem = os.path.splitext(filename)[0]
                
                # If we have shards (e, b, 0), prepend them
                shards = remainder[:-1]
                image_hash = "".join(shards) + stem
        else:
            # Fallback: Just take the filename stem if it looks like a hash (32 chars usually)
            filename = parts[-1]
            stem = os.path.splitext(filename)[0]
            if len(stem) >= 32:
                 image_hash = stem
        
        if not image_hash:
            self.send_error(HTTPStatus.BAD_REQUEST, "Could not extract image hash from URL")
            return

        # 2. Fetch Metadata from DB
        character_data = self.get_character_definition(image_hash)
        
        if not character_data:
             self.send_error(HTTPStatus.NOT_FOUND, f"No character definition found for hash: {image_hash}")
             return

        # 3. Embed Metadata and Serve
        try:
             # Load Image
             with Image.open(full_path) as img:
                 img.load() # Force load image data
                 
                 # Create PngInfo for metadata
                 metadata = PngImagePlugin.PngInfo()
                 
                 # Preserve existing textual chunks? Usually we want to OVERWRITE or ADD the CCV2 formatted one.
                 # Tavern uses 'chara' key with base64 encoded JSON.
                 
                 # Prepare the JSON. The DB returns the 'definition' column which is already a dict (jsonb)
                 json_str = json.dumps(character_data)
                 b64_data = base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
                 
                 metadata.add_text("chara", b64_data)
                 
                 # Save to buffer
                 output = io.BytesIO()
                 img.save(output, format="PNG", pnginfo=metadata)
                 output.seek(0)
                 
                 # Send Headers
                 self.send_response(HTTPStatus.OK)
                 self.send_header("Content-type", "image/png")
                 self.send_header("Content-Length", str(output.getbuffer().nbytes))
                 self.end_headers()
                 
                 # Send Body
                 self.wfile.write(output.read())
                 
        except Exception as e:
            raise e # Caught by do_GET

    def get_character_definition(self, image_hash):
        """Query the database for the character definition using the image hash."""
        # We need to check multiple tables as per app.py logic
        tables = [
            "chub_character_def", 
            "risuai_character_def", 
            "char_tavern_character_def", 
            "generic_character_def", 
            "chub_lorebook_def", 
            "nyaime_character_def", 
            "webring_character_def"
        ]
        
        # Special case for Booru: it doesn't have a 'definition' column in the same way,
        # app.py constructs it. We might skip booru for embedding or handle it separately.
        # User asked for "JSON, was sie erst als Charakterdaten verwendbar macht".
        # Booru data might not be enough for full card import, but let's check standard tables first.
        
        conn = None
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Try efficient union or sequential check. Since hash is indexed (likely unique image), 
            # we can try to find it in any table.
            
            # We select 'definition' column.
            for table in tables:
                sql = f"SELECT definition FROM {table} WHERE image_hash = %s LIMIT 1"
                cur.execute(sql, (image_hash,))
                row = cur.fetchone()
                if row:
                    return row[0] # Return the definition dict
            
            # If not found in main tables, check booru
            # Booru table: booru_character_def (columns: name, summary, tags, etc.)
            # app.py constructs a fake definition for it.
            # We can skip expensive construction unless requested, but let's stick to the main ones first.
            
            return None
            
        except Exception as e:
            print(f"DB Error: {e}")
            return None
        finally:
            if conn: conn.close()
            
def start_image_server(root_path, port=8505):
    """Starts the background server."""
    
    # helper to find IP similar to app.py
    external_url = os.environ.get("EXTERNAL_URL")
    if external_url:
        base_host = external_url.rstrip("/")
    else:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
        except:
            local_ip = "localhost"
        base_host = f"http://{local_ip}:{port}"

    def run_server():
        # Allow reuse address to prevent "Address already in use" on restarts
        socketserver.TCPServer.allow_reuse_address = True
        with socketserver.TCPServer(("", port), ImageRequestHandler) as httpd:
            print(f"Image Server serving at port {port}")
            httpd.serve_forever()

    # Daemon thread so it dies when main app dies
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    
    return base_host
