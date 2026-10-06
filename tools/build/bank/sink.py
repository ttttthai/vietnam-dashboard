from http.server import BaseHTTPRequestHandler,HTTPServer
import urllib.parse,os
class H(BaseHTTPRequestHandler):
    def cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers','*')
        self.send_header('Access-Control-Allow-Private-Network','true')
    def do_OPTIONS(self):
        self.send_response(204); self.cors(); self.end_headers()
    def do_POST(self):
        n=int(self.headers.get('Content-Length',0)); b=self.rfile.read(n)
        name=urllib.parse.urlparse(self.path).path.strip('/') or 'out.json'
        open(os.path.join('dump',os.path.basename(name)),'wb').write(b)
        self.send_response(200); self.cors(); self.end_headers(); self.wfile.write(b'ok')
os.makedirs('dump',exist_ok=True)
HTTPServer(('127.0.0.1',18923),H).serve_forever()
