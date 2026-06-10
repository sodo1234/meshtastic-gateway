#!/usr/bin/env python3
"""Dodaje 📶 LQI do kart urządzeń w dashboardzie supervisora (lovelace).

Pobiera live config (WS), backup do /tmp, wstrzykuje LQI do KAŻDEJ karty z info
odwołującym się do sensor.lora_g1_<dev>_last_seen, zapisuje. Idempotentny.
LQI encja: sensor.lora_<dev>_<dev>_link_quality (kolor progowy ≥100/≥50/<).
Token: env HA_SUP_TOKEN.
"""
import asyncio
import json
import os
import re
import time

import websockets

HA = "ws://localhost:8123/api/websocket"
TOKEN = os.environ.get("HA_SUP_TOKEN", "REPLACE_ME")
URL_PATH = None  # domyślny dashboard lovelace

LS_RE = re.compile(r"sensor\.lora_g1_([a-z0-9_]+)_last_seen")
LQ_SPAN = ' · <span style="color:${lqCol};">\U0001F4F6 ${lqTxt}</span>'


def patch_js(js):
    if 'link_quality' in js:
        return js, False
    m = LS_RE.search(js)
    if not m:
        return js, False
    dev = m.group(1)
    lqe = "sensor.lora_%s_%s_link_quality" % (dev, dev)
    lines, out, injected = js.split('\n'), [], False
    for ln in lines:
        out.append(ln)
        if not injected and 'var ls' in ln and '_last_seen' in ln:
            ws = ln[:len(ln) - len(ln.lstrip())]
            out.append(ws + "var lqe = states['%s'];" % lqe)
            out.append(ws + "var lqv = lqe && lqe.state !== 'unavailable' && lqe.state !== 'unknown' && lqe.state !== '' ? parseInt(lqe.state) : null;")
            out.append(ws + "var lqCol = lqv === null ? '#525252' : (lqv >= 100 ? '#4ade80' : (lqv >= 50 ? '#fbbf24' : '#f87171'));")
            out.append(ws + "var lqTxt = lqv === null ? '--' : lqv;")
            injected = True
    if not injected:
        return js, False
    js2 = '\n'.join(out)
    for batt in ('\U0001F50B ${b}%</span>', '\U0001F50B ${batt}%</span>'):   # Leak/Door vs Temp
        if batt in js2:
            return js2.replace(batt, batt + LQ_SPAN, 1), True
    if '● ${label}</span>' in js2:                          # switch (brak baterii)
        return js2.replace('● ${label}</span>', '● ${label}</span>' + LQ_SPAN, 1), True
    return js, False


def walk(node, stats):
    if isinstance(node, dict):
        cf = node.get('custom_fields')
        if isinstance(cf, dict):
            for fld in ('info', 'content'):                 # switch/binary=info, temp=content
                if isinstance(cf.get(fld), str):
                    new, changed = patch_js(cf[fld])
                    if changed:
                        cf[fld] = new
                        stats['patched'] += 1
        for v in node.values():
            walk(v, stats)
    elif isinstance(node, list):
        for v in node:
            walk(v, stats)


async def main():
    async with websockets.connect(HA, max_size=None) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
        if json.loads(await ws.recv()).get("type") != "auth_ok":
            print("AUTH FAIL"); return
        mid = [0]

        async def call(msg):
            mid[0] += 1; msg["id"] = mid[0]
            await ws.send(json.dumps(msg))
            while True:
                r = json.loads(await ws.recv())
                if r.get("id") == mid[0] and r.get("type") == "result":
                    return r

        cfg = (await call({"type": "lovelace/config", "url_path": URL_PATH})).get("result")
        if not cfg:
            print("brak config (pusty lovelace?)"); return
        bk = "/tmp/lovelace_backup_%d.json" % int(time.time())
        json.dump(cfg, open(bk, "w", encoding="utf-8"), ensure_ascii=False)
        print("backup:", bk)
        stats = {'patched': 0}
        walk(cfg, stats)
        print("patched cards:", stats['patched'])
        if stats['patched']:
            r = await call({"type": "lovelace/config/save", "url_path": URL_PATH, "config": cfg})
            print("save:", r.get("success"), r.get("error", ""))


if __name__ == "__main__":
    asyncio.run(main())
