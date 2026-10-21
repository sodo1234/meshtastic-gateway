# Popup anomalii: zagnieżdżone szablony [[[ ]]] opakowane tak, by kafel (ewaluujacy tap_action w chwili klikniecia)
# zwracal je NIEZEWALUOWANE; ewaluuje je dopiero karta w popupie (na zywo).
import sys,json,asyncio,time
sys.path.insert(0,'.');import config,websockets
url_path=sys.argv[1]; apply=len(sys.argv)>2 and sys.argv[2]=='apply'
url=config.HA_URL.replace('http','ws')+'/api/websocket'
async def main():
    async with websockets.connect(url,max_size=None) as ws:
        await ws.recv(); await ws.send(json.dumps({"type":"auth","access_token":config.HA_TOKEN})); await ws.recv()
        i=[0]
        async def call(t,**k):
            i[0]+=1; await ws.send(json.dumps({"id":i[0],"type":t,**k}))
            while True:
                m=json.loads(await ws.recv())
                if m.get("id")==i[0]: return m
        if url_path=='all':  # wszystkie dashboardy tego HA (+ domyslny)
            paths=[None]+[d["url_path"] for d in (await call("lovelace/dashboards/list"))["result"] if d.get("mode")=="storage"]
            for p in paths: await one(call,p)
            return
        await one(call,url_path)
async def one(call,url_path):
        r=await call("lovelace/config",force=False,url_path=url_path)
        if not r.get("success"): print(url_path,"pominiety:",r.get("error",{}).get("code")); return
        cfg=r["result"]
        print("==",url_path)
        bk=f"/tmp/lovelace_backup_{url_path}_{int(time.time())}.json"; json.dump(cfg,open(bk,'w'),ensure_ascii=False); print("backup:",bk)
        stats={"popups":0,"strings":0}
        def wrap(o):
            if isinstance(o,list): return [wrap(x) for x in o]
            if isinstance(o,dict): return {k:wrap(v) for k,v in o.items()}
            if isinstance(o,str):
                t=o.strip()
                if t.startswith('[[[ return "[[['):  # juz owiniety (2026-10-21: idempotentny)
                    stats["done"]=stats.get("done",0)+1; return o
                if t.startswith('[[[') and t.endswith(']]]'):
                    stats["strings"]+=1
                    return "[[[ return "+json.dumps(t,ensure_ascii=False)+"; ]]]"
            return o
        def walk(o):
            if isinstance(o,list): return [walk(x) for x in o]
            if not isinstance(o,dict): return o
            out={k:walk(v) for k,v in o.items()}
            if o.get("service")=="browser_mod.popup" and isinstance(o.get("data"),dict) and "content" in o["data"]:
                stats["popups"]+=1
                out["data"]=dict(o["data"]); out["data"]["content"]=wrap(o["data"]["content"])
            return out
        new=walk(cfg)
        print("statystyki:",stats)
        if apply:
            res=await call("lovelace/config/save",url_path=url_path,config=new); print("zapis:",res.get("success"),res.get("error"))
asyncio.run(main())
