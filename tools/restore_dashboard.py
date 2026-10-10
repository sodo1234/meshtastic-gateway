import sys,json,asyncio
sys.path.insert(0,'.');import config,websockets
url_path,backup=sys.argv[1],sys.argv[2]
url=config.HA_URL.replace('http','ws')+'/api/websocket'
async def main():
    cfg=json.load(open(backup))
    async with websockets.connect(url,max_size=None) as ws:
        await ws.recv(); await ws.send(json.dumps({"type":"auth","access_token":config.HA_TOKEN})); await ws.recv()
        await ws.send(json.dumps({"id":1,"type":"lovelace/config/save","url_path":url_path,"config":cfg}))
        m=json.loads(await ws.recv()); print("przywrocono:",m.get("success"),m.get("error"))
asyncio.run(main())
