import re, sys
cfgpath, tok = sys.argv[1], sys.argv[2]
cfg = open(cfgpath, encoding='utf-8').read()
cfg, n = re.subn(r'(?m)^HA_TOKEN  = ""', 'HA_TOKEN  = ' + repr(tok), cfg, count=1)
open(cfgpath, 'w', encoding='utf-8').write(cfg)
print(f'ha_token_set={n} (len {len(tok)})  mqtt_pass=PUSTE (do uzupełnienia)')
