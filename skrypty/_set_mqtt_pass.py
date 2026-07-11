import re, sys
cfgpath, pw = sys.argv[1], sys.argv[2]
cfg = open(cfgpath, encoding='utf-8').read()
# tylko pole (pierwsza linia MQTT_PASS = ... która NIE jest os.environ)
cfg, n = re.subn(r'(?m)^MQTT_PASS = (?!os\.environ).*$', 'MQTT_PASS = ' + repr(pw), cfg, count=1)
open(cfgpath, 'w', encoding='utf-8').write(cfg)
print('mqtt_pass_set', n, 'len', len(pw))
