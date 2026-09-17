"""GroupCaster — masowe ON/OFF przez Zigbee groupcast + LOKALNA weryfikacja.

PROBLEM (zmierzony na G1 2026-09-10). Kafel „WSZYSTKIE ON/OFF" strzelal w 78 przekaznikow:
supervisor skladal je w 5 ramek `cmdm`, bramka odbierala wszystkie — i robila 78 pojedynczych
`zigbee2mqtt/<dev>/set` w ciasnej petli. Ramki LoRa dochodzily, ale ZIGBEE tego nie unosilo:
czesc przekaznikow nie przelaczala sie, a potwierdzenia `st` przy 78 sztukach naraz czesciowo
ginely. Retry szedl z supervisora po LoRa = 78 ramek x 2 tury.

ROZWIAZANIE (polecenie usera 2026-09-11):
  1. JEDEN groupcast na grupe zamiast N unicastow — Zigbee multicast, nie 18 osobnych polecen.
  2. Grupa po grupie, SEKWENCYJNIE, z odstepem — nie jeden strzal w 78.
  3. Po kazdej grupie WERYFIKACJA stanu czlonkow i doslanie TYLKO BRAKUJACYCH (nie calej grupy).
  4. Calosc dzieje sie LOKALNIE na bramce (bramka = source of truth) — po LoRa idzie jeden
     wyzwalacz w dol i jeden wynik w gore, zero ruchu na krok posredni.

GWARANCJA bierze sie z punktu 3, nie z 1. Groupcast oszczedza pasmo Zigbee; to weryfikacja
z dosylaniem daje „zawsze wszystkie gasna albo sie zapalaja".

BEZPIECZNIK: groupcast leci tylko wtedy, gdy WSZYSCY czlonkowie grupy sa w zadanym zbiorze.
Przy czesciowym pokryciu multicast ruszylby urzadzenia, ktorych nikt nie prosil — wiec taka
grupa idzie unicastem. Bez tego „zgas szyne RA1-RA6" gasilby cala grupe.
"""
import json
import time


class GroupCaster:
    def __init__(self, publish, get_state, logger=None,
                 settle=2.0, verify_wait=6.0, retries=2, unicast_gap=0.15):
        self.publish = publish          # callable(topic, payload_str) → MQTT
        self.get_state = get_state      # callable(dev) → 'ON'/'OFF'/None (stan z z2m)
        self.log = logger
        self.settle = settle            # przerwa MIEDZY grupami (pozwala sieci Zigbee odetchnac)
        self.verify_wait = verify_wait  # ile czekac na potwierdzenia przed weryfikacja
        self.retries = max(0, int(retries))
        self.unicast_gap = unicast_gap  # odstep miedzy pojedynczymi `set` przy dosylaniu
        self.groups = {}                # friendly_name → [dev, ...]

    def set_groups(self, groups):
        """groups: {nazwa: [dev,...]} — czytane z `zigbee2mqtt/bridge/groups`, nie z configu,
        zeby czlonkostwo mialo JEDNO zrodlo prawdy (z2m) i nie rozjezdzalo sie po edycji."""
        self.groups = {k: list(v) for k, v in (groups or {}).items() if v}

    # ── wysylka ─────────────────────────────────────────
    def _send_group(self, name, cap, val):
        self.publish(f"zigbee2mqtt/{name}/set", json.dumps({cap: val}))

    def _send_dev(self, dev, cap, val):
        self.publish(f"zigbee2mqtt/{dev}/set", json.dumps({cap: val}))

    def _missing(self, devs, cap, val):
        """Ktore urzadzenia NIE maja jeszcze zadanego stanu. Stan nieznany (None) liczy sie jako
        brakujacy — lepiej doslac raz za duzo niz zostawic przekaznik w nieznanym stanie."""
        if cap != 'state':
            return []                   # weryfikujemy tylko on/off; reszta capow nie ma stanu binarnego
        want = str(val).upper()
        return [d for d in devs if str(self.get_state(d) or '').upper() != want]

    def plan(self, devs):
        """Podzial na groupcasty i unicasty. Wydzielony, zeby dalo sie sprawdzic bez MQTT."""
        want = set(devs)
        use_groups, covered = [], set()
        for name, members in sorted(self.groups.items()):
            if members and set(members) <= want:      # PELNE pokrycie — inaczej unicast
                use_groups.append(name)
                covered |= set(members)
        return use_groups, [d for d in devs if d not in covered]

    def apply(self, devs, cap, val, sleep=time.sleep):
        """Masowa komenda. Zwraca raport — to on jedzie do supervisora JEDNYM pakietem."""
        devs = list(dict.fromkeys(devs))              # bez duplikatow, kolejnosc zachowana
        if not devs:
            return {'groups': 0, 'unicast': 0, 'missing': [], 'ok': 0, 'total': 0}
        use_groups, singles = self.plan(devs)

        for i, name in enumerate(use_groups):         # SEKWENCYJNIE, nie jednym strzalem
            self._send_group(name, cap, val)
            if self.log:
                self.log.info('GRP', f'📡 groupcast {name} {cap}={val} '
                                     f'({len(self.groups[name])} urz.) [{i + 1}/{len(use_groups)}]')
            if i + 1 < len(use_groups) or singles:
                sleep(self.settle)
        for d in singles:
            self._send_dev(d, cap, val)
            if self.unicast_gap:
                sleep(self.unicast_gap)
        if singles and self.log:
            self.log.info('GRP', f'📨 unicast {len(singles)} urz. poza grupami')

        missing = devs
        for runda in range(self.retries + 1):
            sleep(self.verify_wait)
            missing = self._missing(devs, cap, val)
            if not missing:
                break
            if runda == self.retries:
                break
            if self.log:
                self.log.warn('GRP', f'🔁 doslam {len(missing)}/{len(devs)} brakujacych '
                                     f'(runda {runda + 1}/{self.retries}): '
                                     + ", ".join(missing[:8]) + ('...' if len(missing) > 8 else ''))
            for d in missing:                         # TYLKO brakujace, nie cala grupa
                self._send_dev(d, cap, val)
                if self.unicast_gap:
                    sleep(self.unicast_gap)

        rap = {'groups': len(use_groups), 'unicast': len(singles),
               'missing': missing, 'ok': len(devs) - len(missing), 'total': len(devs)}
        if self.log:
            if missing:
                self.log.error('GRP', f'❌ {len(missing)}/{len(devs)} NIE przelaczylo sie '
                                      f'po {self.retries} dosylkach: ' + ", ".join(missing))
            else:
                self.log.info('GRP', f'✅ {len(devs)}/{len(devs)} {cap}={val} potwierdzone '
                                     f'({len(use_groups)} groupcastow, {len(singles)} unicastow)')
        return rap
