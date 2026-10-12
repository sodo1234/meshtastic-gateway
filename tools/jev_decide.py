"""Pilot JEV: tanie decyzje typowane (poziom delegacji, triage logu). Klucz TYLKO z env TYPESAFE_API_KEY.

Uzycie:
  python tools/jev_decide.py level "opis zadania"      -> {"level": "0|1|2|3", "confidence": ..}
  python tools/jev_decide.py anomaly "linia logu"      -> {"anomaly_p": 0..1}
Przy braku klucza / bledzie / niskiej pewnosci (< --min, domysl. 0.7) wypisuje {"fallback": true}:
wtedy decyduje Claude sam. Uprawnienia/bezpieczenstwo NIGDY nie sa delegowane do JEV.
"""
import json, os, sys, time, urllib.request

URL = "https://api.typesafe.ai/v1/systemone"
LEVELS = {
    "0": "Robie sam: jedna edycja, grep, komenda, odpowiedz z kontekstu",
    "1": "Haiku: lookupy, przeszukanie wielu plikow, streszczenia logow",
    "2": "Opus: edycje na kilku plikach, testy, refaktor, dokumentacja, praca dluga",
    "3": "Fable: architektura, trudne bugi, review przed wdrozeniem",
}


def ask(state, questions):
    key = os.environ.get("TYPESAFE_API_KEY")
    if not key:
        return None
    req = urllib.request.Request(
        URL,
        data=json.dumps({"model": "jev-latest", "state": state, "questions": questions}).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.load(r)
    except Exception:
        return None


def main():
    if len(sys.argv) < 3 or sys.argv[1] not in ("level", "anomaly"):
        sys.exit(__doc__)
    kind, text = sys.argv[1], " ".join(a for a in sys.argv[2:] if a != "--min")
    mn = float(os.environ.get("JEV_MIN", "0.7"))
    t0 = time.time()
    if kind == "level":
        res = ask({"zadanie": text}, {"level": {
            "type": "choice",
            "instructions": "Ktory poziom delegacji najlepiej pasuje do zadania?",
            "criteria": LEVELS}})
        a = (res or {}).get("answers", {}).get("level")
        out = {"fallback": True} if not a or a.get("confidence", 0) < mn else {
            "level": a["choice"], "confidence": a["confidence"]}
    else:
        res = ask({"log": text}, {"anomaly": {
            "type": "noul",
            "instructions": "Czy ta linia logu bramki LoRa/Meshtastic opisuje realny problem (anomalia, blad, utrata ramki), a nie zwykla prace?"}})
        a = (res or {}).get("answers", {}).get("anomaly")
        p = a.get("noul") if a else None
        out = {"fallback": True} if p is None or 0.3 < p < mn else {"anomaly_p": p}
    out["ms"] = int((time.time() - t0) * 1000)
    if res and "usage" in res:
        out["usage"] = res["usage"]
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
