# agent_status

Panel statusu Codex / Gemini CLI na `local`, G2, SUP (G1 jako offline, gdy nieosiagalny) + ostatnie 5 komentarzy z issue #3.

Wymagania: Python 3 (sama stdlib), `ssh` z kluczem do `td@` maszyn (BatchMode, bez hasla), `gh` zalogowany.

Uruchomienie:
- Windows: `powershell -File start.ps1` (ukryte okno) albo `python server.py`
- Linux: `python3 server.py` (sekcja `local` uzywa wtedy PowerShell, wiec na Linuxie bedzie pusta)

Strona: `http://<host>:8977/`, JSON: `/api/status` i `/api/dispatch`. Strona odswieza co 5 s;
sondy maszyn co 20 s. Jedna strona pokazuje tez ostatnie 50 wpisow `tools/dispatch.log`
(poziom, model, czas wykonania, status i wynik).
Tylko odczyt; zmienne srodowiskowe raportowane jako tak/nie, nigdy wartosc.
Zatrzymanie: zabij proces `python server.py`.
