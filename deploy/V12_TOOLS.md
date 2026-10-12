# Narzedzia v12

Instalacja do katalogu projektu:

```text
python deploy/install_v12_tools.py C:\sciezka\do\projektu
```

Pakiet kopiuje `jev_decide`, `dispatch`, `jev_hook` i `agent_status`. Nie kopiuje ani nie
zapisuje kluczy. `TYPESAFE_API_KEY` musi byc ustawiony w srodowisku procesu. Dashboard startuje
poleceniem `python tools/agent_status/server.py` na porcie 8977.
