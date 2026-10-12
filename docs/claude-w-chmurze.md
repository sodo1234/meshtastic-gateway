# Claude na kredytach API (zamiast limitu planu claude.ai)

## Dwa portfele

| Portfel | Co z niego płaci |
|---|---|
| **Plan claude.ai** (limit subskrypcji) | claude.ai/code, Claude Desktop, Remote Control (`claude remote-control`), `claude` zalogowany kontem claude.ai |
| **Kredyty API** (console.anthropic.com, projekt *meshtastic gateway*) | wszystko na **kluczu API** — GitHub Action z tego repo albo `claude` CLI z `ANTHROPIC_API_KEY` |

Remote Control zawsze liczy się do limitu planu, dlatego mostem między
claude.ai/code a „pracownikiem” na kredytach jest **GitHub**.

```
claude.ai/code — planuje, pisze krótkie zadanie            ← limit planu (mało)
      │ issue / komentarz z "@claude ..."
      ▼
GitHub Actions → Claude Code (.github/workflows/claude.yml) ← kredyty API
      │ koduje, commituje na gałąź claude/..., otwiera PR, odpisuje w issue
      ▼
claude.ai/code przegląda PR, ewentualnie "@claude popraw ..."
```

## Konfiguracja (jednorazowo)

1. console.anthropic.com → workspace/projekt **meshtastic gateway** → **API Keys** → *Create Key*.
2. GitHub → repo → **Settings → Secrets and variables → Actions → New repository secret**:
   `ANTHROPIC_API_KEY` = klucz. Nigdy nie wklejaj klucza do plików ani issue.
3. Claude GitHub App zainstalowana na repo (https://github.com/apps/claude).
4. Workflow musi być na `main` — akcje `issue_comment` GitHub uruchamia z gałęzi domyślnej.
5. Opcjonalnie: w konsoli ustaw spend limit workspace'u (np. 95 $).

## Zlecanie zadań

Przykład issue:

```
@claude Krok 5 planu refactoringu (Ping/Pong/Heartbeat) dla gateway_v38.py.
Nie ruszaj plików "verified:". Po edycji ast.parse() + test importu klasy.
```

Claude w akcji czyta `CLAUDE.md` automatycznie, więc zasady refactoringu
(verified files, constructor injection, `separators=(',',':')`, zachowanie CONFIG)
obowiązują także tam.

## Oszczędzanie kredytów

- Domyślnie Sonnet i max 40 tur na zadanie — zmiana w `claude_args` workflow.
- Jedno issue = jeden krok refactoringu, z nazwami plików/klas — mniej szukania, mniej tokenów.
- Zużycie: konsola Anthropic → **Usage**.
