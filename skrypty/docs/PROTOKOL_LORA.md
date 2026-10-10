# Protokół LoRa — skrócony JSON

> Plik generowany przez `python docs/gen_protokol.py`. Nie edytuj tabel ręcznie; 
> źródłem prawdy są `modules/transport/codec.py:TYPES` i `KEYS`.

Kodek działa wyłącznie na granicy `LoraTransport`: przed `json.dumps` przy TX i zaraz po 
`json.loads` przy RX. Kod domenowy, dispatchery i dashboardy nadal używają długich nazw. 
Skracane są tylko klucze najwyższego poziomu; zagnieżdżone payloady pozostają bez zmian. 
Dekoder akceptuje również starą, długą postać. `g` nigdy nie jest przemianowane.

## Fast lane (ruch natychmiastowy)

Fast lane przywraca małe, sporadyczne ramki sterujące do czasu reakcji sprzed pełnego
slotowania, bez rozszczelniania slotów dla ruchu masowego. Domyślna lista
`slotting.immediate` to: `ping`, `pong`, `sup_ping`, `sup_pong`, `cmd`, `st`, `stm`,
`sync`, `params`, `params_req`, `param_upd`, `zg`. Typ jest rozpoznawany zarówno w długiej,
jak i krótkiej postaci kodeka.

- Ramka fast lane trafia na początek kolejki TX, omija slot i filtr zajętego arbitra transferu;
  nie przerywa ramki już nadawanej.
- Nadal obowiązuje globalny odstęp `lora.tx_cooldown` (produkcyjnie 3,0 s). Przed TX działa
  listen-before-talk: od ostatniej dowolnej ramki RX musi minąć `slotting.lbt_quiet`
  (domyślnie 2,0 s), lecz oczekiwanie kończy się najpóźniej po `slotting.lbt_max_wait`
  (domyślnie 6,0 s).
- `rt_*`, transfery `cal_*`, `b`, `hb`, `an_d`, `ab`, `disc*`, `dsnap`, `avail`, `ansnap`,
  `bdata` i pozostały bulk zachowują dotychczasowe bramkowanie slotem. Reaktywne ACK/CACK
  supervisora zachowują swój wcześniejszy wyjątek.

Uzasadnienie: po włączeniu slotowania benchmark pogorszył `ping→pong` z 6 s do 60 s,
klik sterowania→stan realny z 24–31 s do 38–39 s, Send parametrów→potwierdzenie z 7 s
do 32 s i anomalia→licznik z 11 s do 19 s. Fast lane usuwa oczekiwanie na slot tylko
dla krótkich zdarzeń krytycznych czasowo; bulk nadal korzysta z bezkolizyjnych okien.

## Negocjacja krótkiej postaci kodeka

`LoraTransport` prowadzi wyłącznie w RAM tabelę możliwości per peer (`g`). Odbiornik zawsze
dekoduje obie postacie. Supervisor nadaje krótko do danej bramki dopiero po odebraniu od niej
co najmniej jednej jednoznacznie krótkiej ramki; do nieznanego lub starego peera nadaje długo.
Bramka postępuje analogicznie po krótkiej ramce supervisora albo może zainicjować negocjację
przez `codec.short_tx: true` (domyślnie `false`). Ramki bez `g` zawsze pozostają długie.
Nie dodano flagi do `hb`, ponieważ ramka jest blisko limitu 200 B; zdolność jest wnioskowana
z odebranej postaci radiowej i znika po restarcie procesu.

## Tabele źródłowe TYPES / KEYS

### Typy ramek (`t`)

| Długa nazwa | Na radiu |
|---|---|
| `ab` | `a` |
| `ac_all` | `aa` |
| `ac_b` | `ac` |
| `an_d` | `ad` |
| `ansnap_req` | `ar` |
| `b` | `b` |
| `back` | `bk` |
| `cal_ack` | `ca` |
| `cal_begin` | `cb` |
| `cal_cack` | `cc` |
| `cal_chunk` | `ck` |
| `cal_end` | `ce` |
| `cal_pull` | `cl` |
| `cal_req` | `cr` |
| `cfg` | `cf` |
| `cmd` | `c` |
| `cmdm` | `cm` |
| `db` | `d` |
| `disc` | `dc` |
| `disc_ack` | `da` |
| `disc_ap` | `dp` |
| `disc_meta` | `dm` |
| `disc_vio` | `dv` |
| `dsnap_req` | `dr` |
| `dump_anom` | `du` |
| `hb` | `h` |
| `param_upd` | `pu` |
| `params` | `ps` |
| `params_req` | `pr` |
| `ping` | `pi` |
| `pong` | `po` |
| `req` | `q` |
| `rt_ack` | `ra` |
| `rt_begin` | `rb` |
| `rt_cack` | `rc` |
| `rt_chunk` | `rk` |
| `rt_end` | `re` |
| `set_mode` | `sm` |
| `st` | `s` |
| `stm` | `m` |
| `sup_ping` | `sp` |
| `sup_pong` | `so` |
| `sync` | `sy` |
| `sync_req` | `sr` |
| `vbtn` | `vb` |
| `vbtn_ack` | `va` |
| `vsw` | `v` |
| `vsw_st` | `vt` |
| `zg` | `z` |

### Rodzaje payloadów ReliableTransfer (`kind`)

| Długa nazwa | Na radiu |
|---|---|
| `ansnap` | `an` |
| `avail` | `av` |
| `bdata` | `bd` |
| `devmap` | `md` |
| `dsnap` | `ds` |
| `mass_offline` | `mo` |

### Kierunki kalendarza (`dir`)

| Długa nazwa | Na radiu |
|---|---|
| `cal` | `cal` |
| `gw_push` | `gp` |
| `push` | `psh` |

### Klucze najwyższego poziomu

| Zakres ramki | Długi klucz | Na radiu |
|---|---|---|
| `cal_ack` | `miss` | `mi` |
| `disc_ap` | `hash` | `hh` |
| `disc_meta` | `hash` | `hh` |
| `disc_meta` | `dev_n` | `dn` |
| `hb` | `hash` | `hh` |
| `pong` | `hash` | `hh` |
| `rt_ack` | `miss` | `mi` |
| `rt_begin` | `kind` | `k` |
| `rt_begin` | `win` | `w` |
| `rt_end` | `kind` | `k` |

## Katalog ramek

Reguła slotów: bulk bramki nadaje w jej slocie, a bulk supervisora w slocie supervisora.
Fast lane opisany wyżej omija slot; odpowiedzi `cack`/`ack` i `back` zachowują wcześniejsze wyjątki.

### Łącze, czas i zgodność

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `hb` → `h` | G→SUP | Heartbeat bramki z licznikami i hashami. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`up→up` (int): uptime w sekundach<br>`dev→dev` (int): liczba urządzeń<br>`mon→mon` (int/array): monitored: liczba lub SID-y<br>`pri→pri` (int/array): priority: liczba lub SID-y<br>`ts→ts` (int): czas Unix<br>`hash→hh` (str): hash mapy<br>`cal→cal` (str): hash kalendarza<br>`ph→ph` (str): hash parametrów<br>`dh→dh` (str): hash danych<br>`sh→sh` (str): hash statusu<br>`ah→ah` (str): hash anomalii<br>`gm→gm` (str): tryb bramki<br>`ga→ga` (0/1): aktywność bramki<br>`tq→tq` (str): jakość czasu<br>`m→m` (array/int): braki lub tryb | Okresowo i po zmianie trybu. | slot bramki | <code>{&quot;t&quot;:&quot;hb&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;up&quot;:123456,&quot;dev&quot;:199,&quot;mon&quot;:82,&quot;pri&quot;:78,&quot;ts&quot;:1700000000,&quot;hash&quot;:&quot;abcd&quot;,&quot;cal&quot;:&quot;cafe&quot;,&quot;ph&quot;:&quot;1234&quot;,&quot;dh&quot;:&quot;5678&quot;,&quot;sh&quot;:&quot;9abc&quot;,&quot;ah&quot;:&quot;def0&quot;,&quot;gm&quot;:&quot;calendar&quot;,&quot;ga&quot;:1,&quot;tq&quot;:&quot;synced&quot;,&quot;m&quot;:1}</code> (193 B)<br><code>{&quot;t&quot;:&quot;h&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;up&quot;:123456,&quot;dev&quot;:199,&quot;mon&quot;:82,&quot;pri&quot;:78,&quot;ts&quot;:1700000000,&quot;hh&quot;:&quot;abcd&quot;,&quot;cal&quot;:&quot;cafe&quot;,&quot;ph&quot;:&quot;1234&quot;,&quot;dh&quot;:&quot;5678&quot;,&quot;sh&quot;:&quot;9abc&quot;,&quot;ah&quot;:&quot;def0&quot;,&quot;gm&quot;:&quot;calendar&quot;,&quot;ga&quot;:1,&quot;tq&quot;:&quot;synced&quot;,&quot;m&quot;:1}</code> (190 B) |
| `ping` → `pi` | SUP→G | Sonda bramki. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Przycisk/test startowy lub retry sondy. | slot supervisora | <code>{&quot;t&quot;:&quot;ping&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (21 B)<br><code>{&quot;t&quot;:&quot;pi&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `pong` → `po` | G→SUP | Odpowiedź bramki z diagnostyką HB. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`up→up` (int): uptime w sekundach<br>`dev→dev` (int): liczba urządzeń<br>`mon→mon` (int/array): monitored: liczba lub SID-y<br>`pri→pri` (int/array): priority: liczba lub SID-y<br>`ts→ts` (int): czas Unix<br>`hash→hh` (str): hash mapy<br>`cal→cal` (str): hash kalendarza<br>`ph→ph` (str): hash parametrów | Po odebraniu `ping`. | slot bramki | <code>{&quot;t&quot;:&quot;pong&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;up&quot;:123456,&quot;dev&quot;:199,&quot;mon&quot;:82,&quot;pri&quot;:78,&quot;ts&quot;:1700000000,&quot;hash&quot;:&quot;abcd&quot;,&quot;cal&quot;:&quot;cafe&quot;,&quot;ph&quot;:&quot;1234&quot;}</code> (116 B)<br><code>{&quot;t&quot;:&quot;po&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;up&quot;:123456,&quot;dev&quot;:199,&quot;mon&quot;:82,&quot;pri&quot;:78,&quot;ts&quot;:1700000000,&quot;hh&quot;:&quot;abcd&quot;,&quot;cal&quot;:&quot;cafe&quot;,&quot;ph&quot;:&quot;1234&quot;}</code> (112 B) |
| `sup_ping` → `sp` | G→SUP | Sonda łącza bramka→supervisor. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Po ciszy na łączu do supervisora. | slot bramki | <code>{&quot;t&quot;:&quot;sup_ping&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (25 B)<br><code>{&quot;t&quot;:&quot;sp&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `sup_pong` → `so` | SUP→G | Odpowiedź supervisora z czasem i zgodnością. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`sec→sec` (int): czas Unix supervisora<br>`zg→zg` (str): flagi zgodności | Natychmiast po `sup_ping`. | natychmiastowa odpowiedź SUP | <code>{&quot;t&quot;:&quot;sup_pong&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;sec&quot;:1700000000,&quot;zg&quot;:&quot;1111&quot;}</code> (54 B)<br><code>{&quot;t&quot;:&quot;so&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;sec&quot;:1700000000,&quot;zg&quot;:&quot;1111&quot;}</code> (48 B) |
| `sync` → `sy` | SUP→G | Synchronizacja czasu bramki. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`sec→sec` (int): czas Unix supervisora | Na żądanie lub przy złej jakości czasu. | slot supervisora | <code>{&quot;t&quot;:&quot;sync&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;sec&quot;:1700000000}</code> (38 B)<br><code>{&quot;t&quot;:&quot;sy&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;sec&quot;:1700000000}</code> (36 B) |
| `sync_req` → `sr` | G→SUP | Żądanie synchronizacji czasu. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Gdy bramka potrzebuje czasu. | slot bramki | <code>{&quot;t&quot;:&quot;sync_req&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (25 B)<br><code>{&quot;t&quot;:&quot;sr&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `zg` → `z` | SUP→G | Push czterech flag zgodności. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`z→z` (str): flagi zgodności | Po zmianie wyniku zgodności. | slot supervisora | <code>{&quot;t&quot;:&quot;zg&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;z&quot;:&quot;1111&quot;}</code> (30 B)<br><code>{&quot;t&quot;:&quot;z&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;z&quot;:&quot;1111&quot;}</code> (29 B) |

### Sterowanie i dane

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `b` → `b` | G→SUP | Batch stanu i dostępności urządzeń. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`ts→ts` (int): czas Unix<br>`p→p` (int): id batcha priorytetowego<br>`d→d` (array/object): dane ramki | Po zmianie/flushu batchera i przy pełnym sweepie. | slot bramki | <code>{&quot;t&quot;:&quot;b&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;p&quot;:7,&quot;d&quot;:[[1,{&quot;a&quot;:1,&quot;temperature&quot;:22.5,&quot;s&quot;:&quot;ON&quot;}]]}</code> (86 B)<br><code>{&quot;t&quot;:&quot;b&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;p&quot;:7,&quot;d&quot;:[[1,{&quot;a&quot;:1,&quot;temperature&quot;:22.5,&quot;s&quot;:&quot;ON&quot;}]]}</code> (86 B) |
| `back` → `bk` | SUP→G | ACK batcha priorytetowego. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`p→p` (int): id batcha priorytetowego | Po priorytetowym `b` z polem `p`. | natychmiastowa odpowiedź SUP | <code>{&quot;t&quot;:&quot;back&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;p&quot;:7}</code> (27 B)<br><code>{&quot;t&quot;:&quot;bk&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;p&quot;:7}</code> (25 B) |
| `cmd` → `c` | SUP→G | Sterowanie jednym urządzeniem. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki<br>`c→c` (str): capability<br>`v→v` (any): wartość | Klik lub retry dla jednego urządzenia. | slot supervisora | <code>{&quot;t&quot;:&quot;cmd&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Relay 1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;ON&quot;}</code> (55 B)<br><code>{&quot;t&quot;:&quot;c&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Relay 1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;ON&quot;}</code> (53 B) |
| `cmdm` → `cm` | SUP→G | Sterowanie grupą urządzeń. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki<br>`c→c` (str): capability<br>`v→v` (any): wartość | Flush grupy komend. | slot supervisora | <code>{&quot;t&quot;:&quot;cmdm&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[&quot;Relay 1&quot;,&quot;Relay 2&quot;],&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;OFF&quot;}</code> (69 B)<br><code>{&quot;t&quot;:&quot;cm&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[&quot;Relay 1&quot;,&quot;Relay 2&quot;],&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;OFF&quot;}</code> (67 B) |
| `req` → `q` | SUP→G | Odświeżenie jednego urządzenia. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki | Klik Odśwież. | slot supervisora | <code>{&quot;t&quot;:&quot;req&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Temp 1&quot;}</code> (33 B)<br><code>{&quot;t&quot;:&quot;q&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Temp 1&quot;}</code> (31 B) |
| `st` → `s` | G→SUP | Potwierdzenie stanu jednego urządzenia. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki<br>`c→c` (str): capability<br>`v→v` (any): wartość | Po potwierdzeniu stanu przez Z2M. | slot bramki | <code>{&quot;t&quot;:&quot;st&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Relay 1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;ON&quot;}</code> (54 B)<br><code>{&quot;t&quot;:&quot;s&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:&quot;Relay 1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;v&quot;:&quot;ON&quot;}</code> (53 B) |
| `stm` → `m` | G→SUP | Zbiorcze potwierdzenia stanów. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`c→c` (str): capability<br>`d→d` (array/object): dane ramki | Flush okna zbiorczych statusów. | slot bramki | <code>{&quot;t&quot;:&quot;stm&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;d&quot;:{&quot;Relay 1&quot;:&quot;ON&quot;,&quot;Relay 2&quot;:&quot;OFF&quot;}}</code> (69 B)<br><code>{&quot;t&quot;:&quot;m&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;c&quot;:&quot;state&quot;,&quot;d&quot;:{&quot;Relay 1&quot;:&quot;ON&quot;,&quot;Relay 2&quot;:&quot;OFF&quot;}}</code> (67 B) |
| `set_mode` → `sm` | SUP→G | Zmiana trybu pracy bramki. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`m→m` (array/int): braki lub tryb | Zmiana selecta trybu. | slot supervisora | <code>{&quot;t&quot;:&quot;set_mode&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;m&quot;:&quot;calendar&quot;}</code> (40 B)<br><code>{&quot;t&quot;:&quot;sm&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;m&quot;:&quot;calendar&quot;}</code> (34 B) |
| `vbtn` → `vb` | SUP→G | Naciśnięcie wirtualnego przycisku. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`id→id` (str): id VIO | Klik wirtualnego przycisku. | slot supervisora | <code>{&quot;t&quot;:&quot;vbtn&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;all_on&quot;}</code> (35 B)<br><code>{&quot;t&quot;:&quot;vb&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;all_on&quot;}</code> (33 B) |
| `vbtn_ack` → `va` | G→SUP | ACK wirtualnego przycisku. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`id→id` (str): id VIO | Po obsłużeniu `vbtn`. | slot bramki | <code>{&quot;t&quot;:&quot;vbtn_ack&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;all_on&quot;}</code> (39 B)<br><code>{&quot;t&quot;:&quot;va&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;all_on&quot;}</code> (33 B) |
| `vsw` → `v` | SUP→G | Ustawienie wirtualnego przełącznika. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`id→id` (str): id VIO<br>`v→v` (any): wartość | Zmiana wirtualnego przełącznika. | slot supervisora | <code>{&quot;t&quot;:&quot;vsw&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;auto&quot;,&quot;v&quot;:1}</code> (38 B)<br><code>{&quot;t&quot;:&quot;v&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;auto&quot;,&quot;v&quot;:1}</code> (36 B) |
| `vsw_st` → `vt` | G→SUP | Potwierdzenie stanu wirtualnego przełącznika. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`id→id` (str): id VIO<br>`v→v` (any): wartość | Po zastosowaniu `vsw`. | slot bramki | <code>{&quot;t&quot;:&quot;vsw_st&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;auto&quot;,&quot;v&quot;:1}</code> (41 B)<br><code>{&quot;t&quot;:&quot;vt&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;id&quot;:&quot;auto&quot;,&quot;v&quot;:1}</code> (37 B) |

### Discovery i migawki

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `db` → `d` | G→SUP | Stara, porcjowana mapa urządzeń. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki | Fallback discovery bez transferu plikowego. | slot bramki | <code>{&quot;t&quot;:&quot;db&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[1,&quot;Temp 1&quot;,&quot;S&quot;,&quot;bth&quot;]]}</code> (48 B)<br><code>{&quot;t&quot;:&quot;d&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[1,&quot;Temp 1&quot;,&quot;S&quot;,&quot;bth&quot;]]}</code> (47 B) |
| `disc` → `dc` | SUP→G | Żądanie discovery/mapy. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`h→h` (str): hash lub hash supervisora | Start, przycisk lub rozjazd hasha. | slot supervisora | <code>{&quot;t&quot;:&quot;disc&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;h&quot;:&quot;abcd&quot;}</code> (32 B)<br><code>{&quot;t&quot;:&quot;dc&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;h&quot;:&quot;abcd&quot;}</code> (30 B) |
| `disc_ack` → `da` | G→SUP | Stary ACK discovery. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Tylko kompatybilność starego discovery. | slot bramki | <code>{&quot;t&quot;:&quot;disc_ack&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (25 B)<br><code>{&quot;t&quot;:&quot;da&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `disc_ap` → `dp` | G→SUP | Lekka mapa monitored/priority. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`hash→hh` (str): hash mapy<br>`mon→mon` (int/array): monitored: liczba lub SID-y<br>`pri→pri` (int/array): priority: liczba lub SID-y | Po zmianie zbiorów monitored/priority. | slot bramki | <code>{&quot;t&quot;:&quot;disc_ap&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;hash&quot;:&quot;abcd&quot;,&quot;mon&quot;:[1],&quot;pri&quot;:[2]}</code> (58 B)<br><code>{&quot;t&quot;:&quot;dp&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;hh&quot;:&quot;abcd&quot;,&quot;mon&quot;:[1],&quot;pri&quot;:[2]}</code> (51 B) |
| `disc_meta` → `dm` | G→SUP | Metadane i hash mapy urządzeń. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`hash→hh` (str): hash mapy<br>`dev_n→dn` (int): liczba urządzeń | Przy ogłoszeniu mapy lub meta-only. | slot bramki | <code>{&quot;t&quot;:&quot;disc_meta&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;hash&quot;:&quot;abcd&quot;,&quot;dev_n&quot;:199}</code> (52 B)<br><code>{&quot;t&quot;:&quot;dm&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;hh&quot;:&quot;abcd&quot;,&quot;dn&quot;:199}</code> (40 B) |
| `disc_vio` → `dv` | G→SUP | Mapa wirtualnych wejść/wyjść. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki | Podczas discovery VIO. | slot bramki | <code>{&quot;t&quot;:&quot;disc_vio&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[&quot;all_on&quot;,&quot;b&quot;,&quot;All on&quot;,0]]}</code> (57 B)<br><code>{&quot;t&quot;:&quot;dv&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[&quot;all_on&quot;,&quot;b&quot;,&quot;All on&quot;,0]]}</code> (51 B) |
| `dsnap_req` → `dr` | SUP→G | Żądanie pełnej migawki stanu. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Gdy hash danych/statusu się rozjeżdża. | slot supervisora | <code>{&quot;t&quot;:&quot;dsnap_req&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (26 B)<br><code>{&quot;t&quot;:&quot;dr&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |

### Anomalie

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `ab` → `a` | G→SUP | Paczka zdarzeń anomalii. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`ts→ts` (int): czas Unix<br>`d→d` (array/object): dane ramki | Po flushu bufora anomalii. | slot bramki | <code>{&quot;t&quot;:&quot;ab&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;d&quot;:[[1,&quot;th&quot;,31.2]]}</code> (55 B)<br><code>{&quot;t&quot;:&quot;a&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;d&quot;:[[1,&quot;th&quot;,31.2]]}</code> (54 B) |
| `ac_all` → `aa` | SUP→G | Kasowanie całego kubełka anomalii. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`b→b` (str): kubełek anomalii | Klik kasowania kubełka. | slot supervisora | <code>{&quot;t&quot;:&quot;ac_all&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;b&quot;:&quot;offline&quot;}</code> (37 B)<br><code>{&quot;t&quot;:&quot;aa&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;b&quot;:&quot;offline&quot;}</code> (33 B) |
| `ac_b` → `ac` | SUP→G | Kasowanie wskazanych anomalii. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki | Klik kasowania pojedynczej anomalii. | slot supervisora | <code>{&quot;t&quot;:&quot;ac_b&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[&quot;Temp 1&quot;,&quot;temp&quot;]]}</code> (45 B)<br><code>{&quot;t&quot;:&quot;ac&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:[[&quot;Temp 1&quot;,&quot;temp&quot;]]}</code> (43 B) |
| `an_d` → `ad` | G→SUP | Delta anomalii z kontrolnym hashem. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`ts→ts` (int): czas Unix<br>`a→a` (array): dodane anomalie<br>`r→r` (array): usunięte anomalie<br>`h→h` (str): hash lub hash supervisora | Po dodaniu/usunięciu anomalii. | slot bramki | <code>{&quot;t&quot;:&quot;an_d&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;a&quot;:[],&quot;r&quot;:[],&quot;h&quot;:&quot;abcd&quot;}</code> (62 B)<br><code>{&quot;t&quot;:&quot;ad&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;ts&quot;:1700000000,&quot;a&quot;:[],&quot;r&quot;:[],&quot;h&quot;:&quot;abcd&quot;}</code> (60 B) |
| `ansnap_req` → `ar` | SUP→G | Żądanie pełnej migawki anomalii. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Przy rozjeździe hasha anomalii. | slot supervisora | <code>{&quot;t&quot;:&quot;ansnap_req&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (27 B)<br><code>{&quot;t&quot;:&quot;ar&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `dump_anom` → `du` | SUP→G | Wymuszenie pełnego raportu anomalii. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Przycisk lub cykl reconcile. | slot supervisora | <code>{&quot;t&quot;:&quot;dump_anom&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (26 B)<br><code>{&quot;t&quot;:&quot;du&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |

### Parametry

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `cfg` → `cf` | SUP→G | Zgodność ze starą ramką konfiguracji. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`to→to` (object): timeouty legacy | Wyłącznie dla zgodności ze starym peerem. | slot supervisora | <code>{&quot;t&quot;:&quot;cfg&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;to&quot;:{&quot;sw&quot;:1800,&quot;sn&quot;:86400}}</code> (48 B)<br><code>{&quot;t&quot;:&quot;cf&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;to&quot;:{&quot;sw&quot;:1800,&quot;sn&quot;:86400}}</code> (47 B) |
| `param_upd` → `pu` | G→SUP | Potwierdzony stan parametrów bramki. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki<br>`ph→ph` (str): hash parametrów | Po lokalnej zmianie lub zastosowaniu `params`. | slot bramki | <code>{&quot;t&quot;:&quot;param_upd&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:{&quot;P1&quot;:20},&quot;ph&quot;:&quot;abcd&quot;}</code> (52 B)<br><code>{&quot;t&quot;:&quot;pu&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:{&quot;P1&quot;:20},&quot;ph&quot;:&quot;abcd&quot;}</code> (45 B) |
| `params` → `ps` | SUP→G | Propozycja parametrów z supervisora. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`d→d` (array/object): dane ramki | Po kliknięciu Wyślij konfigurację. | slot supervisora | <code>{&quot;t&quot;:&quot;params&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:{&quot;P1&quot;:20,&quot;T1&quot;:1800}}</code> (47 B)<br><code>{&quot;t&quot;:&quot;ps&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;d&quot;:{&quot;P1&quot;:20,&quot;T1&quot;:1800}}</code> (43 B) |
| `params_req` → `pr` | SUP→G | Żądanie pełnego stanu parametrów. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Start lub auto-heal parametrów. | slot supervisora | <code>{&quot;t&quot;:&quot;params_req&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (27 B)<br><code>{&quot;t&quot;:&quot;pr&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |

### ReliableTransfer

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `rt_begin` → `rb` | obie | Otwarcie transferu pliku/payloadu. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`kind→k` (enum): rodzaj payloadu<br>`n→n` (int): liczba chunków<br>`crc→crc` (str): CRC/MD5-4 skompresowanej całości<br>`win→w` (int): rozmiar okna | Raz na początek każdego transferu. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;rt_begin&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;kind&quot;:&quot;devmap&quot;,&quot;n&quot;:3,&quot;crc&quot;:&quot;cafe&quot;,&quot;win&quot;:4}</code> (81 B)<br><code>{&quot;t&quot;:&quot;rb&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;k&quot;:&quot;md&quot;,&quot;n&quot;:3,&quot;crc&quot;:&quot;cafe&quot;,&quot;w&quot;:4}</code> (66 B) |
| `rt_chunk` → `rk` | obie | Fragment skompresowanego payloadu. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`s→s` (int): numer chunka<br>`d→d` (array/object): dane ramki<br>`w→w` (int): znacznik końca okna | Dla każdego fragmentu/retransmisji. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;rt_chunk&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0,&quot;d&quot;:&quot;AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&quot;,&quot;w&quot;:1}</code> (147 B)<br><code>{&quot;t&quot;:&quot;rk&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0,&quot;d&quot;:&quot;AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA&quot;,&quot;w&quot;:1}</code> (141 B) |
| `rt_cack` → `rc` | obie | ACK chunka lub okna chunków. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`s→s` (int): numer chunka<br>`m→m` (array/int): braki lub tryb | Po chunku albo końcu okna. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;rt_cack&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:3,&quot;m&quot;:[1]}</code> (51 B)<br><code>{&quot;t&quot;:&quot;rc&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:3,&quot;m&quot;:[1]}</code> (46 B) |
| `rt_end` → `re` | obie | Zamknięcie transferu wraz z hashami. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`kind→k` (enum): rodzaj payloadu<br>`n→n` (int): liczba chunków<br>`crc→crc` (str): CRC/MD5-4 skompresowanej całości<br>`hs→hs` (array[str]): hashe końcowe danych/statusu/anomalii | Po wszystkich chunkach i przy retry końca. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;rt_end&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;kind&quot;:&quot;ansnap&quot;,&quot;n&quot;:3,&quot;crc&quot;:&quot;cafe&quot;,&quot;hs&quot;:[&quot;&quot;,&quot;&quot;,&quot;abcd&quot;]}</code> (91 B)<br><code>{&quot;t&quot;:&quot;re&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;k&quot;:&quot;an&quot;,&quot;n&quot;:3,&quot;crc&quot;:&quot;cafe&quot;,&quot;hs&quot;:[&quot;&quot;,&quot;&quot;,&quot;abcd&quot;]}</code> (80 B) |
| `rt_ack` → `ra` | obie | Końcowy ACK/NACK ReliableTransfer. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`ok→ok` (0/1): wynik transferu<br>`miss→mi` (array[int]): brakujące chunki | Po złożeniu/zweryfikowaniu transferu. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;rt_ack&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;ok&quot;:0,&quot;miss&quot;:[2]}</code> (54 B)<br><code>{&quot;t&quot;:&quot;ra&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;ok&quot;:0,&quot;mi&quot;:[2]}</code> (48 B) |

### Kalendarz

| Długa / krótka nazwa | Kierunek | Cel | Pola: długi→krótki, typ, znaczenie | Kiedy | Slot | Przykład przed / po |
|---|---|---|---|---|---|---|
| `cal_begin` → `cb` | obie | Otwarcie transferu kalendarza. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`dir→dir` (enum): kierunek/tryb kalendarza<br>`n→n` (int): liczba chunków<br>`crc→crc` (str): CRC/MD5-4 skompresowanej całości | Raz na początek synchronizacji kalendarza. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;cal_begin&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;dir&quot;:&quot;gw_push&quot;,&quot;n&quot;:2,&quot;crc&quot;:&quot;cafe&quot;}</code> (74 B)<br><code>{&quot;t&quot;:&quot;cb&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;dir&quot;:&quot;gp&quot;,&quot;n&quot;:2,&quot;crc&quot;:&quot;cafe&quot;}</code> (62 B) |
| `cal_chunk` → `ck` | obie | Fragment skompresowanego kalendarza. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`s→s` (int): numer chunka<br>`d→d` (array/object): dane ramki | Dla każdego fragmentu/retransmisji. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;cal_chunk&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0,&quot;d&quot;:&quot;eJyrVkrLz1eyUkpKLFKqBQA&quot;}</code> (75 B)<br><code>{&quot;t&quot;:&quot;ck&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0,&quot;d&quot;:&quot;eJyrVkrLz1eyUkpKLFKqBQA&quot;}</code> (68 B) |
| `cal_cack` → `cc` | obie | ACK pojedynczego chunka kalendarza. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`s→s` (int): numer chunka | Po każdym fragmencie kalendarza. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;cal_cack&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0}</code> (44 B)<br><code>{&quot;t&quot;:&quot;cc&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;s&quot;:0}</code> (38 B) |
| `cal_end` → `ce` | obie | Zamknięcie transferu kalendarza. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`dir→dir` (enum): kierunek/tryb kalendarza<br>`n→n` (int): liczba chunków<br>`crc→crc` (str): CRC/MD5-4 skompresowanej całości | Po wszystkich fragmentach i przy retry. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;cal_end&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;dir&quot;:&quot;cal&quot;,&quot;n&quot;:2,&quot;crc&quot;:&quot;cafe&quot;}</code> (68 B)<br><code>{&quot;t&quot;:&quot;ce&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;dir&quot;:&quot;cal&quot;,&quot;n&quot;:2,&quot;crc&quot;:&quot;cafe&quot;}</code> (63 B) |
| `cal_ack` → `ca` | obie | Końcowy ACK/NACK kalendarza. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki<br>`tid→tid` (str): id transferu<br>`ok→ok` (0/1): wynik transferu<br>`miss→mi` (array[int]): brakujące chunki | Po złożeniu/zweryfikowaniu kalendarza. | TX bramki: slot bramki; reaktywny TX SUP (`*_cack`, `*_ack`): natychmiast | <code>{&quot;t&quot;:&quot;cal_ack&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;ok&quot;:0,&quot;miss&quot;:[2]}</code> (55 B)<br><code>{&quot;t&quot;:&quot;ca&quot;,&quot;g&quot;:&quot;G1&quot;,&quot;tid&quot;:&quot;a1b2&quot;,&quot;ok&quot;:0,&quot;mi&quot;:[2]}</code> (48 B) |
| `cal_pull` → `cl` | SUP→G | Polecenie wysłania kalendarza z bramki. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Klik Pull Calendar. | slot supervisora | <code>{&quot;t&quot;:&quot;cal_pull&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (25 B)<br><code>{&quot;t&quot;:&quot;cl&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |
| `cal_req` → `cr` | G→SUP | Prośba bramki o kalendarz supervisora. | `t→t` (enum): typ ramki<br>`g→g` (str): identyfikator/adres bramki | Gdy bramka nie ma kalendarza. | slot bramki | <code>{&quot;t&quot;:&quot;cal_req&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (24 B)<br><code>{&quot;t&quot;:&quot;cr&quot;,&quot;g&quot;:&quot;G1&quot;}</code> (19 B) |

## Payloady ReliableTransfer

| `kind` długi→krótki | Kierunek | Zawartość |
|---|---|---|
| `devmap→md` | G→SUP | pełna mapa urządzeń |
| `avail→av` | G→SUP | pełna migawka dostępności |
| `bdata→bd` | G→SUP | skompresowany batch danych |
| `ansnap→an` | G→SUP | pełna migawka anomalii |
| `dsnap→ds` | G→SUP | pełna migawka stanu/statusu |
| `mass_offline→mo` | G→SUP | masowe oznaczenie urządzeń offline |

## Niezmienniki

- Każda adresowana ramka zachowuje identyfikator bramki w jednoliterowym kluczu `g`.
- Nieznane typy i klucze przechodzą przez dekoder bez zmian.
- `decode(encode(frame)) == frame` dla wszystkich ramek katalogu.
- Każdy przykład radiowy ma nie więcej niż 220 B; większe dane używają transferu chunkowanego.
- `tid`, `crc`, `hs`, `ph` i `ts` już mają 1–3 znaki, więc pozostają bez zmian.
- Krótkie klucze istniejące od dawna (`g`, `d`, `h`, `m`, `s`, `p`, `v`, `c` itd.) nie są przemianowywane.
