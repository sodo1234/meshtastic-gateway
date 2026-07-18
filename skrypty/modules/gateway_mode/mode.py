"""GatewayMode — tryb pracy bramki day/night/all-time (step 5).

Cel: bramka „nocna" w dzień NIE zgłasza offline dla urządzeń świadomie bez zasilania
(np. zasilanie tylko po zmroku). Liczone LOKALNIE na bramce (harmonogram), niezależnie od HA.

operating_mode:
  - 'all-time' (domyślnie): zawsze aktywna → brak supresji.
  - 'day'  : aktywna w dzień (między świtem a zmierzchem).
  - 'night': aktywna w nocy (poza dniem).

Okno dnia:
  - domyślnie STAŁE GODZINY `day_start`–`day_end` ("HH:MM", zegar lokalny bramki).
  - jeśli podasz lat/lon → zmierzch/świt liczony solarnie (sunrise equation) w czasie lokalnym.

`is_active(now)` → bool używane do supresji anomalii offline (GatewayOfflineAnomaly.is_active
i OfflineMonitor.is_suppressed po stronie supervisora przez stan w HB).
"""
import math
import time


class GatewayMode:
    def __init__(self, operating_mode="all-time", day_start="06:00", day_end="20:00",
                 lat=None, lon=None, clock=time.time, logger=None):
        self.mode = (operating_mode or "all-time").lower()
        self.day_start = self._parse_hm(day_start, 6 * 60)
        self.day_end = self._parse_hm(day_end, 20 * 60)
        self.lat = lat
        self.lon = lon
        self.clock = clock
        self.log = logger

    @staticmethod
    def _parse_hm(s, default_min):
        try:
            h, m = str(s).split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return default_min

    # ── okno dnia ───────────────────────────────────────
    def _day_window(self, lt):
        """(start_min, end_min) okna dnia dla daty lt (time.struct_time lokalny)."""
        if self.lat is not None and self.lon is not None:
            sr, ss = self._solar(lt)
            if sr is not None:
                return sr, ss
        return self.day_start, self.day_end

    def _is_daytime(self, now):
        lt = time.localtime(now)
        mins = lt.tm_hour * 60 + lt.tm_min
        s, e = self._day_window(lt)
        return s <= mins < e

    def _solar(self, lt):
        """Świt/zmierzch (minuty od północy, czas LOKALNY) z lat/lon. Sunrise equation,
        zenit oficjalny 90.833°. Zwraca (None,None) gdy biegun/polar day-night."""
        try:
            n = lt.tm_yday
            lng_hour = self.lon / 15.0
            results = []
            for rising in (True, False):
                t = n + ((6 - lng_hour) / 24.0 if rising else (18 - lng_hour) / 24.0)
                M = (0.9856 * t) - 3.289
                L = (M + 1.916 * math.sin(math.radians(M))
                     + 0.020 * math.sin(math.radians(2 * M)) + 282.634) % 360
                RA = math.degrees(math.atan(0.91764 * math.tan(math.radians(L)))) % 360
                RA += (math.floor(L / 90) * 90) - (math.floor(RA / 90) * 90)
                RA /= 15.0
                sinDec = 0.39782 * math.sin(math.radians(L))
                cosDec = math.cos(math.asin(sinDec))
                cosH = ((math.cos(math.radians(90.833)) - sinDec * math.sin(math.radians(self.lat)))
                        / (cosDec * math.cos(math.radians(self.lat))))
                if cosH > 1 or cosH < -1:
                    return None, None        # polar — brak świtu/zmierzchu
                H = (360 - math.degrees(math.acos(cosH)) if rising
                     else math.degrees(math.acos(cosH))) / 15.0
                T = H + RA - (0.06571 * t) - 6.622
                UT = (T - lng_hour) % 24
                # UT → czas lokalny przez offset systemu (lokalny zegar bramki)
                offset_h = -time.timezone / 3600.0
                if lt.tm_isdst > 0 and time.daylight:
                    offset_h = -time.altzone / 3600.0
                local_h = (UT + offset_h) % 24
                results.append(int(round(local_h * 60)))
            return results[0], results[1]
        except Exception:
            return None, None

    # ── API ─────────────────────────────────────────────
    def is_active(self, now=None):
        now = self.clock() if now is None else now
        if self.mode == "all-time":
            return True
        day = self._is_daytime(now)
        return day if self.mode == "day" else (not day)

    def set_mode(self, mode):
        """Zmiana trybu w RUNTIME (dashboard/LoRa). Zwraca True gdy prawidlowy i ustawiony.
        Wplyw na is_active() -> gating raportowania stanow (GatewayData.active_fn) natychmiast."""
        m = (mode or "").lower()
        if m not in ("all-time", "day", "night"):
            if self.log:
                self.log.warn("MODE", "set_mode: nieznany tryb %r" % (mode,))
            return False
        self.mode = m
        if self.log:
            self.log.info("MODE", "operating_mode -> %s" % m)
        return True

    def state(self, now=None):
        """Dla HB diag_fn: tryb + czy aktywna (supervisor czyta do supresji)."""
        return {"gm": self.mode, "ga": 1 if self.is_active(now) else 0}

    def day_info(self, now=None):
        """Pora dnia + okno świt/zmierzch (minuty od północy, czas LOKALNY) — do wizualizacji
        na dashboardzie. Liczone lokalnie (stałe godziny lub solar lat/lon), niezależnie od HA."""
        now = self.clock() if now is None else now
        lt = time.localtime(now)
        mins = lt.tm_hour * 60 + lt.tm_min
        s, e = self._day_window(lt)
        return {"is_day": bool(s <= mins < e), "sunrise_min": int(s), "sunset_min": int(e)}
