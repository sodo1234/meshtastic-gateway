// Buduje widok "Anomalie" dla dashboardu bramki carport (baner + lista offline z availability).
// Wzor: gen_gateway_anomalies.py. Zrodlo: carport_real_entities.json (realne entity_id).
const fs = require("fs");
const SC = __dirname;
const d = JSON.parse(fs.readFileSync(SC + "/carport_real_entities.json", "utf8"));
const RELAY_RE = /^switch\.(r[ab]\d+_\d+|test_\d+)$/i;
const relays = d.switches.filter(([id]) => RELAY_RE.test(id));
const motion = d.occupancy;
const relayIds = relays.map(x => x[0]);
const motionIds = motion.map(x => x[0]);
const NM = {};
relays.forEach(([id, fn]) => NM[id] = fn.trim());
motion.forEach(([id, fn]) => NM[id] = fn.replace(/\s*(zaj\S*|occupancy)\s*$/i, "").trim());

const R = JSON.stringify(relayIds), M = JSON.stringify(motionIds), NMj = JSON.stringify(NM);
const off = "function off(i){var e=states[i];return !e||e.state==='unavailable'||e.state==='unknown';}";

const BANNER = "[[[ var relays=" + R + ";var motion=" + M + ";" + off +
  "var ro=relays.filter(off).length;var mo=motion.filter(off).length;var total=ro+mo;var col=total>0?'#ef4444':'#4ade80';" +
  "return `<div style=\"text-align:center;width:100%;\">` +" +
  "`<div style=\"font-size:13px;letter-spacing:6px;font-weight:800;color:#fca5a5;\">🚨 URZĄDZENIA OFFLINE 🚨</div>` +" +
  "`<div style=\"font-size:84px;line-height:1;font-weight:900;color:${col};text-shadow:0 0 32px ${col};\">${total}</div>` +" +
  "`<div style=\"display:flex;justify-content:center;gap:10px;flex-wrap:wrap;margin-top:10px;\">` +" +
  "`<span style=\"background:#7f1d1d;color:#fecaca;padding:5px 16px;border-radius:20px;font-weight:800;\">📴 PRZEKAŹNIKI ${ro}/${relays.length}</span>` +" +
  "`<span style=\"background:#78350f;color:#fde68a;padding:5px 16px;border-radius:20px;font-weight:800;\">🎯 CZUJNIKI ${mo}/${motion.length}</span>` +" +
  "`</div></div>`; ]]]";

const LIST = "[[[ var relays=" + R + ";var motion=" + M + ";var NM=" + NMj + ";var rows='';" + off +
  "function add(ids,icon,cls){ids.forEach(function(i){if(off(i)){var nm=NM[i]||i;" +
  "rows+=`<div style=\"display:flex;align-items:center;justify-content:space-between;padding:11px 16px;margin:6px 0;background:#160a0a;border-left:4px solid #ef4444;border-radius:8px;\">` +" +
  "`<span style=\"display:flex;align-items:center;\"><span style=\"font-size:20px;margin-right:12px;\">${icon}</span>` +" +
  "`<span style=\"font-weight:700;color:#e5e5e5;font-size:14px;\">${nm}</span>` +" +
  "`<span style=\"color:#9ca3af;font-size:10px;margin-left:10px;letter-spacing:1px;\">${cls}</span></span>` +" +
  "`<span style=\"font-weight:900;font-size:16px;color:#fca5a5;\">OFFLINE</span></div>`;}});}" +
  "add(relays,'📴','PRZEKAŹNIK');add(motion,'🎯','CZUJNIK RUCHU');" +
  "return rows||`<div style=\"text-align:center;color:#4ade80;padding:24px;font-weight:800;font-size:18px;\">✅ Wszystkie urządzenia online</div>`; ]]]";

function card(js, pulse) {
  const c = { type: "custom:button-card", show_icon: false, show_name: false, show_state: false,
    entity: relayIds[0], triggers_update: "all", tap_action: { action: "none" },
    custom_fields: { content: js },
    styles: { card: [{ background: "#0a0a0a" }, { border: "2px solid #7f1d1d" }, { "border-radius": "16px" },
      { padding: "18px 20px" }, { "box-shadow": "none" }], custom_fields: { content: [{ width: "100%" }] } } };
  if (pulse) c.card_mod = { style: "@keyframes anompulse{0%,100%{box-shadow:0 0 16px #7f1d1d66;}50%{box-shadow:0 0 46px #ef4444cc;}}ha-card{animation:anompulse 1.6s ease-in-out infinite;}" };
  return c;
}

// KAFEL LICZNIK (kompakt) -> tap otwiera browser_mod.popup z banerem+lista (jak supervisor)
const COUNT = "[[[ var relays=" + R + ";var motion=" + M + ";" + off +
  "var ro=relays.filter(off).length;var mo=motion.filter(off).length;var total=ro+mo;var col=total>0?'#ef4444':'#4ade80';" +
  "return `<div style=\"display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:16px;width:100%;\">` +" +
  "`<span style=\"font-size:34px;\">🚨</span>` +" +
  "`<div style=\"display:flex;flex-direction:column;\"><span style=\"font-size:11px;letter-spacing:3px;font-weight:800;color:#fca5a5;\">URZĄDZENIA OFFLINE</span>` +" +
  "`<span style=\"font-size:12px;color:#9ca3af;\">📴 ${ro}/${relays.length}  ·  🎯 ${mo}/${motion.length}  —  dotknij po szczegóły</span></div>` +" +
  "`<span style=\"font-size:44px;line-height:1;font-weight:900;color:${col};text-shadow:0 0 20px ${col};\">${total}</span></div>`; ]]]";

const popupTile = {
  type: "custom:button-card", entity: relayIds[0], triggers_update: "all",
  show_icon: false, show_name: false, show_state: false,
  custom_fields: { content: COUNT },
  tap_action: { action: "fire-dom-event", browser_mod: { service: "browser_mod.popup", data: {
    title: "🚨 Anomalie — Carport", dismissable: true, size: "wide",
    content: { type: "vertical-stack", cards: [card(BANNER, true), card(LIST, false)] },
    style: "--popup-max-width: min(760px,94vw); --mdc-theme-surface: #0a0a0a; --primary-background-color: #0a0a0a;" } } },
  styles: { card: [{ background: "#0a0a0a" }, { border: "2px solid #7f1d1d" }, { "border-radius": "16px" },
    { padding: "16px 22px" }, { "box-shadow": "none" }, { cursor: "pointer" }],
    custom_fields: { content: [{ width: "100%" }] } },
  card_mod: { style: "@keyframes anompulse{0%,100%{box-shadow:0 0 14px #7f1d1d55;}50%{box-shadow:0 0 40px #ef4444aa;}}ha-card{animation:anompulse 1.8s ease-in-out infinite;}" }
};

const view = { path: "carport-anomalie", title: "🚨 Anomalie", icon: "mdi:alert-octagram", badges: [],
  cards: [popupTile] };
const out = process.argv[2];
fs.writeFileSync(out, JSON.stringify({ anomaly_view: view, popup_tile: popupTile }));
console.log("relays:", relayIds.length, "motion:", motionIds.length, "bytes:", JSON.stringify({ anomaly_view: view }).length);
