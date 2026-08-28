"use strict";

const html = htm.bind(React.createElement);
const { useState, useEffect, useRef, useMemo } = React;

const REDUCED = matchMedia("(prefers-reduced-motion: reduce)").matches;

/* ─────────── formatage ─────────── */

const fmt = (v, d = 2) =>
  v === null || v === undefined || Number.isNaN(Number(v))
    ? "—" : Number(v).toFixed(d);
const int = (v) =>
  v === null || v === undefined ? "—" : Math.round(v).toLocaleString("fr-FR");
const pct = (v, d = 1) => (v === null || v === undefined ? "—" : fmt(v, d) + " %");
const weight = (bytes) => {
  if (!bytes) return "—";
  const units = ["o", "Ko", "Mo", "Go"];
  let value = bytes, rank = 0;
  while (value >= 1024 && rank < units.length - 1) { value /= 1024; rank += 1; }
  return `${value.toFixed(rank ? 1 : 0)} ${units[rank]}`;
};

const css = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();

/* ─────────── donnees ─────────── */

const EVENTS = (DATA && DATA.seismes) || [];
const TIMELINES = (DATA && DATA.chronologies) || {};
const CURVE = (DATA && DATA.courbe_alerte) || [];
const MODEL = DATA && DATA.modele;
const COHORTS = (DATA && DATA.cohortes) || [];
const WITNESS = DATA && DATA.temoin_humain;

/* ─────────── moteur d'inference ─────────── */

function buildEngine(exported) {
  const stages = exported.etages;
  const labels = new Map();
  for (const indexer of stages.indexeurs) labels.set(indexer.colonne, indexer.modalites);

  // Largeur d'un vecteur one-hot chez Spark. La regle n'est pas
  // "categories moins un" : quand dropLast et handleInvalid=keep sont tous
  // deux actifs, la case abandonnee EST celle de l'invalide, et la largeur
  // reste inchangee. Se tromper ici decale tous les indices de variables
  // sans qu'aucune prediction ne leve d'erreur.
  const widthOf = (encoder) => {
    const drop = encoder.abandonne_la_derniere;
    const keep = encoder.invalide_conserve !== false;
    if (drop && keep) return encoder.categories;
    if (drop && !keep) return encoder.categories - 1;
    if (!drop && keep) return encoder.categories + 1;
    return encoder.categories;
  };

  const sizes = new Map();
  for (const encoder of stages.encodeurs) {
    const source = encoder.colonne.replace(/_index$/, "");
    sizes.set(source, { width: widthOf(encoder), categories: encoder.categories });
  }

  const plan = [];
  for (const column of stages.assemblage) {
    if (column.endsWith("_filled")) {
      const source = column.slice(0, -"_filled".length);
      plan.push({ genre: "nombre", source,
        mediane: stages.imputation.medianes[source] });
    } else if (column.endsWith("_absent")) {
      plan.push({ genre: "absence", source: column.slice(0, -"_absent".length) });
    } else if (column.endsWith("_vector")) {
      const source = column.slice(0, -"_vector".length);
      const size = sizes.get(source) || { width: 0, categories: 0 };
      plan.push({ genre: "categorie", source,
        modalites: labels.get(source) || [],
        width: size.width, categories: size.categories });
    }
  }

  const assemble = (values) => {
    const vector = [];
    for (const slot of plan) {
      const raw = values[slot.source];
      const missing = raw === null || raw === undefined || raw === ""
        || (slot.genre === "nombre" && Number.isNaN(Number(raw)));

      if (slot.genre === "nombre") {
        vector.push(missing ? Number(slot.mediane) || 0 : Number(raw));
      } else if (slot.genre === "absence") {
        vector.push(missing ? 1 : 0);
      } else {
        let index = slot.modalites.indexOf(raw);
        if (index < 0) index = slot.modalites.length;
        for (let position = 0; position < slot.width; position += 1) {
          vector.push(position === index ? 1 : 0);
        }
      }
    }
    return vector;
  };

  const walk = (root, vector) => {
    let node = root;
    while (node.valeur === undefined) {
      const value = vector[node.variable];
      const goLeft = node.seuil !== undefined
        ? value <= node.seuil
        : node.categories.includes(value);
      node = goLeft ? node.gauche : node.droite;
    }
    return node.valeur;
  };

  const score = (values) => {
    const vector = assemble(values);
    const votes = exported.foret.arbres.map((tree, index) =>
      walk(tree, vector) * exported.foret.poids[index]);
    const raw = votes.reduce((total, vote) => total + vote, 0);
    return {
      brut: raw,
      probabilite: 1 / (1 + Math.exp(-2 * raw)),
      classe: raw > 0 ? 1 : 0,
      votes,
      taille: vector.length,
    };
  };

  const built = plan.reduce(
    (total, slot) => total + (slot.genre === "categorie" ? slot.width : 1), 0);
  const attendu = exported.foret.nombre_de_variables;

  return {
    plan, score,
    largeur: built,
    largeur_attendue: attendu,
    coherent: built === attendu,
  };
}

const ENGINE = FOREST ? buildEngine(FOREST) : null;

function verifyEngine() {
  if (!ENGINE || !FOREST.temoins || !FOREST.temoins.length) return null;
  let worst = 0, agree = 0;
  for (const witness of FOREST.temoins) {
    const outcome = ENGINE.score(witness);
    const gap = Math.abs(outcome.classe - Number(witness.prediction_spark));
    worst = Math.max(worst, gap);
    if (gap < 0.5) agree += 1;
  }
  return { total: FOREST.temoins.length, accord: agree, ecart_max: worst };
}

/* ─────────── scoring du flux temps reel ─────────── */

// Le modele a ete entrainé sur le catalogue M>=4. Le flux horaire est
// majoritairement sous M2 : scorer ces evenements est une extrapolation hors
// du domaine d'apprentissage. On les score quand meme -- mais on le DIT,
// plutot que d'afficher un verdict qui aurait l'air aussi sur que les autres.
const TRAINING_MIN_MAGNITUDE = 4.0;

const SATURATION = [
  ["mww", 10], ["mwr", 10], ["mwc", 10], ["mwb", 10], ["mw", 10],
  ["ms_vx", 8], ["ms", 8], ["mb_lg", 6.5], ["mb", 6.5],
  ["mlr", 6.5], ["ml", 6.5], ["md", 5], ["mh", 5],
];

const FAMILY = [
  ["mww", "moment"], ["mwr", "moment"], ["mwc", "moment"],
  ["mwb", "moment"], ["mw", "moment"],
  ["ms_vx", "surface_wave"], ["ms", "surface_wave"],
  ["mb_lg", "body_wave"], ["mb", "body_wave"],
  ["mlr", "local"], ["ml", "local"],
  ["md", "duration"], ["mh", "duration"],
];

function lookup(table, type, fallback) {
  const token = (type || "").toLowerCase();
  for (const [prefix, value] of table) {
    if (token.startsWith(prefix)) return value;
  }
  return fallback;
}

const saturationValue = (type) => lookup(SATURATION, type, 5);
const familyOf = (type) => lookup(FAMILY, type, "inconnue");

function liveFeatures(event) {
  const minutes = event.time
    ? Math.max(0, (Date.now() - event.time) / 60000) : null;
  return {
    first_magnitude: event.magnitude,
    first_station_count: event.station_count,
    first_azimuthal_gap: event.azimuthal_gap,
    first_minimum_distance: event.minimum_distance,
    first_standard_error: event.standard_error,
    first_depth_km: event.depth,
    first_latitude: event.latitude,
    first_longitude: event.longitude,
    first_minutes_since_quake: minutes,
    first_saturation: saturationValue(event.magnitude_type),
    first_magnitude_type: (event.magnitude_type || "").toLowerCase(),
    first_review_status: (event.status || "").toLowerCase(),
    first_contributor: (event.network || "").toLowerCase(),
    first_family: familyOf(event.magnitude_type),
    // Non publiees par le flux horaire : le moteur imputera la mediane du jeu
    // d'entrainement, exactement comme le pipeline Spark le fait.
    first_magnitude_station_count: null,
    first_phase_count: null,
    first_horizontal_error: null,
    first_magnitude_error: null,
  };
}

const IMPUTED_AT_INFERENCE = [
  "nombre de stations de magnitude", "nombre de phases",
  "erreur horizontale", "erreur de magnitude",
];

function scoreLive(event) {
  if (!ENGINE || event.magnitude === null || event.magnitude === undefined) {
    return null;
  }
  const outcome = ENGINE.score(liveFeatures(event));
  const inDomain = event.magnitude >= TRAINING_MIN_MAGNITUDE;
  return {
    suspicion: outcome.probabilite * 100,
    brut: outcome.brut,
    suspect: outcome.classe === 1,
    inDomain,
    etat: !inDomain ? "hors_domaine" : (outcome.classe === 1 ? "suspect" : "stable"),
  };
}

/* ─────────── outils canvas ─────────── */

function fitCanvas(canvas) {
  const ratio = Math.min(devicePixelRatio || 1, 2);
  const width = canvas.clientWidth, height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { ctx, width, height };
}

function useCanvas(draw, deps) {
  const ref = useRef(null);
  useEffect(() => {
    const canvas = ref.current;
    if (!canvas) return undefined;
    const paint = () => draw(canvas);
    paint();
    addEventListener("resize", paint);
    return () => removeEventListener("resize", paint);
  }, deps);
  return ref;
}

/* ─────────── flux temps reel ─────────── */

function useLive() {
  const [state, setState] = useState({
    connected: false, kafka: null, stack: null, history: [],
  });

  useEffect(() => {
    let source;
    try {
      source = new EventSource("/api/live");
    } catch (error) {
      return undefined;
    }
    source.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        setState((previous) => {
          const rate = (payload.kafka && payload.kafka.cadence_par_minute) || 0;
          const history = [...previous.history, rate].slice(-90);
          return { connected: true, kafka: payload.kafka,
            stack: payload.pile, history };
        });
      } catch (error) { /* trame incomplete, la suivante arrivera */ }
    };
    source.onerror = () =>
      setState((previous) => ({ ...previous, connected: false }));
    return () => source.close();
  }, []);

  return state;
}

/* ─────────── briques ─────────── */

// Les chiffres sont le contenu de cette page : les faire monter attire l'oeil
// dessus au lieu de les livrer inertes. Une seule passe au montage, jamais en
// boucle -- une animation qui tourne en permanence fatigue et coute.
function useCountUp(target, duration = 900) {
  const [shown, setShown] = useState(0);
  useEffect(() => {
    if (!Number.isFinite(target)) return undefined;
    if (REDUCED) { setShown(target); return undefined; }

    let frame;
    const started = performance.now();
    const step = (now) => {
      const t = Math.min(1, (now - started) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      setShown(target * eased);
      if (t < 1) frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);

    // Un navigateur suspend requestAnimationFrame quand la page n'est pas
    // visible : dans un onglet d'arriere-plan, l'animation ne demarre jamais
    // et le compteur resterait a zero. Le filet garantit la valeur finale,
    // que l'animation ait tourne ou non.
    const net = setTimeout(() => setShown(target), duration + 250);

    return () => { cancelAnimationFrame(frame); clearTimeout(net); };
  }, [target, duration]);
  return shown;
}

const Counter = ({ to, digits }) => {
  const shown = useCountUp(to);
  // Un composant React peut rendre une chaine : pas besoin d'un gabarit htm
  // pour une valeur nue.
  return digits ? fmt(shown, digits) : int(shown);
};

const Stat = ({ value, count, digits, label, tone, suffix }) => html`
  <div class="stat">
    <div class=${"k" + (tone ? " " + tone : "")}>
      ${count !== undefined && count !== null && Number.isFinite(count)
        ? html`<${Counter} to=${count} digits=${digits} />${suffix || ""}`
        : value}
    </div>
    <div class="t">${label}</div>
  </div>`;

const Panel = ({ title, aside, children, className }) => html`
  <section class=${"panel" + (className ? " " + className : "")}>
    ${title && html`<div class="panel-head"><h3>${title}</h3>${aside}</div>`}
    ${children}
  </section>`;

const Note = ({ warn, children }) => html`
  <p class=${"note" + (warn ? " warn" : "")}>${children}</p>`;

const Line = ({ label, value }) => html`
  <div class="line"><span>${label}</span><span>${value}</span></div>`;

const Head = ({ title, children }) => html`
  <div class="view-head"><h2>${title}</h2><p>${children}</p></div>`;

/* ─────────── vue 1 : le direct ─────────── */

const PIPE = [
  { key: null, label: "USGS", sub: "all_hour" },
  { key: null, label: "Kafka", sub: "quakes_live" },
  { key: null, label: "Spark", sub: "streaming" },
  { key: "bronze_flux", label: "Bronze", sub: "brut" },
  { key: "silver", label: "Silver", sub: "Delta" },
  { key: "gold", label: "Gold", sub: "Parquet" },
];

const LAYER_NAMES = {
  bronze_flux: "Bronze · flux temps reel",
  bronze_catalogue: "Bronze · catalogue",
  bronze_versions: "Bronze · historique des versions",
  silver: "Silver · Delta",
  gold: "Gold · Parquet",
};

function drawPipeline(canvas, layers, flowing, phase) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  const mid = height * 0.44;
  const margin = 48;
  const step = (width - margin * 2) / (PIPE.length - 1);
  const at = (i) => margin + i * step;

  ctx.strokeStyle = css("--rule");
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(at(0), mid);
  ctx.lineTo(at(PIPE.length - 1), mid);
  ctx.stroke();

  if (flowing) {
    for (let k = 0; k < 5; k += 1) {
      const t = (phase + k * 0.2) % 1;
      const x = at(0) + (at(PIPE.length - 1) - at(0)) * t;
      ctx.globalAlpha = Math.sin(t * Math.PI) * 0.95;
      ctx.fillStyle = css("--settled");
      ctx.shadowBlur = 10;
      ctx.shadowColor = css("--settled");
      ctx.beginPath();
      ctx.arc(x, mid, 2.8, 0, Math.PI * 2);
      ctx.fill();
      ctx.shadowBlur = 0;
      ctx.globalAlpha = 1;
    }
  }

  PIPE.forEach((node, i) => {
    const x = at(i);
    ctx.fillStyle = css("--crust");
    ctx.strokeStyle = flowing && i <= 3 ? css("--settled") : css("--rule");
    ctx.lineWidth = 1.2;
    ctx.beginPath();
    ctx.arc(x, mid, 13, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = css("--ink");
    ctx.font = "600 11.5px 'Space Grotesk',sans-serif";
    ctx.textAlign = "center";
    ctx.fillText(node.label, x, mid - 27);

    ctx.fillStyle = css("--muted");
    ctx.font = "400 9px 'IBM Plex Mono',monospace";
    ctx.fillText(node.sub, x, mid - 15);

    const layer = node.key && layers[node.key];
    if (layer) {
      ctx.fillStyle = css("--ink-2");
      ctx.font = "500 10px 'IBM Plex Mono',monospace";
      ctx.fillText(`${int(layer.fichiers)} fichiers`, x, mid + 33);
      ctx.fillStyle = css("--muted");
      ctx.font = "400 9px 'IBM Plex Mono',monospace";
      ctx.fillText(weight(layer.octets), x, mid + 45);
    }
  });
  ctx.textAlign = "left";
}

function drawThroughput(canvas, series) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  if (series.length < 2) return;

  const peak = Math.max(...series, 1) * 1.2;
  const x = (i) => (i / (series.length - 1)) * width;
  const y = (v) => height - 4 - (v / peak) * (height - 12);

  ctx.beginPath();
  ctx.moveTo(x(0), height);
  series.forEach((value, i) => ctx.lineTo(x(i), y(value)));
  ctx.lineTo(x(series.length - 1), height);
  ctx.closePath();
  ctx.fillStyle = css("--settled-wash");
  ctx.fill();

  ctx.beginPath();
  series.forEach((value, i) =>
    i ? ctx.lineTo(x(i), y(value)) : ctx.moveTo(x(i), y(value)));
  ctx.strokeStyle = css("--settled");
  ctx.lineWidth = 1.8;
  ctx.shadowBlur = 8;
  ctx.shadowColor = css("--settled");
  ctx.stroke();
  ctx.shadowBlur = 0;
}

function Gauge({ label, used, total, text }) {
  const share = total ? Math.min(100, (used / total) * 100) : 0;
  return html`
    <div class="gauge">
      <span>${label}</span>
      <div class="gauge-track">
        <div class="gauge-fill" style=${{ width: share + "%" }}></div>
      </div>
      <span class="v">${text}</span>
    </div>`;
}

function drawDialGauge(canvas, share, tone) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  const cx = width / 2;
  const cy = height * 0.92;
  const radius = Math.min(width * 0.42, height * 0.82);
  const from = Math.PI * 1.02;
  const to = Math.PI * 1.98;

  ctx.lineCap = "round";

  ctx.beginPath();
  ctx.arc(cx, cy, radius, from, to);
  ctx.strokeStyle = css("--sunk");
  ctx.lineWidth = 11;
  ctx.stroke();

  const reach = from + (to - from) * Math.max(0, Math.min(1, share / 100));
  ctx.beginPath();
  ctx.arc(cx, cy, radius, from, reach);
  ctx.strokeStyle = css(tone);
  ctx.lineWidth = 11;
  ctx.shadowBlur = 16;
  ctx.shadowColor = css(tone);
  ctx.stroke();
  ctx.shadowBlur = 0;

  // Graduations : sans reperes, un arc colore ne dit pas ou l'on se situe.
  ctx.strokeStyle = css("--muted");
  ctx.lineWidth = 1;
  ctx.globalAlpha = 0.55;
  for (let step = 0; step <= 10; step += 1) {
    const angle = from + (to - from) * (step / 10);
    const inner = radius - 15, outer = radius - (step % 5 === 0 ? 21 : 18);
    ctx.beginPath();
    ctx.moveTo(cx + Math.cos(angle) * inner, cy + Math.sin(angle) * inner);
    ctx.lineTo(cx + Math.cos(angle) * outer, cy + Math.sin(angle) * outer);
    ctx.stroke();
  }
  ctx.globalAlpha = 1;
}

function Chip({ label, value, alarm }) {
  return html`
    <div class=${"chip" + (alarm ? " alarm" : "")}>
      <span class="chip-k">${label}</span>
      <span class="chip-v">${value}</span>
    </div>`;
}

function TriageHero({ event, verdict }) {
  const tone = verdict.etat === "suspect" ? "--prov"
    : verdict.etat === "stable" ? "--settled" : "--muted";
  const gaugeRef = useCanvas(
    (c) => drawDialGauge(c, verdict.suspicion, tone),
    [verdict.suspicion, tone]);

  const words = {
    suspect: "Mefiez-vous de ce chiffre",
    stable: "Le chiffre devrait tenir",
    hors_domaine: "Hors du domaine d'entrainement",
  };

  return html`
    <section class=${"triage " + verdict.etat}>
      <div class="triage-left">
        <div class="eyebrow">le plus suspect dans la fenetre</div>
        <div class="triage-mag">M${fmt(event.magnitude, 1)}</div>
        <div class="triage-place">${event.place || event.id}</div>
        <div class="chips">
          <${Chip} label="stations" value=${int(event.station_count)}
            alarm=${event.station_count !== null && event.station_count < 20} />
          <${Chip} label="gap" value=${event.azimuthal_gap !== null
            && event.azimuthal_gap !== undefined
            ? fmt(event.azimuthal_gap, 0) + "°" : "—"}
            alarm=${event.azimuthal_gap > 180} />
          <${Chip} label="echelle" value=${event.magnitude_type || "—"} />
          <${Chip} label="reseau" value=${event.network || "—"} />
          <${Chip} label="statut" value=${event.status || "—"}
            alarm=${event.status === "automatic"} />
        </div>
      </div>

      <div class="triage-right">
        <canvas class="dial" ref=${gaugeRef}></canvas>
        <div class=${"triage-verdict " + verdict.etat}>${words[verdict.etat]}</div>
        <div class="triage-score">
          ${verdict.etat === "hors_domaine"
            ? `entraine sur M≥${fmt(TRAINING_MIN_MAGNITUDE, 1)} seulement`
            : `indice de suspicion ${fmt(verdict.suspicion, 1)} %`}
        </div>
      </div>
    </section>`;
}

function ViewDirect() {
  const live = useLive();
  const kafka = live.kafka || {};
  const stack = live.stack || {};
  const layers = stack.couches || {};
  const [phase, setPhase] = useState(0);

  const flowing = live.connected && (kafka.cadence_par_minute || 0) > 0;

  useEffect(() => {
    if (!flowing || REDUCED) return undefined;
    let frame;
    const tick = () => {
      setPhase((p) => (p + 0.004) % 1);
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [flowing]);

  const pipeRef = useCanvas((c) => drawPipeline(c, layers, flowing, phase),
    [layers, flowing, phase]);
  const rateRef = useCanvas((c) => drawThroughput(c, live.history), [live.history]);

  // Le producteur republie tout l'instantane horaire a chaque cycle : sans
  // regroupement la liste montre quatre seismes en boucle. Le compteur de
  // republications porte plus d'information que la repetition elle-meme.
  const grouped = useMemo(() => {
    const byId = new Map();
    const order = [];
    for (const message of kafka.evenements || []) {
      const seen = byId.get(message.id);
      if (seen) {
        seen.republications += 1;
        if (message.kind === "revise" && seen.kind !== "revise") {
          seen.kind = "revise";
          seen.previous_magnitude = message.previous_magnitude;
        }
        continue;
      }
      const entry = { ...message, republications: 1 };
      byId.set(message.id, entry);
      order.push(entry);
    }
    // Chaque seisme est passe dans les 80 arbres exportes du lac. Le tri se
    // fait sur la suspicion, pas sur l'ordre d'arrivee : un operateur de
    // permanence veut voir en haut ce dont il doit se mefier.
    for (const entry of order) entry.verdict = scoreLive(entry);
    return order.sort((a, b) => {
      const sa = a.verdict && a.verdict.inDomain ? a.verdict.suspicion : -1;
      const sb = b.verdict && b.verdict.inDomain ? b.verdict.suspicion : -1;
      if (sb !== sa) return sb - sa;
      const rank = { revise: 0, nouveau: 1, repete: 2 };
      return (rank[a.kind] ?? 2) - (rank[b.kind] ?? 2);
    });
  }, [kafka.evenements]);

  const triage = useMemo(() => {
    const scored = grouped.filter((e) => e.verdict);
    const inDomain = scored.filter((e) => e.verdict.inDomain);
    return {
      scored: scored.length,
      inDomain: inDomain.length,
      suspects: inDomain.filter((e) => e.verdict.suspect).length,
      lead: inDomain[0] || scored[0] || null,
    };
  }, [grouped]);

  const badge = html`
    <span class=${"pill " + (live.connected ? "on" : "off")}>
      <i class="dot"></i>${live.connected ? "en direct" : "hors ligne"}
    </span>`;

  return html`
    <div class="view">
      <${Head} title="Le pipeline, en direct">
        Ce que l'USGS publie entre dans Kafka, traverse Spark et se depose dans
        le lac. Les compteurs sont lus sur la pile elle-meme, pas rejoues.
      <//>

      <div class="stats stagger">
        <${Stat} count=${kafka.seismes_distincts} label="seismes dans la fenetre" />
        <${Stat} count=${triage.scored} label="scores par le modele" tone="settled" />
        <${Stat} count=${triage.suspects}
          label=${`suspects, sur ${triage.inDomain} dans le domaine M≥4`} tone="prov" />
        <${Stat} count=${kafka.revisions_captees} label="revisions captees en direct" tone="prov" />
      </div>

      ${triage.lead && triage.lead.verdict && html`
        <${TriageHero} event=${triage.lead} verdict=${triage.lead.verdict} />`}

      <div class="live-grid">
        <${Panel} title="USGS → Kafka → Spark → HDFS" aside=${badge}>
          <div class="pipe-wrap"><canvas id="pipeline" ref=${pipeRef}></canvas></div>
          <div class="throughput">
            <div class="eyebrow" style=${{ marginBottom: ".4rem" }}>
              cadence, messages par minute
            </div>
            <canvas id="throughput" ref=${rateRef}></canvas>
          </div>
        <//>

        <${Panel} title="Messages Kafka" className="feed">
          <div class="feed-list">
            ${grouped.length ? grouped.slice(0, 22).map((m) => html`
              <div class=${"msg scored " + (m.verdict ? m.verdict.etat : "inconnu")}
                key=${m.id}>
                <span class="tag">
                  ${m.kind === "repete" ? "×" + m.republications : (m.kind || "?")}
                </span>
                <span class="where">
                  ${m.place || m.id}
                  <i class="risk">
                    <i class="risk-fill" style=${{
                      width: (m.verdict && m.verdict.inDomain
                        ? Math.max(3, m.verdict.suspicion) : 0) + "%" }}></i>
                  </i>
                </span>
                <span class="mag">
                  M${fmt(m.magnitude, 1)}${m.kind === "revise"
                    ? " ← " + fmt(m.previous_magnitude, 1) : ""}
                </span>
              </div>`)
              : html`<div class="absent">${live.connected
                  ? "en attente du prochain cycle du producteur"
                  : "service live injoignable"}</div>`}
          </div>
        <//>
      </div>

      <div class="live-grid" style=${{ marginTop: "1.2rem" }}>
        <${Panel} title="Le lac, couche par couche">
          <div class="layers">
            ${Object.entries(LAYER_NAMES).map(([key, label]) => html`
              <div class="layer" key=${key}>
                <span class="nm">${label}</span>
                <span class="fc">${int((layers[key] || {}).fichiers)}</span>
                <span class="by">${weight((layers[key] || {}).octets)}</span>
              </div>`)}
          </div>
        <//>

        <${Panel} title="Cluster Spark">
          ${stack.spark ? html`
            <div class="gauges">
              <${Gauge} label="coeurs" used=${stack.spark.coeurs_utilises}
                total=${stack.spark.coeurs}
                text=${`${stack.spark.coeurs_utilises}/${stack.spark.coeurs}`} />
              <${Gauge} label="memoire" used=${stack.spark.memoire_utilisee_mo}
                total=${stack.spark.memoire_mo}
                text=${`${Math.round(stack.spark.memoire_utilisee_mo / 102.4) / 10} Go`} />
              ${(stack.spark.applications || []).map((app) => html`
                <div class="gauge" key=${app.nom}>
                  <span>${app.nom}</span>
                  <span style=${{ fontSize: ".74rem", color: "var(--muted)" }}>
                    ${app.coeurs} coeurs · ${app.memoire_mo} Mo
                  </span>
                  <span class="v">${Math.round(app.secondes / 60)} min</span>
                </div>`)}
            </div>`
            : html`<div class="absent">cluster injoignable</div>`}
        <//>
      </div>

      <div style=${{ marginTop: "1.2rem", display: "grid", gap: ".9rem" }}>
        <${Note} warn>
          <strong>Le flux horaire sort du domaine d'entrainement.</strong> Le
          modele a appris sur le catalogue M≥${fmt(TRAINING_MIN_MAGNITUDE, 1)} ;
          la plupart des seismes qui arrivent ici sont bien plus faibles. Ils
          sont scores quand meme, et marques <em>hors domaine</em> : un verdict
          affiche avec le meme aplomb que les autres serait un mensonge.
          ${" "}Quatre variables ne sont pas publiees par le flux
          (${IMPUTED_AT_INFERENCE.join(", ")}) : le moteur leur substitue la
          mediane du jeu d'entrainement, comme le fait le pipeline Spark.
        <//>
        <${Note}>
          <strong>Hors de la pile, cette vue est vide.</strong> Elle interroge le
          service embarque dans Docker. Ouverte depuis un fichier publie, elle
          n'a personne a interroger et le dit, plutot que d'inventer un trafic.
        <//>
      </div>
    </div>`;
}

/* ─────────── vue 2 : le globe ─────────── */

function mountGlobe(canvas, events) {
  if (typeof THREE === "undefined") return undefined;

  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 2));

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(38, 1, 0.1, 100);
  camera.position.set(0, 0, 3.1);

  const world = new THREE.Group();
  world.rotation.x = 0.32;
  scene.add(world);

  // Un globe sans continents n'est pas une Terre, c'est un grillage. La sphere
  // pleine et les cotes reelles sont ce qui rend la geographie lisible : sans
  // elles, "les revisions se concentrent sur les dorsales" est invérifiable.
  const ocean = new THREE.Mesh(
    new THREE.SphereGeometry(0.995, 64, 64),
    new THREE.MeshBasicMaterial({
      color: new THREE.Color("#071A24") }));
  world.add(ocean);

  const halo = new THREE.Mesh(
    new THREE.SphereGeometry(1.06, 48, 48),
    new THREE.MeshBasicMaterial({
      color: new THREE.Color("#3ADCE4"),
      transparent: true, opacity: 0.14, side: THREE.BackSide,
    }));
  world.add(halo);

  const toVector = (lat, lon, radius) => {
    const phi = (90 - lat) * Math.PI / 180;
    const theta = (lon + 180) * Math.PI / 180;
    return new THREE.Vector3(
      -radius * Math.sin(phi) * Math.cos(theta),
      radius * Math.cos(phi),
      radius * Math.sin(phi) * Math.sin(theta));
  };

  const graticule = new THREE.LineSegments(
    new THREE.WireframeGeometry(new THREE.SphereGeometry(0.998, 24, 12)),
    new THREE.LineBasicMaterial({
      color: new THREE.Color("#13323D"),
      transparent: true, opacity: 0.5,
    }));
  world.add(graticule);

  if (Array.isArray(LAND)) {
    const coast = new THREE.LineBasicMaterial({
      color: new THREE.Color("#8FBECB"),
      transparent: true, opacity: 0.95,
    });
    for (const ring of LAND) {
      const points = ring.map(([lon, lat]) => toVector(lat, lon, 1.002));
      if (points.length > 2) points.push(points[0]);
      world.add(new THREE.Line(
        new THREE.BufferGeometry().setFromPoints(points), coast));
    }
  }

  const upColor = new THREE.Color("#FF6B3D");
  const downColor = new THREE.Color("#3ADCE4");

  for (const event of events) {
    const shift = event.shift || 0;
    const height = 0.04 + Math.min(Math.abs(shift), 1.6) * 0.34;
    const base = toVector(event.first_latitude, event.first_longitude, 1.005);
    const tip = toVector(event.first_latitude, event.first_longitude, 1.005 + height);
    world.add(new THREE.Line(
      new THREE.BufferGeometry().setFromPoints([base, tip]),
      new THREE.LineBasicMaterial({ color: shift >= 0 ? upColor : downColor })));
  }

  let dragging = false, previous = null, running = true;

  const onDown = (e) => {
    dragging = true; previous = { x: e.clientX, y: e.clientY };
    try { canvas.setPointerCapture(e.pointerId); } catch (error) { /* non capturable */ }
  };
  const onUp = (e) => {
    dragging = false;
    try { canvas.releasePointerCapture(e.pointerId); } catch (error) { /* deja relache */ }
  };
  const onMove = (e) => {
    if (!dragging || !previous) return;
    world.rotation.y += (e.clientX - previous.x) * 0.006;
    world.rotation.x = Math.max(-1.2, Math.min(1.2,
      world.rotation.x + (e.clientY - previous.y) * 0.004));
    previous = { x: e.clientX, y: e.clientY };
  };
  const onWheel = (e) => {
    e.preventDefault();
    camera.position.z = Math.max(1.6, Math.min(5,
      camera.position.z + e.deltaY * 0.002));
  };

  canvas.addEventListener("pointerdown", onDown);
  canvas.addEventListener("pointerup", onUp);
  canvas.addEventListener("pointermove", onMove);
  canvas.addEventListener("wheel", onWheel, { passive: false });

  const tick = () => {
    if (!running) return;
    const width = canvas.clientWidth, height = canvas.clientHeight;
    renderer.setSize(width, height, false);
    camera.aspect = width / Math.max(height, 1);
    camera.updateProjectionMatrix();
    if (!dragging && !REDUCED) world.rotation.y += 0.0009;
    renderer.render(scene, camera);
    requestAnimationFrame(tick);
  };
  tick();

  return () => {
    running = false;
    canvas.removeEventListener("pointerdown", onDown);
    canvas.removeEventListener("pointerup", onUp);
    canvas.removeEventListener("pointermove", onMove);
    canvas.removeEventListener("wheel", onWheel);
    renderer.dispose();
  };
}

function ViewGlobe() {
  const ref = useRef(null);

  const placed = useMemo(() => EVENTS.filter((e) =>
    Number.isFinite(e.first_latitude) && Number.isFinite(e.first_longitude)), []);

  useEffect(() => {
    if (!ref.current) return undefined;
    return mountGlobe(ref.current, placed);
  }, [placed]);

  const up = placed.filter((e) => (e.shift || 0) > 0).length;
  const down = placed.filter((e) => (e.shift || 0) < 0).length;

  return html`
    <div class="view">
      <${Head} title="Ou la Terre se corrige">
        Chaque tige part de l'epicentre reel. Sa hauteur donne l'ampleur de la
        revision ; sa couleur dit si la magnitude a ete relevee ou abaissee.
        Glissez pour tourner, molette pour zoomer.
      <//>

      <${Panel}>
        <div class="globe-wrap">
          <canvas id="globe" ref=${ref}></canvas>
          <div class="globe-read">
            <span>epicentres places : <b>${placed.length}</b></span>
            <span>revisions a la hausse : <b>${up}</b></span>
            <span>a la baisse : <b>${down}</b></span>
          </div>
        </div>
      <//>

      <div style=${{ marginTop: "1.2rem" }}>
        <${Note}>
          <strong>Les revisions ne sont pas reparties au hasard.</strong> Les
          dorsales oceaniques et les arcs insulaires, loin de toute station,
          concentrent les plus gros ecarts. C'est la meme cause que le gap
          azimutal que le modele juge determinant.
        <//>
      </div>
    </div>`;
}

/* ─────────── vue 3 : le dossier ─────────── */

const SCALES = [
  ["mww", 10], ["mwr", 10], ["mwc", 10], ["mwb", 10], ["mw", 10],
  ["ms_vx", 8], ["ms", 8], ["mb_lg", 6.5], ["mb", 6.5],
  ["mlr", 6.5], ["ml", 6.5], ["md", 5], ["mh", 5],
];

function saturationOf(type) {
  const token = (type || "").toLowerCase();
  for (const [prefix, ceiling] of SCALES) {
    if (token.startsWith(prefix)) return ceiling < 9 ? ceiling : null;
  }
  return null;
}

function Ruler({ event }) {
  const [slid, setSlid] = useState(false);
  useEffect(() => {
    setSlid(false);
    const id = setTimeout(() => setSlid(true), 90);
    return () => clearTimeout(id);
  }, [event.event_id]);

  const ceiling = saturationOf(event.first_magnitude_type);
  const low = Math.floor(Math.min(event.first_magnitude, event.final_magnitude) - 0.6);
  const high = Math.ceil(Math.max(event.first_magnitude, event.final_magnitude,
    ceiling || 0) + 0.6);
  const at = (m) => ((m - low) / (high - low || 1)) * 100;

  const ticks = [];
  for (let m = low; m <= high; m += 0.5) ticks.push(m);

  return html`
    <div class="ruler">
      <div class="ruler-track">
        <div class="ruler-line"></div>
        ${ticks.map((m) => html`
          <div class="tick" style=${{ left: at(m) + "%" }} key=${m}>
            ${Math.abs(m - Math.round(m)) < 0.01 && html`<span>${m}</span>`}
          </div>`)}
        ${ceiling && ceiling >= low && ceiling <= high && html`
          <div class="ceiling" style=${{ left: at(ceiling) + "%" }}>
            <span>saturation ${event.first_magnitude_type} · ${ceiling}</span>
          </div>`}
        <div class="marker prov" style=${{ left: at(event.first_magnitude) + "%" }}>
          <div class="val">M${fmt(event.first_magnitude)}</div>
          <div class="stem"></div>
          <div class="tag">${event.first_magnitude_type || "auto"}</div>
        </div>
        <div class="marker settled" style=${{
          left: at(slid ? event.final_magnitude : event.first_magnitude) + "%" }}>
          <div class="stem"></div>
          <div class="val">M${fmt(event.final_magnitude)}</div>
          <div class="tag">${event.final_magnitude_type || "revise"}</div>
        </div>
      </div>
    </div>`;
}

function ViewDossier() {
  const [query, setQuery] = useState("");
  const [chosen, setChosen] = useState(EVENTS[0] ? EVENTS[0].event_id : null);

  const matches = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return EVENTS.filter((e) =>
      !needle || (e.place || "").toLowerCase().includes(needle));
  }, [query]);

  const event = matches.find((e) => e.event_id === chosen) || matches[0];
  const hops = event ? (TIMELINES[event.event_id] || []) : [];

  return html`
    <div class="view">
      <${Head} title="Le dossier d'un seisme">
        Chaque seisme est publie plusieurs fois. Voici la chronologie complete de
        ses versions : qui a publie, avec combien de stations, et comment la
        magnitude s'est deplacee.
      <//>

      <div class="dossier">
        <aside class="panel finder">
          <div style=${{ padding: ".7rem" }}>
            <input type="search" placeholder="chercher un lieu…"
              aria-label="Chercher un seisme par lieu"
              value=${query} onInput=${(e) => setQuery(e.target.value)} />
          </div>
          <div class="finder-list">
            ${matches.length ? matches.slice(0, 200).map((e) => html`
              <button class="finder-item" key=${e.event_id}
                aria-current=${String(e.event_id === (event && event.event_id))}
                onClick=${() => setChosen(e.event_id)}>
                <div class="place">${e.place || e.event_id}</div>
                <div class="meta">
                  M${fmt(e.first_magnitude)} → M${fmt(e.final_magnitude)}
                  ${" "}${e.shift > 0 ? "+" : ""}${fmt(e.shift)}
                </div>
              </button>`)
              : html`<div class="absent">aucun seisme ne correspond</div>`}
          </div>
        </aside>

        <div class="panel">
          ${event ? html`
            <div class="panel-head">
              <h3>${event.place || event.event_id}</h3>
              <span class="eyebrow">
                ${int(event.version_count)} versions · ${event.event_id}
              </span>
            </div>
            <${Ruler} event=${event} />
            <div class="timeline">
              ${hops.length ? hops.map((hop, i) => html`
                <div key=${i} class=${"hop" + (i === 0 ? " first" : "")
                  + (i === hops.length - 1 ? " last" : "")}
                  style=${{ animationDelay: Math.min(i * 0.04, 0.4) + "s" }}>
                  <div class="rank">${hop.version_rank}</div>
                  <div class="who">
                    <b>${hop.contributor || "?"}</b> · <em>
                      ${int(hop.station_count)} stations${hop.azimuthal_gap
                        ? `, gap ${fmt(hop.azimuthal_gap, 0)}°` : ""}${
                        hop.minutes_since_quake !== null
                        && hop.minutes_since_quake !== undefined
                          ? ` · ${fmt(hop.minutes_since_quake, 0)} min apres` : ""}
                    </em>
                  </div>
                  <div class="mag">M${fmt(hop.magnitude)}
                    <small>${hop.magnitude_type || "?"} · ${hop.review_status || "?"}</small>
                  </div>
                </div>`)
                : html`<div class="absent">chronologie non recoltee pour ce seisme</div>`}
            </div>`
            : html`<div class="absent">aucun seisme selectionne</div>`}
        </div>
      </div>
    </div>`;
}

/* ─────────── vue 4 : le seuil ─────────── */

function drawCurve(canvas, index) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  if (!CURVE.length) return;

  const pad = { l: 42, r: 18, t: 18, b: 28 };
  const x = (i) => pad.l + (i / (CURVE.length - 1 || 1)) * (width - pad.l - pad.r);
  const y = (v) => height - pad.b - (v / 100) * (height - pad.t - pad.b);

  ctx.strokeStyle = css("--hair");
  ctx.lineWidth = 1;
  ctx.font = "500 9px 'IBM Plex Mono',monospace";
  for (let v = 0; v <= 100; v += 25) {
    ctx.beginPath();
    ctx.moveTo(pad.l, y(v)); ctx.lineTo(width - pad.r, y(v));
    ctx.stroke();
    ctx.fillStyle = css("--muted");
    ctx.fillText(v + "%", 8, y(v) + 3);
  }

  for (const [key, token] of [["taux_fausse_alerte_pct", "--prov"],
                              ["taux_manque_pct", "--settled"]]) {
    ctx.strokeStyle = css(token);
    ctx.lineWidth = 2;
    ctx.shadowBlur = 6;
    ctx.shadowColor = css(token);
    ctx.beginPath();
    let started = false;
    CURVE.forEach((row, i) => {
      const value = row[key];
      if (value === null || value === undefined) return;
      if (started) ctx.lineTo(x(i), y(value));
      else { ctx.moveTo(x(i), y(value)); started = true; }
    });
    ctx.stroke();
    ctx.shadowBlur = 0;
  }

  ctx.strokeStyle = css("--ink-2");
  ctx.globalAlpha = 0.5;
  ctx.setLineDash([3, 3]);
  ctx.beginPath();
  ctx.moveTo(x(index), pad.t); ctx.lineTo(x(index), height - pad.b);
  ctx.stroke();
  ctx.setLineDash([]); ctx.globalAlpha = 1;

  ctx.fillStyle = css("--prov");
  ctx.fillText("fausses alertes", pad.l + 8, pad.t + 9);
  ctx.fillStyle = css("--settled");
  ctx.fillText("alertes manquees", pad.l + 8, pad.t + 21);
}

function Bar({ label, value, worst, tone }) {
  return html`
    <div class="bar-row">
      <span>${label}</span>
      <div class="bar-track">
        <div class=${"bar-fill " + tone}
          style=${{ width: Math.min(100, (value / (worst || 1)) * 100) + "%" }}></div>
      </div>
      <span class="n">${int(value)}</span>
    </div>`;
}

function ViewSeuil() {
  const start = Math.max(0,
    CURVE.findIndex((r) => Math.abs(r.threshold - 4.5) < 0.001));
  const [index, setIndex] = useState(start);
  const curveRef = useCanvas((c) => drawCurve(c, index), [index]);

  if (!CURVE.length) {
    return html`<div class="view"><div class="panel">
      <div class="absent">courbe non calculee : lancer gold-alert-curve</div>
    </div></div>`;
  }

  const row = CURVE[index];
  const worst = Math.max(row.alertes_emises, row.meritaient_alerte, 1);

  return html`
    <div class="view">
      <${Head} title="Ou placer le seuil d'alerte">
        Une alerte se declenche sur la magnitude annoncee. C'est la magnitude
        revisee qui dit si elle etait meritee. Deplacez le seuil : vous echangez
        des fausses alertes contre des alertes manquees.
      <//>

      <div class="dial">
        <${Panel} className="slider-box">
          <div class="eyebrow">seuil de declenchement</div>
          <div class="slider-val">M ${fmt(row.threshold, 1)}</div>
          <input type="range" min="0" max=${CURVE.length - 1} value=${index}
            aria-label="Seuil de magnitude"
            onInput=${(e) => setIndex(Number(e.target.value))} />
          <div class="scale-ends">
            <span>M ${fmt(CURVE[0].threshold, 1)}</span>
            <span>M ${fmt(CURVE[CURVE.length - 1].threshold, 1)}</span>
          </div>
          <div class="balance">
            <${Bar} label="alertes emises" value=${row.alertes_emises}
              worst=${worst} tone="prov" />
            <${Bar} label="dont injustifiees" value=${row.fausse_alerte}
              worst=${worst} tone="prov" />
            <${Bar} label="meritaient l'alerte" value=${row.meritaient_alerte}
              worst=${worst} tone="settled" />
            <${Bar} label="dont manquees" value=${row.alerte_manquee}
              worst=${worst} tone="settled" />
          </div>
        <//>

        <${Panel} title="Le prix de l'erreur">
          <div class="verdict">
            <${Line} label="taux de fausse alerte"
              value=${pct(row.taux_fausse_alerte_pct, 2)} />
            <${Line} label="taux d'alerte manquee"
              value=${pct(row.taux_manque_pct, 2)} />
            <${Line} label="seismes evalues" value=${int(row.seismes)} />
          </div>
        <//>
      </div>

      <div style=${{ marginTop: "1.2rem" }}>
        <${Panel} className="curve-box">
          <canvas id="curve" ref=${curveRef}></canvas>
        <//>
      </div>
    </div>`;
}

/* ─────────── vue 5 : le modele ─────────── */

const KNOBS = [
  { key: "first_magnitude", label: "magnitude annoncee", min: 4, max: 8, step: 0.1, init: 5, digits: 1 },
  { key: "first_station_count", label: "stations", min: 5, max: 500, step: 1, init: 40, digits: 0 },
  { key: "first_azimuthal_gap", label: "gap azimutal", min: 10, max: 340, step: 1, init: 120, digits: 0, unit: "°" },
  { key: "first_minimum_distance", label: "station la plus proche", min: 0, max: 30, step: 0.1, init: 3, digits: 1, unit: "°" },
  { key: "first_minutes_since_quake", label: "delai de publication", min: 1, max: 90, step: 1, init: 18, digits: 0, unit: " min" },
  { key: "first_depth_km", label: "profondeur", min: 0, max: 600, step: 1, init: 35, digits: 0, unit: " km" },
];

const FAMILY_OF = {
  mb: "body_wave", mww: "moment", mwr: "moment",
  ml: "local", md: "duration", ms: "surface_wave",
};

function drawVotes(canvas, votes) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  if (!votes || !votes.length) return;

  const sorted = [...votes].sort((a, b) => a - b);
  const span = Math.max(Math.abs(sorted[0]),
    Math.abs(sorted[sorted.length - 1]), 0.01);
  const mid = height / 2;
  const barWidth = width / sorted.length;

  ctx.strokeStyle = css("--hair");
  ctx.beginPath(); ctx.moveTo(0, mid); ctx.lineTo(width, mid); ctx.stroke();

  sorted.forEach((vote, i) => {
    const rise = (vote / span) * (mid - 6);
    ctx.fillStyle = vote > 0 ? css("--prov") : css("--settled");
    ctx.globalAlpha = 0.9;
    ctx.fillRect(i * barWidth, rise > 0 ? mid - rise : mid,
      Math.max(barWidth - 0.6, 0.8), Math.abs(rise));
  });
  ctx.globalAlpha = 1;
}

function ViewModele() {
  const [values, setValues] = useState(() => {
    const base = { first_magnitude_type: "mb", first_review_status: "reviewed",
      first_contributor: "us", first_family: "body_wave" };
    for (const knob of KNOBS) base[knob.key] = knob.init;
    return base;
  });

  const outcome = useMemo(() => (ENGINE ? ENGINE.score(values) : null), [values]);
  const votesRef = useCanvas((c) => drawVotes(c, outcome && outcome.votes), [outcome]);
  const check = useMemo(verifyEngine, []);

  if (!ENGINE) {
    return html`<div class="view"><div class="panel">
      <div class="absent">Modele non exporte : lancer src.ml.export_model.</div>
    </div></div>`;
  }

  const chance = outcome.probabilite * 100;
  const suspect = outcome.classe === 1;
  const set = (key, value) => setValues((prev) => ({ ...prev, [key]: value }));

  return html`
    <div class="view">
      <${Head} title="Le modele, en main">
        Ce n'est pas une maquette : les ${FOREST.foret.arbres.length} arbres
        entraines sur le lac ont ete exportes et tournent dans cette page.
        Bougez les curseurs, il repond a la question qui compte — faut-il se
        mefier de ce chiffre.
      <//>

      <div class="lab">
        <${Panel} title="Ce que la station a mesure"
          aside=${html`<span class="eyebrow">connu a l'instant de l'alerte</span>`}>
          <div class="knobs">
            ${KNOBS.map((knob) => html`
              <div class="knob" key=${knob.key}>
                <div class="knob-top">
                  <span>${knob.label}</span>
                  <b>${fmt(values[knob.key], knob.digits)}${knob.unit || ""}</b>
                </div>
                <input type="range" min=${knob.min} max=${knob.max} step=${knob.step}
                  value=${values[knob.key]} aria-label=${knob.label}
                  onInput=${(e) => set(knob.key, Number(e.target.value))} />
              </div>`)}
            <div class="knob">
              <div class="knob-top"><span>echelle employee</span></div>
              <select aria-label="echelle de magnitude"
                value=${values.first_magnitude_type}
                onChange=${(e) => setValues((prev) => ({ ...prev,
                  first_magnitude_type: e.target.value,
                  first_family: FAMILY_OF[e.target.value] || "inconnue" }))}>
                ${["mb", "mww", "mwr", "ml", "md", "ms"].map((t) =>
                  html`<option value=${t} key=${t}>${t}</option>`)}
              </select>
            </div>
          </div>
        <//>

        <${Panel} title="Ce que le modele en dit"
          aside=${html`<span class="pill on"><i class="dot"></i>
            ${FOREST.foret.arbres.length} arbres</span>`}>
          <div class="verdict-box">
            <div class=${"verdict-word " + (suspect ? "suspect" : "stable")}>
              ${suspect ? "Mefiez-vous de ce chiffre" : "Le chiffre devrait tenir"}
            </div>
            <div class="verdict-score">
              indice de suspicion ${fmt(chance, 1)} % · score brut ${fmt(outcome.brut, 3)}
            </div>
            <div class="score-track">
              <div class="score-fill" style=${{
                width: Math.min(100, Math.max(2, chance)) + "%",
                background: suspect ? "var(--prov)" : "var(--settled)",
                boxShadow: `0 0 14px ${suspect ? "var(--prov-glow)" : "var(--settled-glow)"}`,
              }}></div>
            </div>
          </div>
          <div class="votes">
            <div class="eyebrow" style=${{ marginBottom: ".35rem" }}>
              vote de chaque arbre, du plus rassurant au plus alarmant
            </div>
            <canvas id="votes" ref=${votesRef}></canvas>
          </div>
        <//>
      </div>

      ${check && html`
        <div style=${{ marginTop: "1.2rem" }}>
          <${Note} warn=${check.ecart_max !== 0}>
            <strong>${check.ecart_max === 0
              ? `Portage verifie sur ${check.total} cas. `
              : `Portage divergent sur ${check.total - check.accord} cas. `}</strong>
            ${check.ecart_max === 0
              ? `Le moteur JavaScript de cette page a rejoue l'inference sur ${check.total} seismes du jeu de test et a retrouve exactement la decision de Spark. Sans ce controle, un portage silencieusement faux passerait pour le modele.`
              : "Le moteur de cette page ne reproduit pas Spark sur tous les cas temoins : le resultat affiche ci-dessus n'est pas fiable."}
          <//>
        </div>`}

      ${MODEL && MODEL.regression && html`<${Evaluation} />`}
      <${Witness} />
    </div>`;
}

function Evaluation() {
  const reg = MODEL.regression;
  const cls = MODEL.classification || {};
  const zero = reg.references && reg.references.toujours_zero;
  const ranked = reg.importances || [];
  const top = (ranked[0] && ranked[0].poids) || 1;

  return html`
    <div>
      <h3 style=${{ margin: "1.8rem 0 .9rem", fontSize: "1.1rem" }}>
        Ce que vaut ce modele, mesure
      </h3>

      <div class="stats stagger">
        <${Stat} count=${MODEL.taille_apprentissage} label="exemples d'apprentissage" />
        <${Stat} count=${MODEL.taille_test} label="exemples de test, tous posterieurs" />
        <${Stat} count=${cls.aire_sous_roc} digits=${3}
          label="aire sous ROC, 0,5 = hasard" tone="settled" />
        <${Stat} count=${reg.accord_de_signe && reg.accord_de_signe.signe_correct_pct}
          digits=${1} suffix=" %"
          label="sens de la revision correctement predit" />
      </div>

      <${Panel} title="Question ecartee — de combien la magnitude va bouger"
        aside=${html`<span class="pill off">echouee</span>`}>
        <div class="verdict">
          <${Line} label="erreur absolue moyenne du modele" value=${fmt(reg.modele.mae, 4)} />
          <${Line} label="erreur si l'on suppose zero revision"
            value=${fmt(zero && zero.mae, 4)} />
          <${Line} label="gain sur la reference" value=${pct(reg.gain_sur_reference_pct, 1)} />
        </div>
      <//>

      <div style=${{ margin: "1.1rem 0 1.4rem" }}>
        <${Note} warn>
          <strong>Predire l'ampleur echoue, et nous le publions.</strong> La
          majorite des seismes ne bougent pas : supposer zero revision est une
          strategie forte qu'un modele de regression ne bat pas. C'est pourquoi
          la question posee au modele est binaire — se mefier ou non — et non
          numerique.
        <//>
      </div>

      <div class="lab">
        <${Panel} title="Sur quoi il se fonde"
          aside=${html`<span class="eyebrow">poids relatif</span>`}>
          <div class="weights">
            ${ranked.slice(0, 8).map((row) => html`
              <div class="weight" key=${row.variable}>
                <span>${row.variable.replace(/^first_/, "").replace(/_/g, " ")}</span>
                <div class="wt">
                  <div class="wf" style=${{ width: (row.poids / top) * 100 + "%" }}></div>
                </div>
                <span class="wv">${fmt(row.poids * 100, 1)}</span>
              </div>`)}
          </div>
        <//>

        ${COHORTS.length > 1 && html`
          <${Panel} title="Biais de selection, mesure"
            aside=${html`<span class="eyebrow">tracee contre temoin</span>`}>
            <div class="scroller">
              <table>
                <thead><tr>
                  <th>cohorte</th><th>seismes</th><th>% deplaces</th><th>ecart moyen</th>
                </tr></thead>
                <tbody>
                  ${COHORTS.map((row) => html`
                    <tr key=${row.cohort}>
                      <td>${row.cohort === "traced" ? "tracee" : "temoin"}</td>
                      <td class="n">${int(row.seismes)}</td>
                      <td class="n">${pct(row.pct_deplace)}</td>
                      <td class="n">${fmt(row.ecart_moyen, 3)}</td>
                    </tr>`)}
                </tbody>
              </table>
            </div>
          <//>`}
      </div>

      <div style=${{ marginTop: "1.2rem" }}>
        <${Note}>
          <strong>La coupure est temporelle, jamais aleatoire.</strong> Le modele
          apprend sur le passe et est evalue sur des seismes posterieurs, comme
          il le serait en service.
        <//>
      </div>
    </div>`;
}

function Witness() {
  if (!WITNESS || !WITNESS.par_temoignage) return null;

  const label = (v) => v === true ? "au moins 5 temoins"
    : v === false ? "moins de 5 temoins" : "champ absent";

  return html`
    <div>
      <h3 style=${{ margin: "1.8rem 0 .6rem", fontSize: "1.1rem" }}>
        Le troisieme temoin : les humains et la machine
      </h3>
      <div style=${{ marginBottom: "1rem" }}>
        <${Note}>
          L'USGS collecte les temoignages du public. Une intuition naturelle veut
          que, si des gens ont senti un seisme plus fort que la mesure ne
          l'indiquait, la magnitude ait ete sous-estimee.
        <//>
      </div>

      <div class="lab">
        <${Panel} title="La revision suit-elle le ressenti ?"
          aside=${html`<span class="eyebrow">
            ${int(WITNESS.avec_temoins)} ressentis sur ${int(WITNESS.seismes)}
          </span>`}>
          <div class="scroller">
            <table>
              <thead><tr>
                <th>temoignages</th><th>seismes</th><th>hausse</th>
                <th>baisse</th><th>ecart absolu</th>
              </tr></thead>
              <tbody>
                ${WITNESS.par_temoignage.map((row, i) => html`
                  <tr key=${i}>
                    <td>${label(row.has_witness)}</td>
                    <td class="n">${int(row.seismes)}</td>
                    <td class="n">${pct(row.pct_hausse)}</td>
                    <td class="n up">${pct(row.pct_baisse)}</td>
                    <td class="n">${fmt(row.ecart_absolu_moyen, 3)}</td>
                  </tr>`)}
              </tbody>
            </table>
          </div>
        <//>

        ${WITNESS.par_ressenti && WITNESS.par_ressenti.length > 0 && html`
          <${Panel} title="Quand le ressenti depasse la mesure"
            aside=${html`<span class="pill off">hypothese refutee</span>`}>
            <div class="scroller">
              <table>
                <thead><tr>
                  <th>senti plus fort</th><th>seismes</th>
                  <th>hausse</th><th>ecart moyen</th>
                </tr></thead>
                <tbody>
                  ${WITNESS.par_ressenti.map((row, i) => html`
                    <tr key=${i}>
                      <td>${row.witness_louder ? "oui" : "non"}</td>
                      <td class="n">${int(row.seismes)}</td>
                      <td class="n">${pct(row.pct_hausse)}</td>
                      <td class="n">${fmt(row.ecart_moyen, 3)}</td>
                    </tr>`)}
                </tbody>
              </table>
            </div>
          <//>`}
      </div>

      <div style=${{ marginTop: "1.2rem" }}>
        <${Note} warn>
          <strong>L'intuition est fausse ici.</strong> Quand le ressenti humain
          depasse la mesure, la magnitude est relevee ensuite MOINS souvent, pas
          plus. L'ecart reste faible sur une centaine de seismes par groupe :
          hypothese plausible refutee, pas effet etabli. Le constat solide est
          ailleurs — un seisme ressenti est revise a la baisse pres de trois fois
          plus souvent, parce qu'il est proche des zones peuplees donc annonce
          par un reseau regional en ml, que le moment global corrige ensuite.
        <//>
      </div>
    </div>`;
}

/* ─────────── sismogramme de marque ─────────── */

function BrandPulse() {
  const ref = useCanvas((canvas) => {
    const { ctx, width, height } = fitCanvas(canvas);
    ctx.clearRect(0, 0, width, height);
    const mid = height / 2;
    ctx.strokeStyle = css("--prov");
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    for (let x = 0; x <= width; x += 1) {
      const t = x / width;
      const burst = Math.exp(-Math.pow((t - 0.42) * 7, 2));
      const noise = Math.sin(x * 0.7) * 0.35 + Math.sin(x * 1.9) * 0.25;
      ctx.lineTo(x, mid - noise * burst * (height * 0.44)
        - Math.sin(x * 0.22) * 1.1);
    }
    ctx.stroke();
  }, []);
  return html`<canvas class="pulse-line" ref=${ref}></canvas>`;
}

/* ─────────── assemblage ─────────── */

const VIEWS = [
  { id: "direct", label: "Le direct", hint: "le pipeline qui tourne", Component: ViewDirect },
  { id: "globe", label: "Le globe", hint: "ou la Terre se corrige", Component: ViewGlobe },
  { id: "dossier", label: "Le dossier", hint: "l'histoire d'un seisme", Component: ViewDossier },
  { id: "seuil", label: "Le seuil", hint: "le prix de l'erreur", Component: ViewSeuil },
  { id: "modele", label: "Le modele", hint: "predire la revision", Component: ViewModele },
];

function App() {
  const [active, setActive] = useState("direct");

  if (!DATA) {
    return html`<div class="shell"><main class="stage"><div class="panel">
      <div class="absent">
        Aucune donnee embarquee. Lancer scripts/build_site.py apres le pipeline.
      </div>
    </div></main></div>`;
  }

  const current = VIEWS.find((v) => v.id === active) || VIEWS[0];

  return html`
    <div class="shell">
      <nav class="rail">
        <div class="brand">
          <div class="wordmark"><b>AFTER</b><span>SHOCK</span></div>
          <${BrandPulse} />
          <p class="tagline">
            La memoire des alertes sismiques : ce que la Terre a d'abord
            annonce, face a ce qu'elle a fini par reveler.
          </p>
        </div>

        <div class="views" role="tablist" aria-label="Vues">
          ${VIEWS.map((view, i) => html`
            <button class="view-btn" role="tab" key=${view.id}
              aria-current=${String(view.id === active)}
              aria-label=${`${view.label} — ${view.hint}`}
              onClick=${() => setActive(view.id)}>
              <span class="idx">${String(i + 1).padStart(2, "0")}</span>
              <span class="label">${view.label}</span>
              <span class="hint">${view.hint}</span>
            </button>`)}
        </div>

        <div class="rail-foot">
          <div class="legend">
            <div><i class="swatch prov"></i> premiere annonce, automatique</div>
            <div><i class="swatch settled"></i> valeur revisee, humaine</div>
          </div>
          <div class="stamp">
            <span class="eyebrow">lac genere le</span>
            <span>${(DATA.genere_le || "").slice(0, 16).replace("T", " ")} UTC</span>
          </div>
        </div>
      </nav>

      <main class="stage">
        <${current.Component} key=${active} />
      </main>
    </div>`;
}

ReactDOM.createRoot(document.getElementById("root")).render(html`<${App} />`);
