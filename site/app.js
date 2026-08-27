"use strict";

const REDUCED = matchMedia("(prefers-reduced-motion: reduce)").matches;

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

function el(tag, attrs = {}, ...kids) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === false || value === null || value === undefined) continue;
    if (key === "class") node.className = value;
    else if (key === "html") node.innerHTML = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const kid of kids.flat()) {
    if (kid === null || kid === undefined || kid === false) continue;
    node.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return node;
}

function fitCanvas(canvas) {
  const ratio = Math.min(devicePixelRatio || 1, 2);
  const width = canvas.clientWidth, height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  return { ctx, width, height };
}

const OVERVIEW = (DATA && DATA.overview) || {};
const EVENTS = (DATA && DATA.seismes) || [];
const TIMELINES = (DATA && DATA.chronologies) || {};
const CURVE = (DATA && DATA.courbe_alerte) || [];
const MODEL = DATA && DATA.modele;
const COHORTS = (DATA && DATA.cohortes) || [];
const WITNESS = DATA && DATA.temoin_humain;

/* ════════ le direct ════════ */

const LIVE = {
  connected: false,
  source: null,
  kafka: null,
  stack: null,
  history: [],
  listeners: new Set(),
};

function liveConnect() {
  if (LIVE.source) return;
  try {
    LIVE.source = new EventSource("/api/live");
  } catch (error) {
    LIVE.connected = false;
    return;
  }
  LIVE.source.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data);
      LIVE.connected = true;
      LIVE.kafka = payload.kafka;
      LIVE.stack = payload.pile;
      const rate = payload.kafka && payload.kafka.cadence_par_minute;
      LIVE.history.push(rate || 0);
      if (LIVE.history.length > 90) LIVE.history.shift();
      for (const listener of LIVE.listeners) listener();
    } catch (error) { /* trame incomplete, on attend la suivante */ }
  };
  LIVE.source.onerror = () => {
    LIVE.connected = false;
    for (const listener of LIVE.listeners) listener();
  };
}

function viewDirect() {
  const root = el("div");

  root.append(el("div", { class: "view-head" },
    el("h2", {}, "Le pipeline, en direct"),
    el("p", {}, "Ce que l'USGS publie arrive dans Kafka, traverse Spark et se "
      + "depose dans le lac. Les compteurs ci-dessous sont lus sur la pile "
      + "elle-meme, pas rejoues.")));

  const badge = el("span", { class: "pill off" },
    el("i", { class: "dot" }), "hors ligne");

  const stats = el("div", { class: "stats" });
  const pipeline = el("canvas", { id: "pipeline" });
  const throughput = el("canvas", { id: "throughput" });
  const feed = el("div", { class: "feed-list" });
  const gauges = el("div", { class: "gauges" });
  const layers = el("div", { class: "layers" });

  const left = el("section", { class: "panel" },
    el("div", { class: "panel-head" },
      el("h3", {}, "USGS → Kafka → Spark → HDFS"), badge),
    el("div", { class: "pipe-wrap" }, pipeline),
    el("div", { class: "throughput" },
      el("div", { class: "eyebrow", style: "margin-bottom:.35rem" },
        "cadence, messages par minute"),
      throughput));

  const right = el("section", { class: "panel feed" },
    el("div", { class: "panel-head" }, el("h3", {}, "Messages Kafka")),
    feed);

  root.append(stats, el("div", { class: "live-grid" }, left, right));

  root.append(el("div", { class: "live-grid", style: "margin-top:1.1rem" },
    el("section", { class: "panel" },
      el("div", { class: "panel-head" }, el("h3", {}, "Le lac, couche par couche")),
      layers),
    el("section", { class: "panel" },
      el("div", { class: "panel-head" }, el("h3", {}, "Cluster Spark")),
      gauges)));

  root.append(el("p", { class: "note", style: "margin-top:1.1rem" },
    el("strong", {}, "Hors de la pile, cette vue est vide. "),
    "Elle interroge le service embarque dans Docker. Ouverte depuis un fichier "
    + "publie, elle n'a personne a interroger et le dit, plutot que d'inventer "
    + "un trafic."));

  const paint = () => {
    const kafka = LIVE.kafka || {};
    const stack = LIVE.stack || {};

    badge.className = "pill " + (LIVE.connected ? "on" : "off");
    badge.replaceChildren(el("i", { class: "dot" }),
      LIVE.connected ? "en direct" : "hors ligne");

    stats.replaceChildren(
      statTile(int(kafka.messages_total), "messages consommes"),
      statTile(int(kafka.seismes_distincts), "seismes distincts dans la fenetre"),
      statTile(fmt(kafka.cadence_par_minute, 1), "messages par minute", "settled"),
      statTile(int(kafka.revisions_captees), "revisions captees en direct", "prov"));

    // Le producteur republie tout l'instantane horaire a chaque cycle : sans
    // regroupement, la liste n'affiche que quatre seismes repetes en boucle.
    // Le compteur de republications est plus informatif que la repetition.
    const events = kafka.evenements || [];
    const grouped = [];
    const byId = new Map();
    for (const message of events) {
      const seen = byId.get(message.id);
      if (seen) {
        seen.republications += 1;
        if (message.kind === "revise" && seen.kind !== "revise") {
          seen.kind = "revise";
          seen.previous_magnitude = message.previous_magnitude;
        }
        continue;
      }
      const entry = Object.assign({}, message, { republications: 1 });
      byId.set(message.id, entry);
      grouped.push(entry);
    }

    grouped.sort((a, b) => {
      const rank = { revise: 0, nouveau: 1, repete: 2 };
      return (rank[a.kind] ?? 2) - (rank[b.kind] ?? 2);
    });

    feed.replaceChildren(...grouped.slice(0, 22).map((message) =>
      el("div", { class: "msg " + (message.kind || "repete") },
        el("span", { class: "tag" },
          message.kind === "repete"
            ? "×" + message.republications
            : message.kind || "?"),
        el("span", { class: "where" }, message.place || message.id),
        el("span", { class: "mag" },
          "M" + fmt(message.magnitude, 1)
          + (message.kind === "revise"
            ? " ← " + fmt(message.previous_magnitude, 1) : "")))));

    if (!events.length) {
      feed.replaceChildren(el("div", { class: "absent" },
        LIVE.connected ? "en attente du prochain cycle du producteur"
          : "service live injoignable"));
    }

    const names = {
      bronze_flux: "Bronze · flux temps reel",
      bronze_catalogue: "Bronze · catalogue",
      bronze_versions: "Bronze · historique des versions",
      silver: "Silver · Delta",
      gold: "Gold · Parquet",
    };
    const couches = stack.couches || {};
    layers.replaceChildren(...Object.entries(names).map(([key, label]) =>
      el("div", { class: "layer" },
        el("span", { class: "nm" }, label),
        el("span", { class: "fc" }, int((couches[key] || {}).fichiers)),
        el("span", { class: "by" }, weight((couches[key] || {}).octets)))));

    const spark = stack.spark;
    if (spark) {
      gauges.replaceChildren(
        gaugeRow("coeurs", spark.coeurs_utilises, spark.coeurs,
          `${spark.coeurs_utilises}/${spark.coeurs}`),
        gaugeRow("memoire", spark.memoire_utilisee_mo, spark.memoire_mo,
          `${Math.round(spark.memoire_utilisee_mo / 1024 * 10) / 10} Go`),
        ...(spark.applications || []).map((app) =>
          el("div", { class: "gauge" },
            el("span", {}, app.nom),
            el("span", { style: "font-size:.75rem;color:var(--muted)" },
              `${app.coeurs} coeurs · ${app.memoire_mo} Mo`),
            el("span", { class: "v" }, `${Math.round(app.secondes / 60)} min`))));
    } else {
      gauges.replaceChildren(el("div", { class: "absent" }, "cluster injoignable"));
    }

    drawPipeline(pipeline, kafka, couches);
    drawThroughput(throughput);
  };

  LIVE.listeners.add(paint);
  liveConnect();
  queueMicrotask(paint);
  return root;
}

function statTile(value, label, tone) {
  return el("div", { class: "stat" },
    el("div", { class: "k" + (tone ? " " + tone : "") }, value),
    el("div", { class: "t" }, label));
}

function gaugeRow(label, used, total, text) {
  const share = total ? Math.min(100, (used / total) * 100) : 0;
  return el("div", { class: "gauge" },
    el("span", {}, label),
    el("div", { class: "gauge-track" },
      el("div", { class: "gauge-fill", style: `width:${share}%` })),
    el("span", { class: "v" }, text));
}

const PIPE_NODES = [
  { key: null, label: "USGS", sub: "all_hour" },
  { key: null, label: "Kafka", sub: "quakes_live" },
  { key: null, label: "Spark", sub: "streaming" },
  { key: "bronze_flux", label: "Bronze", sub: "brut" },
  { key: "silver", label: "Silver", sub: "Delta" },
  { key: "gold", label: "Gold", sub: "Parquet" },
];

const particles = [];

function drawPipeline(canvas, kafka, layers) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  const midline = height * 0.44;
  const margin = 46;
  const step = (width - margin * 2) / (PIPE_NODES.length - 1);
  const at = (index) => margin + index * step;

  ctx.strokeStyle = css("--line");
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(at(0), midline);
  ctx.lineTo(at(PIPE_NODES.length - 1), midline);
  ctx.stroke();

  const flowing = LIVE.connected && (kafka.cadence_par_minute || 0) > 0;
  if (flowing && !REDUCED && particles.length < 14 && Math.random() < 0.4) {
    particles.push({ t: 0 });
  }

  for (let i = particles.length - 1; i >= 0; i -= 1) {
    particles[i].t += 0.006;
    if (particles[i].t > 1) { particles.splice(i, 1); continue; }
    const x = at(0) + (at(PIPE_NODES.length - 1) - at(0)) * particles[i].t;
    ctx.fillStyle = css("--settled");
    ctx.globalAlpha = Math.sin(particles[i].t * Math.PI) * 0.9;
    ctx.beginPath();
    ctx.arc(x, midline, 2.6, 0, Math.PI * 2);
    ctx.fill();
    ctx.globalAlpha = 1;
  }

  PIPE_NODES.forEach((node, index) => {
    const x = at(index);
    ctx.fillStyle = css("--panel");
    ctx.strokeStyle = css("--line");
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.arc(x, midline, 13, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();

    if (flowing && index <= 3) {
      ctx.strokeStyle = css("--settled");
      ctx.globalAlpha = 0.5;
      ctx.beginPath();
      ctx.arc(x, midline, 17, 0, Math.PI * 2);
      ctx.stroke();
      ctx.globalAlpha = 1;
    }

    ctx.fillStyle = css("--ink");
    ctx.font = "600 11px " + css("--display").split(",")[0].replace(/'/g, "");
    ctx.textAlign = "center";
    ctx.fillText(node.label, x, midline - 26);

    ctx.fillStyle = css("--muted");
    ctx.font = "400 9px monospace";
    ctx.fillText(node.sub, x, midline - 14);

    const layer = node.key && layers[node.key];
    if (layer) {
      ctx.fillStyle = css("--ink-2");
      ctx.font = "500 10px monospace";
      ctx.fillText(`${int(layer.fichiers)} fichiers`, x, midline + 32);
      ctx.fillStyle = css("--muted");
      ctx.font = "400 9px monospace";
      ctx.fillText(weight(layer.octets), x, midline + 44);
    }
  });

  ctx.textAlign = "left";
  if (!REDUCED && flowing) requestAnimationFrame(() => drawPipeline(canvas, kafka, layers));
}

function drawThroughput(canvas) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);
  const series = LIVE.history;
  if (series.length < 2) return;

  const peak = Math.max(...series, 1) * 1.15;
  const x = (i) => (i / (series.length - 1)) * width;
  const y = (v) => height - 4 - (v / peak) * (height - 12);

  ctx.beginPath();
  ctx.moveTo(x(0), height);
  series.forEach((value, index) => ctx.lineTo(x(index), y(value)));
  ctx.lineTo(x(series.length - 1), height);
  ctx.closePath();
  ctx.fillStyle = css("--settled-wash");
  ctx.fill();

  ctx.beginPath();
  series.forEach((value, index) =>
    index ? ctx.lineTo(x(index), y(value)) : ctx.moveTo(x(index), y(value)));
  ctx.strokeStyle = css("--settled");
  ctx.lineWidth = 1.6;
  ctx.stroke();

  ctx.fillStyle = css("--settled");
  ctx.beginPath();
  ctx.arc(x(series.length - 1), y(series[series.length - 1]), 2.8, 0, Math.PI * 2);
  ctx.fill();
}

/* ════════ le globe ════════ */

function viewGlobe() {
  const root = el("div");

  root.append(el("div", { class: "view-head" },
    el("h2", {}, "Ou la Terre se corrige"),
    el("p", {}, "Chaque tige part de l'epicentre reel. Sa hauteur donne la "
      + "magnitude annoncee ; sa couleur dit si la revision l'a relevee ou "
      + "abaissee. Faites tourner le globe.")));

  const canvas = el("canvas", { id: "globe" });
  const readout = el("div", { class: "globe-read" });

  root.append(el("section", { class: "panel" },
    el("div", { class: "globe-wrap" }, canvas, readout)));

  root.append(el("p", { class: "note", style: "margin-top:1.1rem" },
    el("strong", {}, "Les revisions ne sont pas reparties au hasard. "),
    "Les dorsales oceaniques et les arcs insulaires, mal couverts par les "
    + "stations, concentrent les plus gros ecarts. C'est la meme cause que le "
    + "gap azimutal que le modele juge determinant."));

  queueMicrotask(() => mountGlobe(canvas, readout));
  return root;
}

function mountGlobe(canvas, readout) {
  if (typeof THREE === "undefined") {
    readout.textContent = "moteur 3D indisponible";
    return;
  }

  const points = EVENTS.filter((event) =>
    Number.isFinite(event.first_latitude) && Number.isFinite(event.first_longitude));

  const usable = points.length ? points : EVENTS;
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
  renderer.setPixelRatio(Math.min(devicePixelRatio || 1, 2));

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(42, 1, 0.1, 100);
  camera.position.set(0, 0, 3.05);

  const world = new THREE.Group();
  scene.add(world);

  const sphere = new THREE.Mesh(
    new THREE.SphereGeometry(1, 48, 48),
    new THREE.MeshBasicMaterial({
      color: new THREE.Color(css("--sunk")),
      transparent: true, opacity: 0.92,
    }));
  world.add(sphere);

  const grid = new THREE.LineSegments(
    new THREE.WireframeGeometry(new THREE.SphereGeometry(1.002, 24, 16)),
    new THREE.LineBasicMaterial({
      color: new THREE.Color(css("--line")), transparent: true, opacity: 0.5,
    }));
  world.add(grid);

  const upColor = new THREE.Color(css("--provisional"));
  const downColor = new THREE.Color(css("--settled"));
  const spikes = [];

  const toVector = (latitude, longitude, radius) => {
    const phi = (90 - latitude) * Math.PI / 180;
    const theta = (longitude + 180) * Math.PI / 180;
    return new THREE.Vector3(
      -radius * Math.sin(phi) * Math.cos(theta),
      radius * Math.cos(phi),
      radius * Math.sin(phi) * Math.sin(theta));
  };

  for (const event of usable) {
    const latitude = event.first_latitude, longitude = event.first_longitude;
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) continue;

    const height = 0.05 + Math.min(Math.abs(event.shift || 0), 1.6) * 0.42;
    const base = toVector(latitude, longitude, 1);
    const tip = toVector(latitude, longitude, 1 + height);

    const geometry = new THREE.BufferGeometry().setFromPoints([base, tip]);
    const material = new THREE.LineBasicMaterial({
      color: (event.shift || 0) >= 0 ? upColor : downColor,
    });
    const line = new THREE.Line(geometry, material);
    line.userData = event;
    world.add(line);
    spikes.push({ line, event, tip });
  }

  let dragging = false, previous = null;
  let spinX = 0.15, spinY = 0.0006;

  const resize = () => {
    const width = canvas.clientWidth, height = canvas.clientHeight;
    renderer.setSize(width, height, false);
    camera.aspect = width / Math.max(height, 1);
    camera.updateProjectionMatrix();
  };

  canvas.addEventListener("pointerdown", (event) => {
    dragging = true; previous = { x: event.clientX, y: event.clientY };
    canvas.setPointerCapture(event.pointerId);
  });
  canvas.addEventListener("pointerup", (event) => {
    dragging = false;
    try { canvas.releasePointerCapture(event.pointerId); } catch (error) { /* deja relache */ }
  });
  canvas.addEventListener("pointermove", (event) => {
    if (!dragging || !previous) return;
    world.rotation.y += (event.clientX - previous.x) * 0.006;
    world.rotation.x += (event.clientY - previous.y) * 0.004;
    world.rotation.x = Math.max(-1.2, Math.min(1.2, world.rotation.x));
    previous = { x: event.clientX, y: event.clientY };
  });
  canvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    camera.position.z = Math.max(1.7, Math.min(5, camera.position.z + event.deltaY * 0.002));
  }, { passive: false });

  const summary = () => {
    const up = usable.filter((event) => (event.shift || 0) > 0).length;
    const down = usable.filter((event) => (event.shift || 0) < 0).length;
    readout.replaceChildren(
      el("span", {}, "epicentres places : ", el("b", {}, String(usable.length))),
      el("span", {}, "revisions a la hausse : ", el("b", {}, String(up))),
      el("span", {}, "a la baisse : ", el("b", {}, String(down))),
      el("span", {}, "glisser pour tourner, molette pour zoomer"));
  };
  summary();

  const tick = () => {
    resize();
    if (!dragging && !REDUCED) world.rotation.y += spinY;
    world.rotation.x = world.rotation.x || spinX * 0;
    renderer.render(scene, camera);
    requestAnimationFrame(tick);
  };
  world.rotation.x = 0.28;
  tick();
}

/* ════════ le dossier ════════ */

let selected = EVENTS[0] ? EVENTS[0].event_id : null;

function viewDossier() {
  const root = el("div");

  root.append(el("div", { class: "view-head" },
    el("h2", {}, "Le dossier d'un seisme"),
    el("p", {}, "Chaque seisme est publie plusieurs fois. Voici la chronologie "
      + "complete de ses versions : qui a publie, avec combien de stations, et "
      + "comment la magnitude s'est deplacee.")));

  const detail = el("div", { class: "panel", id: "detail" });
  const list = el("div", { class: "finder-list" });

  const search = el("input", {
    type: "search", placeholder: "chercher un lieu…",
    "aria-label": "Chercher un seisme par lieu",
    oninput: (event) => fillList(list, event.target.value, detail),
  });

  const finder = el("aside", { class: "panel finder" },
    el("div", { style: "padding:.65rem" }, search), list);

  fillList(list, "", detail);
  root.append(el("div", { class: "dossier" }, finder, detail));
  return root;
}

function fillList(list, query, detail) {
  const needle = query.trim().toLowerCase();
  const matches = EVENTS.filter((event) =>
    !needle || (event.place || "").toLowerCase().includes(needle));

  list.replaceChildren(...matches.slice(0, 200).map((event) =>
    el("button", {
      class: "finder-item",
      "aria-current": String(event.event_id === selected),
      onclick: () => { selected = event.event_id; fillList(list, query, detail); },
    },
      el("div", { class: "place" }, event.place || event.event_id),
      el("div", { class: "meta" },
        `M${fmt(event.first_magnitude)} → M${fmt(event.final_magnitude)}  `
        + `${event.shift > 0 ? "+" : ""}${fmt(event.shift)}`))));

  if (!matches.length) {
    list.replaceChildren(el("div", { class: "absent" }, "aucun seisme ne correspond"));
  }
  const chosen = matches.find((event) => event.event_id === selected) || matches[0];
  if (chosen) { selected = chosen.event_id; renderDetail(detail, chosen); }
}

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

function renderDetail(host, event) {
  const hops = TIMELINES[event.event_id] || [];
  const ceiling = saturationOf(event.first_magnitude_type);

  const low = Math.floor(Math.min(event.first_magnitude, event.final_magnitude) - 0.6);
  const high = Math.ceil(Math.max(event.first_magnitude, event.final_magnitude,
    ceiling || 0) + 0.6);
  const at = (magnitude) => ((magnitude - low) / (high - low || 1)) * 100;

  const track = el("div", { class: "ruler-track" }, el("div", { class: "ruler-line" }));

  for (let magnitude = low; magnitude <= high; magnitude += 0.5) {
    const tick = el("div", { class: "tick", style: `left:${at(magnitude)}%` });
    if (Math.abs(magnitude - Math.round(magnitude)) < 0.01) {
      tick.append(el("span", {}, String(magnitude)));
    }
    track.append(tick);
  }

  if (ceiling && ceiling >= low && ceiling <= high) {
    track.append(el("div", { class: "ceiling", style: `left:${at(ceiling)}%` },
      el("span", {}, `saturation ${event.first_magnitude_type} · ${ceiling}`)));
  }

  const provisional = el("div", {
    class: "marker prov", style: `left:${at(event.first_magnitude)}%`,
  },
    el("div", { class: "val" }, "M" + fmt(event.first_magnitude)),
    el("div", { class: "stem" }),
    el("div", { class: "tag" }, event.first_magnitude_type || "auto"));

  const settled = el("div", {
    class: "marker settled", style: `left:${at(event.first_magnitude)}%`,
  },
    el("div", { class: "stem" }),
    el("div", { class: "val" }, "M" + fmt(event.final_magnitude)),
    el("div", { class: "tag" }, event.final_magnitude_type || "revise"));

  track.append(provisional, settled);
  requestAnimationFrame(() => {
    settled.style.left = at(event.final_magnitude) + "%";
  });

  const timeline = el("div", { class: "timeline" },
    hops.length
      ? hops.map((hop, index) => el("div", {
        class: "hop" + (index === 0 ? " first" : "")
          + (index === hops.length - 1 ? " last" : ""),
      },
        el("div", { class: "rank" }, String(hop.version_rank)),
        el("div", { class: "who" },
          el("b", {}, hop.contributor || "?"), " · ",
          el("em", {}, `${int(hop.station_count)} stations`
            + (hop.azimuthal_gap ? `, gap ${fmt(hop.azimuthal_gap, 0)}°` : "")
            + (hop.minutes_since_quake !== null && hop.minutes_since_quake !== undefined
              ? ` · ${fmt(hop.minutes_since_quake, 0)} min apres` : ""))),
        el("div", { class: "mag" }, "M" + fmt(hop.magnitude),
          el("small", {}, `${hop.magnitude_type || "?"} · ${hop.review_status || "?"}`))))
      : el("div", { class: "absent" }, "chronologie non recoltee pour ce seisme"));

  host.replaceChildren(
    el("div", { class: "panel-head" },
      el("h3", {}, event.place || event.event_id),
      el("span", { class: "eyebrow" },
        `${int(event.version_count)} versions · ${event.event_id}`)),
    el("div", { class: "ruler" }, track),
    timeline);
}

/* ════════ le seuil ════════ */

function viewSeuil() {
  const root = el("div");

  root.append(el("div", { class: "view-head" },
    el("h2", {}, "Ou placer le seuil d'alerte"),
    el("p", {}, "Une alerte se declenche sur la magnitude annoncee. C'est la "
      + "magnitude revisee qui dit si elle etait meritee. Deplacez le seuil : "
      + "vous echangez des fausses alertes contre des alertes manquees.")));

  if (!CURVE.length) {
    root.append(el("div", { class: "panel" },
      el("div", { class: "absent" }, "courbe non calculee : lancer gold-alert-curve")));
    return root;
  }

  const readout = el("div", { class: "slider-val" });
  const bars = el("div", { class: "balance" });
  const summary = el("div", { class: "verdict" });
  const curve = el("canvas", { id: "curve" });

  const start = CURVE.findIndex((row) => Math.abs(row.threshold - 4.5) < 0.001);
  const slider = el("input", {
    type: "range", min: "0", max: String(CURVE.length - 1),
    value: String(start >= 0 ? start : Math.floor(CURVE.length / 2)),
    "aria-label": "Seuil de magnitude",
    oninput: () => update(),
  });

  function update() {
    const row = CURVE[Number(slider.value)];
    readout.textContent = "M " + fmt(row.threshold, 1);
    const worst = Math.max(row.alertes_emises, row.meritaient_alerte, 1);

    bars.replaceChildren(
      barRow("alertes emises", row.alertes_emises, worst, "prov"),
      barRow("dont injustifiees", row.fausse_alerte, worst, "prov"),
      barRow("meritaient l'alerte", row.meritaient_alerte, worst, "settled"),
      barRow("dont manquees", row.alerte_manquee, worst, "settled"));

    summary.replaceChildren(
      line("taux de fausse alerte", pct(row.taux_fausse_alerte_pct, 2)),
      line("taux d'alerte manquee", pct(row.taux_manque_pct, 2)),
      line("seismes evalues", int(row.seismes)));

    drawCurve(curve, Number(slider.value));
  }

  root.append(el("div", { class: "dial" },
    el("section", { class: "panel slider-box" },
      el("div", { class: "eyebrow" }, "seuil de declenchement"),
      readout, slider,
      el("div", { class: "scale-ends" },
        el("span", {}, "M " + fmt(CURVE[0].threshold, 1)),
        el("span", {}, "M " + fmt(CURVE[CURVE.length - 1].threshold, 1))),
      bars),
    el("section", { class: "panel" },
      el("div", { class: "panel-head" }, el("h3", {}, "Le prix de l'erreur")),
      summary)));

  root.append(el("section", { class: "panel curve-box", style: "margin-top:1.1rem" },
    curve));

  queueMicrotask(update);
  addEventListener("resize", () => drawCurve(curve, Number(slider.value)));
  return root;
}

function barRow(label, value, worst, tone) {
  return el("div", { class: "bar-row" },
    el("span", {}, label),
    el("div", { class: "bar-track" },
      el("div", { class: "bar-fill " + tone,
        style: `width:${Math.min(100, (value / worst) * 100)}%` })),
    el("span", { class: "n" }, int(value)));
}

function line(label, value) {
  return el("div", { class: "line" }, el("span", {}, label), el("span", {}, value));
}

function drawCurve(canvas, index) {
  const { ctx, width, height } = fitCanvas(canvas);
  ctx.clearRect(0, 0, width, height);

  const pad = { l: 40, r: 16, t: 16, b: 26 };
  const x = (i) => pad.l + (i / (CURVE.length - 1 || 1)) * (width - pad.l - pad.r);
  const y = (v) => height - pad.b - (v / 100) * (height - pad.t - pad.b);

  ctx.strokeStyle = css("--hair");
  ctx.lineWidth = 1;
  ctx.font = "500 9px monospace";
  for (let value = 0; value <= 100; value += 25) {
    ctx.beginPath();
    ctx.moveTo(pad.l, y(value)); ctx.lineTo(width - pad.r, y(value));
    ctx.stroke();
    ctx.fillStyle = css("--muted");
    ctx.fillText(value + "%", 8, y(value) + 3);
  }

  for (const [key, token] of [["taux_fausse_alerte_pct", "--provisional"],
                              ["taux_manque_pct", "--settled"]]) {
    ctx.strokeStyle = css(token);
    ctx.lineWidth = 1.8;
    ctx.beginPath();
    let started = false;
    CURVE.forEach((row, i) => {
      const value = row[key];
      if (value === null || value === undefined) return;
      if (started) ctx.lineTo(x(i), y(value));
      else { ctx.moveTo(x(i), y(value)); started = true; }
    });
    ctx.stroke();
  }

  ctx.strokeStyle = css("--ink-2");
  ctx.globalAlpha = 0.45;
  ctx.setLineDash([3, 3]);
  ctx.beginPath();
  ctx.moveTo(x(index), pad.t); ctx.lineTo(x(index), height - pad.b);
  ctx.stroke();
  ctx.setLineDash([]); ctx.globalAlpha = 1;

  ctx.fillStyle = css("--provisional");
  ctx.fillText("fausses alertes", pad.l + 6, pad.t + 8);
  ctx.fillStyle = css("--settled");
  ctx.fillText("alertes manquees", pad.l + 6, pad.t + 20);
  ctx.fillStyle = css("--muted");
  ctx.fillText("M" + fmt(CURVE[0].threshold, 1), pad.l - 6, height - 8);
  ctx.fillText("M" + fmt(CURVE[CURVE.length - 1].threshold, 1),
    width - pad.r - 24, height - 8);
}

/* le laboratoire : le modele tourne ici */

const ENGINE = FOREST ? buildEngine(FOREST) : null;

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

function verifyEngine() {
  if (!ENGINE || !FOREST.temoins || !FOREST.temoins.length) return null;
  let worst = 0;
  let agree = 0;
  for (const witness of FOREST.temoins) {
    const outcome = ENGINE.score(witness);
    const gap = Math.abs(outcome.classe - Number(witness.prediction_spark));
    worst = Math.max(worst, gap);
    if (gap < 0.5) agree += 1;
  }
  return {
    total: FOREST.temoins.length,
    accord: agree,
    ecart_max: worst,
    largeur: ENGINE.largeur,
    largeur_attendue: ENGINE.largeur_attendue,
    coherent: ENGINE.coherent,
    valide: worst === 0 && ENGINE.coherent,
  };
}

const FAMILY_OF = {
  mb: "body_wave", mww: "moment", mwr: "moment",
  ml: "local", md: "duration", ms: "surface_wave",
};

const KNOBS = [
  { key: "first_magnitude", label: "magnitude annoncee", min: 4, max: 8, step: 0.1, value: 5, digits: 1, unit: "" },
  { key: "first_station_count", label: "stations", min: 5, max: 500, step: 1, value: 40, digits: 0, unit: "" },
  { key: "first_azimuthal_gap", label: "gap azimutal", min: 10, max: 340, step: 1, value: 120, digits: 0, unit: "°" },
  { key: "first_minimum_distance", label: "station la plus proche", min: 0, max: 30, step: 0.1, value: 3, digits: 1, unit: "°" },
  { key: "first_minutes_since_quake", label: "delai de publication", min: 1, max: 90, step: 1, value: 18, digits: 0, unit: " min" },
  { key: "first_depth_km", label: "profondeur", min: 0, max: 600, step: 1, value: 35, digits: 0, unit: " km" },
];

function viewModele() {
  const root = el("div");

  root.append(el("div", { class: "view-head" },
    el("h2", {}, "Le modele, en main"),
    el("p", {}, "Ce n'est pas une maquette : les arbres entraines sur le lac ont "
      + "ete exportes et tournent dans cette page. Bougez les curseurs, il "
      + "repond a la question qui compte — faut-il se mefier de ce chiffre.")));

  if (!ENGINE) {
    root.append(el("div", { class: "panel" },
      el("div", { class: "absent" },
        "Modele non exporte : lancer src.ml.export_model.")));
    return root;
  }

  const values = {};
  for (const knob of KNOBS) values[knob.key] = knob.value;
  values.first_magnitude_type = "mb";
  values.first_review_status = "reviewed";
  values.first_contributor = "us";
  values.first_family = "body_wave";

  const verdict = el("div", { class: "verdict-word" });
  const score = el("div", { class: "verdict-score" });
  const fill = el("div", { class: "score-fill" });
  const votes = el("canvas", { id: "votes" });
  const knobs = el("div", { class: "knobs" });

  for (const knob of KNOBS) {
    const shown = el("b", {});
    const slider = el("input", {
      type: "range", min: String(knob.min), max: String(knob.max),
      step: String(knob.step), value: String(knob.value),
      "aria-label": knob.label,
      oninput: (event) => {
        values[knob.key] = Number(event.target.value);
        shown.textContent = fmt(values[knob.key], knob.digits) + knob.unit;
        update();
      },
    });
    shown.textContent = fmt(knob.value, knob.digits) + knob.unit;
    knobs.append(el("div", { class: "knob" },
      el("div", { class: "knob-top" }, el("span", {}, knob.label), shown), slider));
  }

  const scaleSelect = el("select", {
    "aria-label": "echelle de magnitude",
    onchange: (event) => {
      values.first_magnitude_type = event.target.value;
      values.first_family = FAMILY_OF[event.target.value] || "inconnue";
      update();
    },
  }, ["mb", "mww", "mwr", "ml", "md", "ms"].map((token) =>
    el("option", { value: token, selected: token === "mb" }, token)));

  knobs.append(el("div", { class: "knob" },
    el("div", { class: "knob-top" }, el("span", {}, "echelle employee")),
    scaleSelect));

  function update() {
    const outcome = ENGINE.score(values);
    const chance = outcome.probabilite * 100;
    const suspect = outcome.classe === 1;

    verdict.className = "verdict-word " + (suspect ? "suspect" : "stable");
    verdict.textContent = suspect
      ? "Mefiez-vous de ce chiffre"
      : "Le chiffre devrait tenir";
    score.textContent = "indice de suspicion " + fmt(chance, 1) + " %"
      + "  ·  score brut " + fmt(outcome.brut, 3);
    fill.style.width = Math.min(100, Math.max(2, chance)) + "%";
    fill.style.background = suspect ? css("--provisional") : css("--settled");
    drawVotes(votes, outcome.votes);
  }

  root.append(el("div", { class: "lab" },
    el("section", { class: "panel" },
      el("div", { class: "panel-head" },
        el("h3", {}, "Ce que la station a mesure"),
        el("span", { class: "eyebrow" }, "connu a l'instant de l'alerte")),
      knobs),
    el("section", { class: "panel" },
      el("div", { class: "panel-head" },
        el("h3", {}, "Ce que le modele en dit"),
        el("span", { class: "pill on" }, el("i", { class: "dot" }),
          (FOREST.foret.arbres.length) + " arbres")),
      el("div", { class: "verdict-box" },
        verdict, score, el("div", { class: "score-track" }, fill)),
      el("div", { class: "votes" },
        el("div", { class: "eyebrow", style: "margin-bottom:.3rem" },
          "vote de chaque arbre, du plus rassurant au plus alarmant"),
        votes))));

  const check = verifyEngine();
  if (check) {
    let body;
    if (!check.coherent) {
      body = [
        el("strong", {}, "Vecteur incoherent : "
          + check.largeur + " cases construites pour "
          + check.largeur_attendue + " attendues. "),
        "Les indices de variables sont decales et la reponse ci-dessus n'a "
        + "aucune valeur. Ce controle existe precisement parce que l'erreur "
        + "ne leve rien d'elle-meme.",
      ];
    } else if (check.ecart_max !== 0) {
      body = [
        el("strong", {}, "Portage divergent sur "
          + (check.total - check.accord) + " cas. "),
        "Le moteur de cette page ne reproduit pas Spark sur tous les temoins : "
        + "la reponse affichee n'est pas fiable.",
      ];
    } else {
      body = [
        el("strong", {}, "Portage verifie sur " + check.total + " cas. "),
        "Le moteur JavaScript a rejoue l'inference sur " + check.total
        + " seismes du jeu de test et a retrouve exactement la decision de "
        + "Spark, avec un vecteur de " + check.largeur + " variables conforme "
        + "a celui du modele. Sans ce controle, un portage silencieusement "
        + "faux passerait pour le modele.",
      ];
    }
    root.append(el("p", {
      class: "note" + (check.valide ? "" : " warn"), style: "margin-top:1.1rem",
    }, body));
  }

  root.append(el("h3", { style: "margin:1.6rem 0 .8rem;font-size:1.05rem" },
    "Ce que vaut ce modele, mesure"));

  if (MODEL && MODEL.regression) appendEvaluation(root);
  return root;
}

function drawVotes(canvas, votes) {
  const surface = fitCanvas(canvas);
  const ctx = surface.ctx;
  ctx.clearRect(0, 0, surface.width, surface.height);
  if (!votes || !votes.length) return;

  const sorted = votes.slice().sort((a, b) => a - b);
  const span = Math.max(Math.abs(sorted[0]),
    Math.abs(sorted[sorted.length - 1]), 0.01);
  const mid = surface.height / 2;
  const barWidth = surface.width / sorted.length;

  ctx.strokeStyle = css("--hair");
  ctx.beginPath();
  ctx.moveTo(0, mid);
  ctx.lineTo(surface.width, mid);
  ctx.stroke();

  ctx.globalAlpha = 0.85;
  sorted.forEach((vote, index) => {
    const rise = (vote / span) * (mid - 6);
    ctx.fillStyle = vote > 0 ? css("--provisional") : css("--settled");
    ctx.fillRect(index * barWidth, rise > 0 ? mid - rise : mid,
      Math.max(barWidth - 0.6, 0.8), Math.abs(rise));
  });
  ctx.globalAlpha = 1;
}

function appendEvaluation(root) {
  const reg = MODEL.regression;
  const cls = MODEL.classification || {};
  const zero = reg.references && reg.references.toujours_zero;
  const ranked = reg.importances || [];

  root.append(el("div", { class: "stats" },
    statTile(int(MODEL.taille_apprentissage), "exemples d'apprentissage"),
    statTile(int(MODEL.taille_test), "exemples de test, tous posterieurs"),
    statTile(fmt(cls.aire_sous_roc, 3), "aire sous ROC, 0,5 = hasard", "settled"),
    statTile(fmt(reg.accord_de_signe && reg.accord_de_signe.signe_correct_pct, 1) + " %",
      "sens de la revision correctement predit")));

  root.append(el("section", { class: "panel" },
    el("div", { class: "panel-head" },
      el("h3", {}, "Question ecartee — de combien la magnitude va bouger"),
      el("span", { class: "pill off" }, "echouee")),
    el("div", { class: "verdict" },
      line("erreur absolue moyenne du modele", fmt(reg.modele.mae, 4)),
      line("erreur si l'on suppose zero revision", fmt(zero && zero.mae, 4)),
      line("gain sur la reference", pct(reg.gain_sur_reference_pct, 1)))));

  root.append(el("p", { class: "note warn", style: "margin:1rem 0 1.4rem" },
    el("strong", {}, "Predire l'ampleur echoue, et nous le publions. "),
    "La majorite des seismes ne bougent pas : supposer zero revision est une "
    + "strategie forte qu'un modele de regression ne bat pas. C'est pourquoi la "
    + "question posee au modele est binaire — se mefier ou non — et non "
    + "numerique."));

  const weights = el("section", { class: "panel" },
    el("div", { class: "panel-head" },
      el("h3", {}, "Sur quoi il se fonde"),
      el("span", { class: "eyebrow" }, "poids relatif")),
    el("div", { class: "weights" },
      ranked.length ? ranked.slice(0, 8).map((row) => {
        const top = ranked[0].poids || 1;
        return el("div", { class: "weight" },
          el("span", {}, row.variable.replace(/^first_/, "").replace(/_/g, " ")),
          el("div", { class: "wt" },
            el("div", { class: "wf",
              style: "width:" + ((row.poids / top) * 100) + "%" })),
          el("span", { class: "wv" }, fmt(row.poids * 100, 1)));
      }) : el("div", { class: "absent" }, "aucune variable retenue")));

  const cohorts = COHORTS.length > 1
    ? el("section", { class: "panel" },
      el("div", { class: "panel-head" },
        el("h3", {}, "Biais de selection, mesure"),
        el("span", { class: "eyebrow" }, "tracee contre temoin")),
      el("div", { class: "scroller" },
        el("table", {},
          el("thead", {}, el("tr", {},
            el("th", {}, "cohorte"), el("th", {}, "seismes"),
            el("th", {}, "% deplaces"), el("th", {}, "ecart moyen"))),
          el("tbody", {}, COHORTS.map((row) => el("tr", {},
            el("td", {}, row.cohort === "traced" ? "tracee" : "temoin"),
            el("td", { class: "n" }, int(row.seismes)),
            el("td", { class: "n" }, pct(row.pct_deplace)),
            el("td", { class: "n" }, fmt(row.ecart_moyen, 3))))))))
    : null;

  root.append(el("div", { class: "lab" }, weights, cohorts));

  root.append(el("p", { class: "note", style: "margin-top:1.1rem" },
    el("strong", {}, "La coupure est temporelle, jamais aleatoire. "),
    "Le modele apprend sur le passe et est evalue sur des seismes posterieurs, "
    + "comme il le serait en service."));

  appendWitness(root);
}

/* assemblage */

const VIEWS = [
  { id: "direct", label: "Le direct", hint: "le pipeline qui tourne", build: viewDirect },
  { id: "globe", label: "Le globe", hint: "ou la Terre se corrige", build: viewGlobe },
  { id: "dossier", label: "Le dossier", hint: "l'histoire d'un seisme", build: viewDossier },
  { id: "seuil", label: "Le seuil", hint: "le prix de l'erreur", build: viewSeuil },
  { id: "modele", label: "Le modele", hint: "predire la revision", build: viewModele },
];

const stage = document.getElementById("stage");
const tabs = document.querySelector(".views");
let active = null;

VIEWS.forEach((view, index) => {
  tabs.append(el("button", {
    class: "view-btn", role: "tab", "aria-current": "false", id: "tab-" + view.id,
    "aria-label": view.label + " — " + view.hint,
    onclick: () => show(view.id),
  },
    el("span", { class: "idx" }, String(index + 1).padStart(2, "0")),
    el("span", { class: "label" }, view.label),
    el("span", { class: "hint" }, view.hint)));
});

function show(id) {
  if (active === id) return;
  active = id;
  const view = VIEWS.find((entry) => entry.id === id);
  stage.replaceChildren(
    el("div", { class: "view on", role: "tabpanel" }, view.build()));
  for (const button of tabs.children) {
    button.setAttribute("aria-current", String(button.id === "tab-" + id));
  }
}

document.getElementById("theme").addEventListener("click", () => {
  const root = document.documentElement;
  const dark = root.getAttribute("data-theme") === "dark"
    || (!root.hasAttribute("data-theme")
      && matchMedia("(prefers-color-scheme: dark)").matches);
  root.setAttribute("data-theme", dark ? "light" : "dark");
});

if (!DATA) {
  stage.append(el("div", { class: "panel" },
    el("div", { class: "absent" },
      "Aucune donnee embarquee. Lancer scripts/build_site.py apres le pipeline.")));
} else {
  show("direct");
}
