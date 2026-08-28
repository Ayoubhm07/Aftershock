// Rend CHAQUE vue de la page avec le moteur React serveur et signale toute
// erreur.
//
// `node --check` ne valide que la syntaxe : un appel a une fonction inexistante
// passe, et la vue meurt en silence dans le navigateur. C'est exactement ce qui
// est arrive a la vue "Le modele" -- appendWitness() appelee, jamais definie.
//
//   node scripts/smoke_views.js [chemin/page.html]

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const PAGE = process.argv[2] || path.join("site", "aftershock.html");
const APP = path.join("site", "app.js");
const VENDOR = path.join("site", "vendor");

const LIBRARIES = ["react.min.js", "react-dom-server.min.js", "htm.min.js"];

function injected(html, name) {
  const marker = `const ${name} = `;
  const start = html.indexOf(marker);
  if (start < 0) throw new Error(`${name} absent de la page`);
  const from = start + marker.length;
  const raw = html.slice(from, html.indexOf(";\n", from)).trim()
    .replace(/^\/\*__[A-Z_]+__\*\//, "")
    .replace(/\/\*__[A-Z_]+_END__\*\/$/, "")
    .trim();
  return raw === "null" ? null : JSON.parse(raw);
}

/* --- juste ce que la page touche hors React --- */

const CANVAS_METHODS = [
  "clearRect", "beginPath", "moveTo", "lineTo", "closePath", "fill", "stroke",
  "arc", "fillRect", "fillText", "setTransform", "setLineDash", "save",
  "restore", "translate", "rotate", "scale",
];

function canvasContext() {
  const ctx = {};
  for (const name of CANVAS_METHODS) ctx[name] = () => {};
  return ctx;
}

function makeNode(tag) {
  return {
    tagName: String(tag).toUpperCase(),
    style: {}, attributes: {}, clientWidth: 800, clientHeight: 300,
    setAttribute(k, v) { this.attributes[k] = String(v); },
    getAttribute(k) { return this.attributes[k] ?? null; },
    hasAttribute(k) { return k in this.attributes; },
    addEventListener() {}, removeEventListener() {},
    getContext: () => canvasContext(),
  };
}

const sandbox = {
  console, Math, JSON, Date, Number, String, Boolean, Array, Object,
  Map, Set, RegExp, Error, TypeError, isNaN, parseFloat, parseInt,
  Symbol, Promise, WeakMap, ArrayBuffer, Uint8Array, Float64Array,
  devicePixelRatio: 1,
  requestAnimationFrame: () => 0,
  cancelAnimationFrame: () => {},
  setTimeout: () => 0,
  clearTimeout: () => {},
  queueMicrotask: (fn) => fn(),
  matchMedia: () => ({ matches: false, addEventListener() {} }),
  getComputedStyle: () => ({ getPropertyValue: () => "#000000" }),
  EventSource: function () { return { close() {} }; },
  addEventListener: () => {},
  removeEventListener: () => {},
  document: {
    documentElement: makeNode("html"),
    createElement: (tag) => makeNode(tag),
    getElementById: () => makeNode("div"),
    querySelector: () => makeNode("div"),
  },
  process: { env: { NODE_ENV: "production" } },
  // Le moteur serveur de React encode ses flux : sans TextEncoder, le module
  // ne se charge meme pas.
  TextEncoder, TextDecoder,
};
sandbox.window = sandbox;
sandbox.self = sandbox;
sandbox.globalThis = sandbox;

const context = vm.createContext(sandbox);

for (const name of LIBRARIES) {
  const file = path.join(VENDOR, name);
  if (!fs.existsSync(file)) {
    console.error(`ECHEC : ${name} absent de ${VENDOR}`);
    process.exit(1);
  }
  vm.runInContext(fs.readFileSync(file, "utf8"), context, { filename: name });
}

// La page appelle ReactDOM.createRoot ; le moteur serveur n'expose que
// renderToString. Un createRoot inerte suffit pour que le module se charge,
// puis chaque vue est rendue ici.
vm.runInContext(
  "var ReactDOM = { createRoot: function () { return { render: function () {} }; } };",
  context);

const page = fs.readFileSync(PAGE, "utf8");
sandbox.DATA = injected(page, "DATA");
sandbox.FOREST = injected(page, "FOREST");
sandbox.LAND = injected(page, "LAND");

try {
  vm.runInContext(fs.readFileSync(APP, "utf8"), context, { filename: "app.js" });
} catch (error) {
  console.error(`ECHEC au chargement de app.js : ${error.message}`);
  process.exit(1);
}

let views = [];
try {
  views = vm.runInContext("typeof VIEWS !== 'undefined' ? VIEWS : []", context);
} catch (error) {
  console.error(`ECHEC a la lecture de VIEWS : ${error.message}`);
  process.exit(1);
}
if (!views.length) {
  console.error("ECHEC : aucune vue declaree");
  process.exit(1);
}

console.log("-".repeat(62));
let broken = 0;

for (const view of views) {
  const id = JSON.stringify(view.id);
  try {
    const markup = vm.runInContext(
      `ReactDOMServer.renderToString(React.createElement(`
      + `VIEWS.find(function (v) { return v.id === ${id}; }).Component))`,
      context);
    if (!markup || markup.length < 40) throw new Error("vue rendue vide");
    console.log(`  ${view.id.padEnd(10)} OK      ${markup.length} caracteres`);
  } catch (error) {
    broken += 1;
    console.log(`  ${view.id.padEnd(10)} ECHEC   ${error.message}`);
  }
}

try {
  const shell = vm.runInContext(
    "ReactDOMServer.renderToString(React.createElement(App))", context);
  console.log(`  ${"App".padEnd(10)} OK      ${shell.length} caracteres`);
} catch (error) {
  broken += 1;
  console.log(`  ${"App".padEnd(10)} ECHEC   ${error.message}`);
}

console.log("-".repeat(62));

if (broken) {
  console.error(`  ${broken} rendu(s) en erreur.`);
  process.exit(1);
}
console.log(`  ${views.length} vues + la coquille rendues sans erreur.`);
