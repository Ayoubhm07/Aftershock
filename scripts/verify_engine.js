// Rejoue l'inference du navigateur hors navigateur, sur les temoins exportes
// avec la prediction de Spark. Un portage silencieusement faux afficherait des
// verdicts credibles et faux : ce controle est la condition pour publier la
// page, pas un supplement.
//
//   node scripts/verify_engine.js [chemin/page.html]

const fs = require("fs");
const path = require("path");

const PAGE = process.argv[2] || path.join("site", "aftershock.html");
const APP = path.join("site", "app.js");

function extractForest(html) {
  const marker = "const FOREST = ";
  const start = html.indexOf(marker);
  if (start < 0) throw new Error("FOREST absent de la page");
  const from = start + marker.length;
  const end = html.indexOf(";\n", from);
  // build_site.py encadre la valeur de marqueurs d'injection : les retirer
  // avant de parser, sinon le JSON est precede d'un commentaire.
  const raw = html.slice(from, end).trim()
    .replace(/^\/\*__MODEL__\*\//, "")
    .replace(/\/\*__MODEL_END__\*\/$/, "")
    .trim();
  if (raw === "null" || raw === "") {
    throw new Error("FOREST vaut null : modele non injecte");
  }
  return JSON.parse(raw);
}

function extractFunction(source, name) {
  const head = `function ${name}(`;
  const start = source.indexOf(head);
  if (start < 0) throw new Error(`${name} introuvable dans app.js`);
  let depth = 0, seen = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (ch === "{") { depth += 1; seen = true; }
    else if (ch === "}") {
      depth -= 1;
      if (seen && depth === 0) return source.slice(start, i + 1);
    }
  }
  throw new Error(`${name} non terminee`);
}

const html = fs.readFileSync(PAGE, "utf8");
const app = fs.readFileSync(APP, "utf8");

const FOREST = extractForest(html);
const buildEngine = eval(`(${extractFunction(app, "buildEngine")})`);
const engine = buildEngine(FOREST);

const witnesses = FOREST.temoins || [];
if (!witnesses.length) {
  console.error("aucun temoin exporte : verification impossible");
  process.exit(1);
}

let agree = 0;
const divergent = [];
let widest = 0;

for (const witness of witnesses) {
  const outcome = engine.score(witness);
  const expected = Number(witness.prediction_spark);
  widest = Math.max(widest, Math.abs(outcome.classe - expected));
  if (outcome.classe === expected) agree += 1;
  else divergent.push({
    id: witness.event_id,
    attendu: expected,
    obtenu: outcome.classe,
    brut: Number(outcome.brut.toFixed(5)),
  });
}

console.log("-".repeat(66));
console.log(`  arbres charges        : ${FOREST.foret.arbres.length}`);
console.log(`  variables assemblees  : ${FOREST.foret.nombre_de_variables}`);
console.log(`  longueur du vecteur   : ${engine.score(witnesses[0]).taille}`);
console.log(`  temoins compares      : ${witnesses.length}`);
console.log(`  decisions identiques  : ${agree}/${witnesses.length}`);
console.log(`  ecart maximal         : ${widest}`);
console.log("-".repeat(66));

if (divergent.length) {
  console.log("  DIVERGENCES :");
  for (const row of divergent.slice(0, 10)) {
    console.log(`    ${row.id}  attendu ${row.attendu}  obtenu ${row.obtenu}`
      + `  score brut ${row.brut}`);
  }
  console.error("\nECHEC : le portage ne reproduit pas Spark.");
  process.exit(1);
}

console.log("  Le moteur de la page reproduit Spark sur tous les temoins.");
process.exit(0);
