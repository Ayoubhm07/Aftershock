"""Remet le carnet au niveau des tables produites depuis sa redaction.

Le carnet lisait `revision_history` (2 lignes, issues du seul flux accumule)
et `alert_reliability`, dont l'echantillon etait trop maigre pour conclure.
La troisieme source USGS a produit `magnitude_revision` (2 128 seismes) et
`alert_curve` (41 seuils) : la question 3, laissee ouverte, est desormais
mesurable, et deux sections s'ajoutent.
"""
from __future__ import annotations

import json
import pathlib
import sys

NOTEBOOK = pathlib.Path("notebooks/insights.ipynb")


def cell(kind: str, text: str) -> dict:
    lines = text.strip("\n").split("\n")
    source = [line + "\n" for line in lines[:-1]] + [lines[-1]]
    base = {"cell_type": kind, "metadata": {}, "source": source}
    if kind == "code":
        base["execution_count"] = None
        base["outputs"] = []
    return base


LOAD = '''
revisions = load("revision_history", fs)
identity = load("identity_history", fs)
scales = load("magnitude_scales", fs)
lag = load("review_lag", fs)

pairs = load("magnitude_revision", fs)
curve = load("alert_curve", fs)
witness = load("human_witness", fs)

report = json.loads(fs.read(f"{GOLD_ROOT}/model_report/report.json"))

for name, frame in [("revision_history", revisions), ("identity_history", identity),
                    ("magnitude_scales", scales), ("review_lag", lag),
                    ("magnitude_revision", pairs), ("alert_curve", curve),
                    ("human_witness", witness)]:
    print(f"{name:<22} {len(frame):>7} lignes")
'''

SECTION3_TEXT = '''
---
## 3. A quel seuil d'alerte, quel taux de fausse alerte ?

C'est la question operationnelle : un service de protection civile doit choisir
un seuil de magnitude annoncee au-dela duquel il declenche.

**Cette question etait laissee ouverte dans la version precedente de ce carnet**,
faute d'echantillon : la table `alert_reliability` ne portait que deux seismes
observes a la fois en version automatique et en version revisee.

La recolte de l'historique des versions apporte **2 128 paires**. La table
`alert_curve` croise chaque seuil candidat avec elles : une alerte est emise si
la magnitude *annoncee* depasse le seuil, elle est meritee si la magnitude
*finale* le depasse aussi.
'''

SECTION3_CODE = '''
display(curve[curve.threshold.isin([4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0])][
    ["threshold", "alertes_emises", "fausse_alerte", "taux_fausse_alerte_pct",
     "alerte_manquee", "taux_manque_pct"]])

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(curve.threshold, curve.taux_fausse_alerte_pct,
        color="#C2410C", lw=2, label="fausses alertes")
ax.plot(curve.threshold, curve.taux_manque_pct,
        color="#0E7C86", lw=2, label="alertes manquees")
ax.set_xlabel("seuil de magnitude annoncee")
ax.set_ylabel("taux (%)")
ax.set_title(f"Le prix de l'erreur, sur {len(pairs)} seismes apparies",
             fontweight="bold", loc="left")
ax.legend(); ax.grid(alpha=0.3)
fig.tight_layout()
'''

SECTION3_NOTE = '''
> **Lecture.** Le taux de fausse alerte **monte avec le seuil** : a M4,0 il est
> de 0,05 %, a M6,5 il atteint 22,9 %. Ce n'est pas un paradoxe. Les seuils
> hauts se situent la ou `mb` sature et ou les echelles divergent le plus ;
> c'est exactement le piege documente en section 4.
>
> Le point de croisement se situe vers **M5,0**, ou les deux taux valent environ
> 3,6 % et 3,7 %. Un service qui refuse par-dessus tout de manquer un seisme
> descendra plus bas et acceptera de declencher pour rien.
'''

SECTION5_TEXT = '''
---
## 5. Le modele : ce qu'il predit, et ce qu'il ne predit pas

Les variables connues **a l'instant de l'alerte** — nombre de stations, trou de
couverture azimutal, echelle employee, erreur d'ajustement — suffisent-elles a
anticiper la revision ?

Deux questions ont ete posees au meme jeu de donnees. **Une seule recoit une
reponse utile, et nous publions les deux.**
'''

SECTION5_CODE = '''
reg = report["regression"]
cls = report["classification"]

print(f"coupure temporelle : {report['coupure_temporelle']}")
print(f"apprentissage {report['taille_apprentissage']} | test {report['taille_test']}")
print()
print("QUESTION 1 — de combien la magnitude va-t-elle bouger")
print(f"   MAE du modele           : {reg['modele']['mae']:.4f}")
print(f"   MAE si l'on suppose 0   : {reg['references']['toujours_zero']['mae']:.4f}")
print(f"   bat la reference        : {reg['bat_la_reference']}")
print()
print("QUESTION 2 — faut-il se mefier de ce chiffre")
print(f"   aire sous ROC           : {cls['aire_sous_roc']:.4f}   (0,5 = hasard)")
print(f"   part de cas positifs    : {cls['reference']['part_positive_test_pct']} %")
print()
print("variables les plus explicatives :")
for row in reg["importances"][:6]:
    print(f"   {row['variable']:<34} {row['poids']:.4f}")
'''

SECTION5_NOTE = '''
> **La regression echoue, et c'est un resultat.** La majorite des seismes ne
> bougent pas : supposer zero revision est une strategie forte qu'un modele
> d'ampleur ne bat pas. L'erreur moyenne recompense le silence.
>
> **La classification, elle, informe** : une aire sous ROC de 0,83 signifie que
> le modele sait ordonner les seismes du plus suspect au moins suspect. Sa
> precision au seuil par defaut reste faible parce que 3 % seulement des cas
> sont positifs — c'est un probleme de calibration, pas de pouvoir predictif.
>
> **Une variable a ete auditee.** Le delai de premiere publication portait 57 %
> du poids du modele. Une importance elevee ne prouve pas une dependance : nous
> avons donc reentraine sans elle (`EXCLUDED_FEATURES`) pour verifier que le
> modele ne repose pas sur un artefact d'archivage de l'USGS.
'''

SECTION6_TEXT = '''
---
## 6. Le troisieme temoin : les humains sont-ils d'accord avec la machine ?

L'USGS collecte les temoignages du public (`felt`, `cdi`). Une intuition
naturelle veut que, si des gens ont senti un seisme plus fort que ne l'indiquait
la mesure, la magnitude ait ete sous-estimee.

**Cette intuition est fausse dans nos donnees.**
'''

SECTION6_CODE = '''
heard = witness[witness.has_witness == True]

print(f"seismes apparies             : {len(witness)}")
print(f"dont >= 5 temoignages humains: {len(heard)}  ({100*len(heard)/len(witness):.1f} %)")
print()

groups = witness.groupby(witness.has_witness.fillna("champ absent")).agg(
    seismes=("event_id", "count"),
    pct_hausse=("moved_up", lambda s: round(100 * s.mean(), 1)),
    pct_baisse=("moved_down", lambda s: round(100 * s.mean(), 1)),
    ecart_absolu_moyen=("shift_abs", lambda s: round(s.mean(), 3)))
display(groups)

comparable = heard[heard.intensity_gap.notna()]
louder = comparable.groupby("witness_louder").agg(
    seismes=("event_id", "count"),
    pct_hausse=("moved_up", lambda s: round(100 * s.mean(), 1)),
    ecart_moyen=("shift", lambda s: round(s.mean(), 3)),
    ecart_intensite=("intensity_gap", lambda s: round(s.mean(), 2)))
display(louder)
'''

SECTION6_NOTE = '''
> **Ce que la mesure dit.** Un seisme ressenti par au moins cinq personnes est
> revise a la baisse presque **trois fois plus souvent** qu'un seisme non
> ressenti. L'explication tient au piege des echelles : les seismes ressentis
> sont proches des zones peuplees, donc annonces par des reseaux regionaux en
> `ml` ou `md`, que le moment global corrige ensuite vers le bas.
>
> **Ce que la mesure ne dit pas.** Quand le ressenti humain depasse la mesure
> instrumentale, la magnitude n'est pas relevee ensuite — elle baisse un peu
> plus. L'ecart est faible (0,044 de magnitude) sur une centaine de seismes par
> groupe : **nous le presentons comme une hypothese plausible refutee, pas comme
> un effet etabli.**
'''

CLOSING = '''
---
## Ce que ces donnees permettent, et ce qu'elles ne permettent pas

**Etabli, sur 17 526 seismes du catalogue et 2 128 historiques complets :**

- la premiere solution archivee parait **17,8 minutes** apres le seisme en
  mediane ; la fiche se stabilise **75 jours** plus tard ;
- **34,5 %** des seismes voient leur magnitude bouger entre deux versions ;
- 6,6 % portent la trace d'une solution automatique remplacee, et **tous** ont
  vu leur identifiant changer — une jointure naive les compterait deux fois ;
- le seuil d'alerte est desormais **chiffre** : de 0,05 % de fausses alertes a
  M4,0 jusqu'a 22,9 % a M6,5 ;
- un modele peut dire **s'il faut se mefier** d'un chiffre (ROC 0,83), pas
  **de combien** il se trompera.

**Ce qui reste hors de portee.**

L'echantillon de 2 128 seismes est une fraction du catalogue : la recolte
appelle l'API une fois par seisme et prend pres de deux heures. L'architecture
ne change pas d'une ligne pour en traiter dix fois plus — c'est un choix de
temps de calcul, pas une limite de conception.

La cohorte temoin a permis de **mesurer** le biais de selection plutot que de
l'ignorer : les seismes ayant conserve un identifiant `usauto` portent 4,48
versions en moyenne contre 2,89 pour un tirage aleatoire. Un modele entraine
sur la seule cohorte tracee aurait appris sur une population particuliere sans
que personne ne le sache.
'''


def main() -> int:
    if not NOTEBOOK.exists():
        print(f"carnet introuvable : {NOTEBOOK}", file=sys.stderr)
        return 1

    book = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    cells = book["cells"]

    header = "".join(cells[1]["source"])
    if "GOLD_ROOT" not in header:
        cells[1]["source"] = [
            line for line in cells[1]["source"]
        ]
        cells[1]["source"].insert(2, "import json\n")
        cells[1]["source"].insert(
            7, "from src.common.config import GOLD_ROOT\n")

    cells[2] = cell("code", LOAD)

    cells[10] = cell("markdown", SECTION3_TEXT)
    cells[11] = cell("code", SECTION3_CODE)
    cells.insert(12, cell("markdown", SECTION3_NOTE))

    closing = len(cells) - 1
    while closing > 0 and cells[closing]["cell_type"] != "markdown":
        closing -= 1

    cells[closing:] = [
        cell("markdown", SECTION5_TEXT),
        cell("code", SECTION5_CODE),
        cell("markdown", SECTION5_NOTE),
        cell("markdown", SECTION6_TEXT),
        cell("code", SECTION6_CODE),
        cell("markdown", SECTION6_NOTE),
        cell("markdown", CLOSING),
    ]

    NOTEBOOK.write_text(json.dumps(book, ensure_ascii=False, indent=1),
                        encoding="utf-8")
    print(f"carnet recale : {len(cells)} cellules")
    return 0


if __name__ == "__main__":
    sys.exit(main())
