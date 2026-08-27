# AFTERSHOCK — le séisme qui rétrécit

Datalake **Bronze → Silver → Gold** sur HDFS, alimenté par **trois** sources
USGS — un flux temps réel via Kafka, un catalogue historique en lots, et
l'historique des versions supersédées — orchestré par Airflow, prolongé par un
modèle MLlib et une application web autonome.

```bash
docker compose up -d
make bronze      # les lots mensuels du catalogue
make versions    # l'historique des versions — long, ne rien lancer en parallèle
make chain       # Silver -> Gold -> modèle -> bundle de restitution
make site        # injecte le bundle dans site/aftershock.html
```

**http://localhost:8889** pour le carnet · `site/aftershock.html` pour
l'application.

---

## Le problème métier

Quand un séisme se produit, l'USGS publie une magnitude calculée
**automatiquement**, à partir des premières stations qui ont enregistré la
secousse. Cette valeur déclenche des alertes tsunami, des évacuations, des
mobilisations de secours.

Des heures — souvent des semaines — plus tard, un sismologue **révise** cette
magnitude avec l'ensemble des enregistrements disponibles. Elle change. Une M3,60
annoncée à Takotna, en Alaska, est devenue **M5,20** ; une M6,30 au large du
Kamtchatka est retombée à **M5,30**.

Ce projet mesure cet écart, explique d'où il vient, et en tire un seuil d'alerte
défendable : à partir de quelle magnitude annoncée faut-il déclencher, en
acceptant quel taux de fausse alerte ?

### Une correction que nous devons au jury

Une version antérieure de ce README affirmait que « personne n'archive l'écart,
parce que chaque version écrase la précédente ». **C'est faux, et la
vérification nous l'a montré.** Le paramètre `includesuperseded=true` de
l'endpoint FDSN ouvre l'historique complet :

| Requête sur `us7000pwpu` | Versions d'origine | Poids |
|---|---|---|
| `query?eventid=X&format=geojson` | 3 | 58 Ko |
| `query?eventid=X&includesuperseded=true` | **10** | **309 Ko** |

Ce qui reste vrai est plus solide : l'archive **n'est consultable qu'un séisme à
la fois, en connaissant son identifiant à l'avance, au prix de 309 Ko**. Elle
n'est ni dans les flux temps réel, ni dans l'endpoint de masse, ni jointe à quoi
que ce soit. Elle est **archivée mais inexploitable**.

C'est ce qu'un lac corrige — et c'est cette troisième source qui rend le modèle
possible.

## Ce que le pipeline établit

Sur **17 526 séismes** du catalogue 2025, **2 128** dont l'historique complet
des versions a été récolté, et un flux temps réel accumulé en continu :

| Constat | Chiffre mesuré |
|---|---|
| Versions successives archivées | **7 939** pour 2 128 séismes |
| Séismes dont la magnitude bouge entre deux versions | **735 (34,5 %)** |
| Écart maximal observé | **+1,60** (M3,60 → M5,20, Takotna, Alaska) |
| Délai médian de la première solution archivée | **17,8 minutes** |
| Délai médian avant stabilisation de la fiche | **75,1 jours** |
| Séismes portant la trace d'une solution automatique remplacée | **1 151 (6,6 %)** |
| … dont l'identifiant retenu a changé depuis | **1 151, soit la totalité** |
| Part de l'échelle `mb`, qui sature à 6,5 | **89 %** des séismes M≥4 |

### Deux délais qui ne mesurent pas la même chose

Une version antérieure de ce README annonçait « 1,6 à 3,5 minutes » pour la
publication d'une solution automatique. Ce chiffre venait de la table
`review_lag`, qui mesure l'écart entre l'instant du séisme et le champ `updated`
des fiches vues dans le flux — donc **la fraîcheur d'une fiche déjà publiée**.

La récolte de l'historique donne une autre mesure, plus exigeante : l'écart
entre le séisme et le **premier produit `origin` que l'USGS a conservé**, soit
**17,8 minutes de médiane** (min 2,9 · max 66,9).

Les deux sont exacts et ne se contredisent pas ; ils ne répondent pas à la même
question. Le second est celui qui compte pour l'alerte, et c'est celui que nous
retenons désormais.

---

## Architecture

```
  ┌ SOURCE 1 ─ all_hour.geojson ──► Kafka ──► Spark Structured Streaming ─┐
  │            (chaque minute)      quakes_live                           │
  │                                                                       ▼
  ├ SOURCE 2 ─ FDSN query ──────────────────────────────►  HDFS /lake/bronze
  │            (lots mensuels, M≥4)                        GeoJSON brut + _SUCCESS
  │                                                                       │
  └ SOURCE 3 ─ FDSN includesuperseded=true ───────────────────────────────┤
               (historique des versions, lots de 100)                     │
                                                                          ▼  Spark + Delta
                                            HDFS /lake/silver/events · event_versions
                                    versions, identités résolues, magnitudes qualifiées
                                                                          │
                                                                          ▼  Spark
                                                              HDFS /lake/gold
              revision_history · identity_history · alert_reliability · magnitude_scales
              review_lag · magnitude_revision · alert_curve · lake_diff · human_witness
                                                                          │
                              ┌───────────────────────────┬───────────────┤
                              ▼                           ▼               ▼
                   Carnet Pandas          Modèle MLlib GBT      Tableau de bord
                  (Parquet direct)      /lake/models/*          temps réel + globe
                                        exporté en JSON             (Docker)
```

**Orchestration.** Quatre DAG, chaînés par **Datasets Airflow** et non par
capteurs : chaque DAG déclare le Dataset qu'il produit, le suivant est planifié
dessus. Une seule impulsion cascade jusqu'au bout.

```
bronze_catalog_ingestion  ──►  silver_events  ──►  gold_insights
                        Dataset            Dataset

versions_pipeline : recolte ─► silver ─► dataset ─► courbe ─► modele ─► export
```

Le DAG `versions_pipeline` est déclenché à la main et non planifié : sa première
tâche appelle l'API USGS une fois par séisme, soit **près de deux heures de
réseau**. Le planifier reviendrait à marteler un service public gratuit.

## Le rejeu, prouvé et non affirmé

L'énoncé souligne cette exigence de deux points d'exclamation. Une phrase dans un
README ne vaut rien ; une manipulation qu'un correcteur rejoue vaut tout.

```bash
make replay
```

Le script fait table rase, déclenche le DAG, **tue Airflow en plein vol**, le
redémarre, relance, puis rejoue une fois de plus sur un lac déjà complet.

| Étape | Marqueurs | Fichiers |
|---|---|---|
| après table rase | 0 | 0 |
| **après arrêt brutal** | **5** | 5 |
| après relance | **12** | 12 |
| après second rejeu | **12** | 12 |

**12 462 374 octets avant et après le rejeu, à l'octet près.**

---

## Les pièges du jeu de données

### 1. L'identité d'un séisme change au fil du temps

C'est le piège central, et il n'est pas celui qu'on croit.

```
us6000thra   ids = ['usauto6000thra', 'us6000thra']   sources = ,usauto,us,
```

`usauto` est la solution automatique, `us` la solution révisée. **Même séisme,
identifiants différents.** Joindre le flux et le catalogue sur `id` ferait
disparaître la révision : le séisme apparaîtrait comme deux événements distincts,
une alerte automatique évaporée et un événement révisé surgi de nulle part.

**1 151 séismes sur 17 536** sont concernés — et pour tous, l'identifiant retenu
a changé.

La clé de jointure est donc **l'intersection des ensembles `ids`**, pas `id`.

> Le brief initial annonçait un autre piège : plusieurs réseaux déclarant le
> même séisme séparément dans un même instantané. **Vérification faite, ça
> n'existe pas** : sur 252 événements du flux, aucun alias n'apparaît comme
> événement distinct, l'USGS fusionne déjà côté serveur.

### 2. Six échelles de magnitude qui ne mesurent pas la même chose

| Famille | Échelle | Séismes | Sature à | Magnitude max observée |
|---|---|---|---|---|
| ondes de volume | `mb` | 15 614 | **6,5** | 6,3 |
| moment | `mww` | 1 437 | — | **8,8** |
| moment | `mwr` | 347 | — | 5,2 |
| locale | `ml` | 74 | 6,5 | 5,85 |
| durée | `md` | 30 | 5,0 | 4,55 |

`mb` couvre 89 % des séismes mais **sature vers 6,5** : au-delà, elle
sous-estime. `mww`, qui ne sature pas, porte les magnitudes extrêmes.

**Nous ne convertissons pas.** Les formules entre échelles sont régionales et
empiriques — une relation calibrée en Californie ne vaut pas en Indonésie.
Appliquer une formule universelle produirait des nombres d'apparence rigoureuse
et sans fondement. Silver **qualifie** chaque mesure (famille, seuil de
saturation) au lieu de la transformer, ce qui rend la règle de comparaison
énonçable.

### 3. Trois états, pas deux

Un événement peut être `automatic`, `reviewed` — ou **retiré** du catalogue
(faux positif, tir de carrière). Le flux l'a publié, le catalogue ne le contient
plus.

### 4. `time` n'est pas `updated`

`time` est l'instant du séisme, `updated` celui de la dernière modification de
la fiche. **Bronze est partitionné sur la date d'ingestion**, immuable par
construction : un séisme révisé ne peut pas retomber dans une partition déjà
marquée `_SUCCESS`.

---

## Démarrer

### 1. Lancer la pile

```bash
docker compose up -d
```

Onze services : HDFS (namenode + datanode), Kafka en KRaft, Spark standalone
(master + worker), Airflow avec Postgres, le producteur USGS, le job de
streaming, le carnet et le **tableau de bord temps réel**.

Le flux temps réel démarre seul et alimente Bronze en continu.

### 2. Déclencher la chaîne batch

```bash
make bronze
```

Le DAG Bronze ingère les lots mensuels manquants, puis réveille Silver, qui
réveille Gold — par Datasets, sans intervention.

### 3. Consulter

| Service | URL | Identifiants |
|---|---|---|
| **Carnet de restitution** | http://localhost:8889 | — |
| Airflow | http://localhost:8099 | `admin` / `admin` |
| HDFS | http://localhost:9871 | — |
| Spark master | http://localhost:8097 | — |

### 4. Arrêter

```bash
make down       # arrêt    |    make clean : arrêt + purge des volumes
```

---

## Empreinte mesurée

| Ressource | Mesure |
|---|---|
| Mémoire, pile complète au repos | **2,78 Gio** sur 11,37 alloués à Docker |
| Bronze | 11,9 Mo catalogue + flux en croissance |
| Silver | 4,7 Mo en Delta |
| Images | Spark 2,65 Go · Airflow 2,13 Go · app 223 Mo · carnet |

Airflow tourne en **LocalExecutor sans Celery ni Redis** : rien dans l'énoncé ne
réclame d'exécution distribuée pour trois DAG dont les tâches sont des appels
HTTP. Le travail lourd est chez Spark.

## Structure

```
docker-compose.yml        onze services, une commande
Makefile                  make bronze / versions / chain / site / replay / clean
DECISIONS.md              les arbitrages : le choix, l'alternative, la raison
scripts/prove_replay.sh   la preuve de rejeu, rejouable
dags/
  lake_datasets.py        les Datasets qui chaînent les couches
  bronze_catalog_ingestion.py
  silver_events.py
  gold_insights.py
src/
  common/                 config, WebHDFS, client USGS, session Spark, lecteur Gold
  ingest/                 producteur Kafka, stream vers Bronze, batch vers Bronze
  silver/                 échelles de magnitude, modèle unifié, versionnement Delta
  gold/                   les cinq tables de restitution
notebooks/insights.ipynb  le carnet, exécuté avec ses figures
```

## Ce que ces données ne permettent pas encore

Le **seuil d'alerte optimal** exige d'observer le même séisme en version
automatique *puis* en version révisée. Seul le flux accumulé le permet, et il
tourne depuis peu : la table `alert_reliability` existe, sa méthode est écrite,
son échantillon grandit à chaque heure d'écoute.

C'est une limite du temps d'observation, pas de l'architecture. Le carnet
l'affiche explicitement plutôt que de publier un taux calculé sur deux
événements — ce ne serait pas un résultat, ce serait une illusion de résultat.
