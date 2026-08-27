# Décisions d'architecture

Chaque entrée : le choix retenu, l'alternative écartée, la raison.

---

## D1 — La clé de jointure est l'intersection des `ids`, pas l'`id`.

**Choix.** Deux enregistrements décrivent le même séisme si leurs ensembles
`ids` s'intersectent.

**Alternative écartée.** Joindre le flux et le catalogue sur `id`.

**Pourquoi.** Mesuré sur le catalogue du 1er au 5 août : `us6000thra` porte
`ids=['usauto6000thra','us6000thra']` et `sources=,usauto,us,`. `usauto` est la
solution automatique, `us` la solution révisée — **même séisme, identifiants
différents**. Sur 16 événements multi-identifiants, **9 ont un id retenu qui
n'est pas le premier de leur liste**.

Joindre sur `id` ferait disparaître la révision : le séisme apparaîtrait comme
deux événements distincts, un automatique évaporé et un révisé surgi de nulle
part. C'est exactement la mesure que ce projet produit — elle serait fausse à
100 %, sans lever la moindre erreur.

**Correction apportée au brief.** Le brief annonçait une déduplication
inter-réseaux au sein d'un même instantané. Vérification faite sur les 252
événements du flux : **0 alias apparaît comme événement séparé**, les 13 alias
relevés sont déjà fusionnés côté USGS. Ce problème n'existe pas. Le vrai
problème est que l'identité change dans le temps.

---

## D2 — Delta Lake en Silver, Parquet nu en Gold.

**Choix.** Silver en Delta, Gold en Parquet.

**Alternative écartée.** Tout en Parquet, avec un SCD2 écrit à la main.

**Pourquoi.** Le critère n°1 de l'énoncé n'est pas le format mais le rejeu sans
duplication. Un job Spark tué pendant une écriture Parquet laisse des fichiers
partiels dans le répertoire cible : la reprise duplique, ou exige un ménage
manuel qu'aucun test ne couvre. Delta donne des **commits atomiques** — un job
tué ne laisse rien de visible.

Gold reste en Parquet nu parce que l'énoncé exige qu'il soit « lisible tel
quel » par un notebook Pandas. Un `read.parquet` sur un répertoire Delta
renverrait toutes les versions, y compris les lignes supprimées, donc des
résultats faux. Delta là où le problème est dur, Parquet là où la simplicité est
exigée.

---

## D3 — Le partitionnement Bronze porte sur la date d'INGESTION.

**Choix.** `source=X/ingest_date=YYYY-MM-DD` pour le flux,
`source=X/year=YYYY/month=MM` pour le catalogue.

**Alternative écartée.** Partitionner sur `time`, l'instant du séisme.

**Pourquoi.** Un séisme révisé produit un nouvel enregistrement. S'il était
partitionné sur `time`, il retomberait dans une partition ancienne, déjà
marquée `_SUCCESS` — et le marqueur d'idempotence deviendrait un mensonge. La
date d'ingestion est immuable par construction : ce qui est écrit un jour reste
dans la partition de ce jour, quoi qu'il advienne de l'événement ensuite.

Le catalogue est partitionné sur le mois de l'événement parce qu'un lot batch
**est** défini par sa fenêtre temporelle — c'est ce qui rend le marqueur
`_SUCCESS` signifiant.

---

## D4 — Airflow en LocalExecutor, sans Celery ni Redis.

**Choix.** Un seul conteneur Airflow, `LocalExecutor`, Postgres pour les
métadonnées.

**Alternative écartée.** CeleryExecutor avec Redis et des workers séparés.

**Pourquoi.** 11,37 Go alloués à Docker pour HDFS, Kafka, Spark et Airflow.
CeleryExecutor ajoute Redis et au moins un worker, soit environ 1,5 Go, pour
distribuer trois DAG dont les tâches sont des appels HTTP de quelques secondes.
Rien dans l'énoncé ne réclame d'exécution distribuée côté orchestrateur — le
travail lourd est chez Spark.

**Mesuré, pile complète au repos : 2,78 Gio sur 11,37.** La contrainte
matérielle annoncée comme risque ne s'est pas matérialisée.

---

## D5 — `spark.cores.max` plafonné par application.

**Choix.** Chaque application Spark déclare son plafond de cœurs.

**Pourquoi.** En standalone, une application prend par défaut **tous** les cœurs
du worker. Leçon retenue d'un projet précédent : un premier job de streaming
avait monopolisé les huit cœurs et le second attendait indéfiniment sur
`Initial job has not accepted any resources`, sans erreur ni log explicite.
Avec plusieurs jobs de streaming permanents, le plafond n'est pas une
optimisation, c'est une condition de fonctionnement.

---

## D6 — Le producteur crée son topic ; pas de conteneur dédié.

**Choix.** Le producteur appelle `AdminClient` au démarrage.

**Alternative écartée.** Un service `topics` éphémère, comme sur le projet Kafka
précédent.

**Pourquoi.** Un conteneur de moins dans un budget mémoire à surveiller, et le
producteur est propriétaire du topic qu'il alimente. La création automatique de
topics reste désactivée côté broker : une faute de frappe échoue au lieu de
fabriquer un topic fantôme à une partition.

---

## D7 — Le producteur tourne dans une image Python, pas dans l'image Spark.

**Choix.** `python:3.11-slim` pour le producteur, image Spark pour les jobs.

**Alternative écartée.** Une image unique.

**Pourquoi.** Constaté au build : `confluent-kafka` 2.6.0 n'a pas de roue pour
Python 3.8, la version embarquée par l'image Apache Spark officielle, et la
compilation depuis les sources échoue faute d'en-têtes `librdkafka`.

Mais l'argument tient sans cet incident : le producteur appelle une API HTTP et
écrit dans Kafka. Il n'a aucune raison d'embarquer 2,6 Go de Spark. **223 Mo au
lieu de 2,65 Go.**

---

## D8 — Le compte d'événements se lit dans `features`, pas dans `metadata.count`.

**Choix.** `len(document["features"])`.

**Pourquoi.** Constaté à l'exécution : l'endpoint FDSN `query` **ne renvoie pas**
`metadata.count`, contrairement aux flux `summary`. Il renvoie `limit` et
`offset`. Le marqueur `_SUCCESS` enregistrait donc `"events": 0` sur des lots de
800 Ko — une valeur fausse, écrite sans erreur, dans le fichier même censé
attester de l'intégrité du lot.

C'est précisément le genre de bug que ce projet dénonce, et il s'était glissé
dans notre propre code.

**Garde-fou ajouté.** Si le nombre d'événements atteint la limite de l'API, le
lot est rejeté plutôt qu'écrit tronqué : la troncature FDSN est silencieuse.

---

## D9 — Le rejeu est prouvé par un script, pas affirmé.

**Choix.** `scripts/prove_replay.sh` : table rase, déclenchement, arrêt brutal
d'Airflow en plein vol, redémarrage, relance, puis second rejeu sur un lac
complet.

**Pourquoi.** L'énoncé souligne cette exigence de deux points d'exclamation.
Une phrase dans un README ne vaut rien ; une manipulation qu'un correcteur
rejoue vaut tout.

**Résultat mesuré :**

| Étape | Marqueurs | Fichiers |
|---|---|---|
| après table rase | 0 | 0 |
| après arrêt brutal | **5** | 5 |
| après relance | **12** | 12 |
| après second rejeu | **12** | 12 |

Octets dans Bronze : **12 462 374 avant et après le rejeu**, à l'octet près.

---

## D10 — La révision est temporelle, pas « flux contre catalogue ».

**Constat mesuré.** Sur 247 séismes présents à la fois dans le flux et dans le
catalogue au même instant : **247 ensembles `ids` identiques, 0 magnitude
différente**. Les deux endpoints sont servis par la même base USGS ; ils ne
divergent pas.

**Conséquence sur la conception.** La révision ne se mesure pas en comparant les
deux sources à un instant donné. Elle se mesure dans le temps, et c'est **notre
propre flux, accumulé minute après minute en Bronze, qui constitue l'archive**
des états successifs. Le catalogue fournit l'état de référence ; le flux fournit
l'histoire.

**Bonne nouvelle collatérale.** Puisque les ensembles `ids` sont stables entre
les versions, `min(ids)` est une clé d'identité fiable. Pas besoin d'un calcul
de composantes connexes.

---

## D11 — Les magnitudes ne sont pas converties, elles sont qualifiées.

**Choix.** Silver porte `magnitude`, `magnitude_type`, `magnitude_family`,
`saturation_threshold`, `magnitude_saturated` et `magnitude_is_reference`.
Aucune conversion de valeur.

**Alternative écartée.** Convertir toutes les magnitudes vers l'échelle de
moment avec une formule de régression.

**Pourquoi.** Les formules de conversion entre échelles sont **régionales et
empiriques** : une relation `ml` vers `mw` calibrée en Californie ne vaut pas en
Indonésie. Appliquer une formule universelle produirait des nombres d'apparence
rigoureuse et sans fondement — exactement ce que ce projet dénonce.

La normalisation retenue porte donc sur les **métadonnées de l'échelle**, pas
sur la valeur : on rend explicite à quelle famille appartient chaque mesure et
au-delà de quel seuil elle sature. La règle de comparaison est alors énonçable :
comparer au sein d'une famille, ou en dessous du seuil de saturation.

**Mesuré sur les 17 536 séismes M≥4 :** `body_wave` 15 614, `moment` 1 817,
`local` 77, `duration` 30, `surface_wave` 1. La répartition annoncée dans le
brief (`ml` 155, `md` 76) portait sur le flux 24 h **toutes magnitudes
confondues** — les petits séismes locaux. Au-delà de M4, c'est `mb` qui domine à
89 %. Le seuil de magnitude change complètement la distribution des échelles.

---

## D12 — Dépendance entre DAG par Dataset Airflow, pas par capteur.

**Choix.** Le DAG Bronze déclare `outlets=[BRONZE_CATALOG]`, le DAG Silver est
planifié sur `schedule=[BRONZE_CATALOG]`.

**Alternative écartée.** `ExternalTaskSensor`.

**Pourquoi.** Le capteur exige que les deux DAG partagent la même date
d'exécution : un déclenchement manuel du Silver attendrait indéfiniment un run
Bronze à l'horodatage exact. Le Dataset exprime la dépendance réelle — « Silver
dépend de la fraîcheur de Bronze » — sans coupler les calendriers, et sans
occuper un emplacement de tâche à attendre.

**Vérifié :** un `dags trigger bronze_catalog_ingestion` produit un run Silver
`dataset_triggered__…` qui passe en `success`, sans intervention.

---

## D13 — Le DAG Bronze signale même quand il n'a rien à faire.

**Choix.** La tâche finale porte `trigger_rule=ALL_DONE`.

**Le bug, mesuré.** Avec la règle par défaut, les états des tâches étaient :
`summarise` **sautée 6 fois sur 8**. Quand tous les mois sont déjà ingérés,
`pending_months()` rend une liste vide, `ingest.expand([])` ne produit aucune
tâche mappée, et Airflow saute l'aval. Le Dataset n'était donc jamais émis et
**le DAG Silver ne se réveillait jamais**.

**Ce que ça enseigne.** L'idempotence qui rend le DAG sûr le rendait muet. Un
étage parfaitement idempotent qui ne trouve rien à faire doit quand même
signaler « je suis à jour » — sinon l'aval ne tourne plus dès le second jour.
C'est le genre de panne qui ne se voit pas au premier lancement, seulement au
deuxième.

---

## D14 — Le lecteur JSON de Spark, pas `text` avec `wholetext`.

**Choix.** `session.read.option("multiLine", "true").schema(...).json(path)`.

**Le bug, mesuré.** La première version lisait le catalogue par
`read.option("wholetext","true").text(path)` puis `from_json`. Résultat :
**17 526 lignes retournées pour 12 fichiers** — l'option n'a pas pris effet, et
la réponse FDSN contient des retours à la ligne internes. Chaque ligne était
donc un fragment de JSON invalide, `from_json` rendait `null`, et le Silver ne
contenait que les 10 séismes du flux temps réel. **Aucune erreur levée.**

Le lecteur JSON avec `multiLine` est l'outil prévu pour un document par fichier.
Après correction : 17 536 séismes distincts.

---

## D15 — `identity_history` remplace `network_disagreement`.

**Choix.** La table Gold mesure le changement d'identité dans le temps.

**Alternative écartée.** La table prévue au brief, qui mesurait le désaccord
entre réseaux sur un même séisme.

**Pourquoi.** La table prévue reposait sur un phénomène qui n'existe pas : les
réseaux ne déclarent pas le même séisme séparément, l'USGS fusionne côté
serveur. Construire une table sur un phénomène absent aurait produit une page
vide, ou pire, un artefact.

**Ce que la table mesure, sur 17 536 séismes :** 1 151 (6,6 %) portent la trace
d'une solution automatique remplacée, et **tous** ont vu leur identifiant
retenu changer. Chacun aurait été compté deux fois par une jointure naïve.

---

## D16 — Le carnet affiche ce qui n'est pas encore mesurable.

**Choix.** La section sur le seuil d'alerte teste la taille de l'échantillon et
affiche un avertissement quand elle est insuffisante, au lieu de tracer une
courbe.

**Alternative écartée.** Publier le taux de fausse alerte calculé sur les deux
séismes dont nous avons capté la révision.

**Pourquoi.** Un taux calculé sur deux événements n'est pas un résultat, c'est
une illusion de résultat — et c'est précisément le travers que ce projet
dénonce chez les autres. La méthode est écrite, la table existe, l'échantillon
grandit à chaque heure d'écoute du flux. Dire « pas encore » vaut mieux que
publier un chiffre indéfendable.

C'est aussi une limite du **temps d'observation**, pas de l'architecture : le
pipeline capture correctement chaque version, il lui faut simplement des jours
plutôt que des heures.

---

## D17 — Le carnet lit Gold sans Spark.

**Choix.** Une image `python:3.11-slim` avec pandas, pyarrow et WebHDFS.

**Alternative écartée.** Un noyau Jupyter dans l'image Spark.

**Pourquoi.** Les tables Gold font au maximum quelques milliers de lignes. Le
travail distribué a déjà eu lieu en amont ; démarrer une SparkSession pour lire
17 536 lignes de Parquet coûterait plus que la lecture. C'est aussi ce que
demande l'énoncé — « un notebook Pandas lisant directement les tables Gold » —
et c'est la raison pour laquelle Gold est en Parquet nu et non en Delta.

---

## D18 — La prémisse du projet était fausse : l'USGS archive bien les versions.

**Constat mesuré.** Le paramètre `includesuperseded=true` de l'endpoint FDSN
ouvre l'historique complet d'un séisme. Sur `us7000pwpu` :

| Requête | Versions d'origine | Poids |
|---|---|---|
| `query?eventid=X&format=geojson` | 3 | 58 Ko |
| `query?eventid=X&includesuperseded=true` | **10** | **309 Ko** |

Chaque version porte magnitude, échelle, statut de révision, nombre de stations
et gap azimutal.

**Ce que le README affirmait.** « Personne n'archive l'écart, parce que chaque
version écrase la précédente. » **C'est faux et il fallait le corriger.**

**Ce qui reste vrai, et qui est plus défendable.** L'archive existe mais n'est
consultable **qu'un séisme à la fois, en connaissant son identifiant à
l'avance, au prix de 309 Ko**. Elle n'est ni dans les flux temps réel, ni dans
l'endpoint de masse, ni jointe à quoi que ce soit. Elle est **archivée mais
inexploitable**. C'est exactement ce qu'un lac corrige.

Cette formulation survit à un jury qui connaît l'API ; l'ancienne non.

---

## D19 — Une cohorte témoin, parce qu'une sélection non mesurée est un biais.

**Choix.** Récolter l'historique de deux populations : les **1 151** séismes qui
ont conservé un identifiant `usauto`, et **1 000** tirés au hasard parmi les
16 375 autres (graine fixée `20260827`, tirage sans remise).

**Alternative écartée.** Ne récolter que les 1 151, qui sont les seuls dont on
sait qu'une solution automatique a existé.

**Pourquoi.** N'entraîner que sur la cohorte tracée reviendrait à apprendre sur
une population particulière — ceux dont USGS a conservé la trace automatique —
et à appliquer le modèle à tous. **Le biais serait invisible et non mesuré.**
La cohorte témoin ne sert pas à grossir l'échantillon : elle sert à **chiffrer
l'écart entre les deux populations**. Si elles se comportent pareil, la
sélection est inoffensive et c'est démontré ; sinon, la limite est connue et
énonçable.

---

## D20 — MLlib plutôt que scikit-learn, en assumant que ce n'est pas évident.

**Choix.** `GBTRegressor` de Spark MLlib.

**Alternative écartée.** scikit-learn.

**Pourquoi.** Honnêtement : sur environ 2 000 lignes, **scikit-learn est le
choix rationnel** — il entraînerait en une seconde là où MLlib paie un shuffle
distribué. Trois raisons font pencher l'autre côté :

1. Le modèle doit être **un artefact du lac**, versionné et rejouable comme le
   reste. MLlib écrit nativement sur HDFS ; sklearn imposerait un format de
   sérialisation ad hoc.
2. **Aucune dépendance nouvelle** — numpy arrive déjà avec pandas.
3. Le jeu de données est plafonné par le **temps de récolte**, pas par la
   méthode : sans le filtre M≥4 et sur plusieurs années, il passe à des
   centaines de milliers de lignes sans changer une ligne de code.

Ce n'est pas un choix évident et il ne faut pas le présenter comme tel.

---

## D21 — Une erreur de transport n'est pas une réponse HTTP.

**Constat.** La récolte est tombée au lot 6 sur
`requests.exceptions.ConnectionError` levé par `WebHdfs.write`. Le gestionnaire
d'appel n'attrapait que `HdfsError` — l'erreur l'a traversé et tué le job après
six lots réussis.

**Cause.** `WebHdfs._check` convertissait les codes HTTP ≥ 400 en `HdfsError`
mais **laissait passer les échecs de transport**, qui surviennent avant toute
réponse. Le défaut touchait les **sept** points d'appel, donc toutes les
écritures Bronze, pas seulement la récolte.

**Correction.** Un enrobage `call()` convertit toute `RequestException` en
`HdfsError` après quatre tentatives à recul croissant.

---

## D22 — Un identifiant Kafka qui ressemblait à de l'ASCII sans en être.

**Constat.** Après un redémarrage de Docker, Kafka refuse de démarrer :

```
Invalid cluster.id in: /tmp/kafka-logs/meta.properties.
Expected 7Qm2xVfLTBСqXn9pLwKdRA, but read 7Qm2xVfLTBÐ¡qXn9pLwKdRA
```

Les deux chaînes paraissent identiques à l'écran.

**Cause.** Le `CLUSTER_ID` du `docker-compose.yml` contenait un **С cyrillique
(U+0421)** en onzième position, à la place du `C` latin. Vérifié : Python refuse
d'imprimer la valeur en `cp1252` et nomme le coupable, `'\u0421'`.

Le caractère traversait Docker en UTF-8 lors du premier formatage, puis était
relu autrement au démarrage suivant. **La pile a fonctionné des heures avec cet
identifiant** : le volume gardait la version écrite au premier lancement, et
rien ne comparait les deux avant un redémarrage complet.

**Correction.** Identifiant régénéré en base64 pur ASCII, avec l'assertion
`fresh.isascii() and len(fresh) == 22` au moment de la génération. Le volume
Kafka a été purgé — perte nulle, le lac Bronze porte déjà les messages et le
producteur republie chaque minute.

**Pourquoi cette entrée figure ici.** C'est le cinquième défaut de la même
famille rencontré sur ce projet : **une valeur fausse qui ne lève aucune erreur
au moment où elle est écrite**. Elle a survécu à toute une phase de
développement parce que rien ne la relisait. C'est exactement ce que nous
reprochons à une magnitude automatique publiée sans mention de son incertitude.

---

## D23 — Le tableau de bord temps réel est un service, pas une page.

**Choix.** Un service `dashboard` dans la pile, qui consomme Kafka en continu,
sonde HDFS et Spark, et diffuse l'état en SSE. La page se connecte à
`/api/live` quand elle est servie par lui, et se déclare hors ligne sinon.

**Alternative écartée.** Rejouer un flux enregistré dans la page publiée, pour
qu'elle paraisse vivante partout.

**Pourquoi.** Un faux direct est un mensonge de démonstration. La page dit
explicitement « hors ligne » hors de la pile plutôt que de simuler un trafic.
Ce qu'elle affiche en direct est lu sur les conteneurs : 1 255 fichiers en
Bronze flux, 2 cœurs sur 6 pris par le job de streaming, cadence mesurée à
5,6 messages par minute.

**Deux mesures fausses corrigées en chemin.**

1. **La cadence.** Calculée sur l'heure de réception, le rattrapage initial de
   dix minutes passait pour un pic à **1 767 messages/minute**. Elle se calcule
   désormais sur l'horodatage de production : **5,6/minute**, ce qui correspond
   aux 6 événements par cycle du producteur.
2. **Le compte de fichiers.** La sonde descendait l'arborescence HDFS avec une
   requête par entrée, soit des centaines d'appels par rafraîchissement.
   `GETCONTENTSUMMARY` donne fichiers et octets en un seul appel.

**Un piège d'API rencontré.** Fournir un `on_assign` à `confluent-kafka`
**remplace** l'assignation par défaut : sans appeler `assign()` soi-même, le
consommateur n'obtient aucune partition et **se tait sans lever d'erreur**.
Encore la même famille de défaut.

---

## D24 — Une variable dominante n'est pas une variable indispensable.

**Le doute.** `first_minutes_since_quake` — le délai entre le séisme et la
première solution archivée — portait **57 % du poids** du modèle. Physiquement
défendable : un séisme long à localiser est mal contraint, donc plus révisé.

Mais une lecture inquiétante existait. Notre « première version » est la plus
ancienne **que l'USGS a conservée**. Si des versions anciennes ont été purgées
pour certains séismes, ce délai mesurerait le **comportement d'archivage**, pas
la physique — et le modèle apprendrait un artefact de notre propre collecte.

**Le test.** Réentraînement à l'identique sans cette variable
(`EXCLUDED_FEATURES=first_minutes_since_quake`), même graine, même coupure
temporelle, mêmes 1 490 / 638 exemples.

| Mesure | Avec le délai | Sans le délai |
|---|---|---|
| Aire sous ROC | 0,8328 | **0,8264** |
| MAE régression | 0,0775 | 0,0791 |
| Précision / rappel | 33,3 % / 20,0 % | 25,0 % / 15,0 % |

**Verdict.** Le modèle perd **0,0064 de ROC**, soit 0,8 % de son pouvoir de
classement. L'information portée par le délai est **redondante** : sans lui,
`first_magnitude` (0,54) et `first_longitude` (0,23) la reprennent.

Une importance élevée mesure la fréquence à laquelle une variable sert à couper
un arbre, **pas** la dépendance du modèle envers elle. Seule l'ablation
distingue les deux, et nous ne pouvions pas publier ce modèle sans l'avoir
faite.

**Garde-fou ajouté.** Une ablation écrivait dans `gold/model_report/report.json`
— le fichier que lit le site. Un essai exploratoire aurait remplacé en silence
les chiffres de référence par ceux d'un modèle amputé. `RUN_LABEL` isole
désormais chaque essai dans son propre répertoire.

---

## D25 — Le témoignage humain, une hypothèse plausible et réfutée.

**L'hypothèse.** Si le public a ressenti un séisme plus fort que ne l'indiquait
la mesure instrumentale, la magnitude automatique a sous-estimé l'événement.

**Mesuré sur 2 128 séismes**, dont 305 (14,3 %) portant au moins 5 témoignages :

| Le ressenti dépasse la mesure | Séismes | Révision à la hausse | Écart moyen |
|---|---|---|---|
| non | 92 | 6,5 % | −0,018 |
| **oui** | **100** | **3,0 %** | **−0,062** |

**L'hypothèse est fausse dans ces données** : ces séismes sont révisés à la
hausse **moins** souvent, et baissent davantage.

**Ce que nous en disons.** L'écart est de 0,044 de magnitude sur une centaine de
séismes par groupe. Nous le présentons comme **une hypothèse plausible réfutée
par la mesure**, jamais comme un effet établi — l'effectif ne le permet pas.

**Un résultat au passage, plus solide.** Un séisme ressenti par au moins cinq
personnes est révisé à la baisse **13,8 %** du temps, contre **5,2 %** pour un
séisme non ressenti. L'explication tient au piège n°2 : les séismes ressentis
sont proches des zones peuplées, donc annoncés par des réseaux régionaux en `ml`
ou `md`, que le moment global corrige ensuite vers le bas.
