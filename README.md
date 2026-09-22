# AFTERSHOCK — the shrinking earthquake

A **Bronze → Silver → Gold** data lake on HDFS, fed by **three** USGS sources —
a real-time feed through Kafka, a historical catalog loaded in batches, and the
history of superseded versions — orchestrated by Airflow, extended by an MLlib
model and a standalone web application.

```bash
docker compose up -d
make bronze      # the monthly catalog batches
make versions    # the version history — long, run nothing else in parallel
make chain       # Silver -> Gold -> model -> reporting bundle
make site        # injects the bundle into site/aftershock.html
```

**http://localhost:8889** for the notebook · `site/aftershock.html` for the
application.

---

## The business problem

When an earthquake occurs, the USGS publishes a magnitude computed
**automatically** from the first stations that recorded the shaking. That value
triggers tsunami alerts, evacuations and rescue deployments.

Hours — often weeks — later, a seismologist **reviews** that magnitude using all
the available recordings. It changes. An M3.60 announced at Takotna, Alaska,
became **M5.20**; an M6.30 off Kamchatka fell back to **M5.30**.

This project measures that gap, explains where it comes from, and derives a
defensible alert threshold from it: from which announced magnitude should an
alert be triggered, accepting which false-alarm rate?

### A correction we owe the jury

An earlier version of this README claimed that "nobody archives the gap,
because each version overwrites the previous one". **That is false, and
verification showed us so.** The `includesuperseded=true` parameter of the FDSN
endpoint opens the full history:

| Query on `us7000pwpu` | Origin versions | Size |
|---|---|---|
| `query?eventid=X&format=geojson` | 3 | 58 KB |
| `query?eventid=X&includesuperseded=true` | **10** | **309 KB** |

What remains true is more solid: the archive **can only be queried one
earthquake at a time, knowing its identifier in advance, at a cost of 309 KB**.
It is not in the real-time feeds, not in the bulk endpoint, and not joined to
anything. It is **archived but unusable**.

That is what a lake fixes — and it is this third source that makes the model
possible.

## What the pipeline establishes

Over **17,526 earthquakes** in the 2025 catalog, **2,128** whose full version
history was harvested, and a real-time feed accumulated continuously:

| Finding | Measured figure |
|---|---|
| Successive versions archived | **7,939** for 2,128 earthquakes |
| Earthquakes whose magnitude moves between two versions | **735 (34.5%)** |
| Largest gap observed | **+1.60** (M3.60 → M5.20, Takotna, Alaska) |
| Median delay of the first archived solution | **17.8 minutes** |
| Median delay before the record stabilises | **75.1 days** |
| Earthquakes carrying the trace of a replaced automatic solution | **1,151 (6.6%)** |
| … whose retained identifier has changed since | **1,151, i.e. all of them** |
| Share of the `mb` scale, which saturates at 6.5 | **89%** of M≥4 earthquakes |

### Two delays that do not measure the same thing

An earlier version of this README announced "1.6 to 3.5 minutes" for the
publication of an automatic solution. That figure came from the `review_lag`
table, which measures the gap between the moment of the earthquake and the
`updated` field of the records seen in the feed — in other words **the freshness
of a record that has already been published**.

Harvesting the history gives another, more demanding measure: the gap between
the earthquake and the **first `origin` product the USGS kept**, i.e. a
**17.8-minute median** (min 2.9 · max 66.9).

Both are accurate and do not contradict each other; they do not answer the same
question. The second is the one that matters for alerting, and it is the one we
now retain.

---

## Architecture

```
  ┌ SOURCE 1 ─ all_hour.geojson ──► Kafka ──► Spark Structured Streaming ─┐
  │            (every minute)       quakes_live                           │
  │                                                                       ▼
  ├ SOURCE 2 ─ FDSN query ──────────────────────────────►  HDFS /lake/bronze
  │            (monthly batches, M≥4)                      raw GeoJSON + _SUCCESS
  │                                                                       │
  └ SOURCE 3 ─ FDSN includesuperseded=true ───────────────────────────────┤
               (version history, batches of 100)                          │
                                                                          ▼  Spark + Delta
                                            HDFS /lake/silver/events · event_versions
                                    versions, resolved identities, qualified magnitudes
                                                                          │
                                                                          ▼  Spark
                                                              HDFS /lake/gold
              revision_history · identity_history · alert_reliability · magnitude_scales
              review_lag · magnitude_revision · alert_curve · lake_diff · human_witness
                                                                          │
                              ┌───────────────────────────┬───────────────┤
                              ▼                           ▼               ▼
                   Pandas notebook        MLlib GBT model       Real-time
                  (direct Parquet)      /lake/models/*          dashboard + globe
                                        exported as JSON            (Docker)
```

**Orchestration.** Four DAGs, chained by **Airflow Datasets** rather than by
sensors: each DAG declares the Dataset it produces, and the next one is
scheduled on it. A single trigger cascades all the way through.

```
bronze_catalog_ingestion  ──►  silver_events  ──►  gold_insights
                        Dataset            Dataset

versions_pipeline : harvest ─► silver ─► dataset ─► curve ─► model ─► export
```

The `versions_pipeline` DAG is triggered manually, not scheduled: its first task
calls the USGS API once per earthquake, i.e. **nearly two hours of network
time**. Scheduling it would mean hammering a free public service.

## Replay, proven rather than claimed

The assignment stresses this requirement with two exclamation marks. A sentence
in a README is worth nothing; a procedure that a grader can replay is worth
everything.

```bash
make replay
```

The script wipes everything, triggers the DAG, **kills Airflow mid-run**,
restarts it, relaunches, then replays once more on an already complete lake.

| Step | Markers | Files |
|---|---|---|
| after wipe | 0 | 0 |
| **after hard kill** | **5** | 5 |
| after relaunch | **12** | 12 |
| after second replay | **12** | 12 |

**12,462,374 bytes before and after the replay, to the byte.**

---

## The dataset's traps

### 1. An earthquake's identity changes over time

This is the central trap, and it is not the one you would expect.

```
us6000thra   ids = ['usauto6000thra', 'us6000thra']   sources = ,usauto,us,
```

`usauto` is the automatic solution, `us` the reviewed solution. **Same
earthquake, different identifiers.** Joining the feed and the catalog on `id`
would make the revision disappear: the earthquake would appear as two distinct
events, an automatic alert that evaporated and a reviewed event that came out of
nowhere.

**1,151 earthquakes out of 17,536** are affected — and for all of them, the
retained identifier has changed.

The join key is therefore **the intersection of the `ids` sets**, not `id`.

> The initial brief announced another trap: several networks reporting the same
> earthquake separately in the same snapshot. **Once verified, it does not
> exist**: across 252 events in the feed, no alias appears as a distinct event;
> the USGS already merges them server-side.

### 2. Six magnitude scales that do not measure the same thing

| Family | Scale | Earthquakes | Saturates at | Max magnitude observed |
|---|---|---|---|---|
| body waves | `mb` | 15,614 | **6.5** | 6.3 |
| moment | `mww` | 1,437 | — | **8.8** |
| moment | `mwr` | 347 | — | 5.2 |
| local | `ml` | 74 | 6.5 | 5.85 |
| duration | `md` | 30 | 5.0 | 4.55 |

`mb` covers 89% of earthquakes but **saturates around 6.5**: above that, it
underestimates. `mww`, which does not saturate, carries the extreme magnitudes.

**We do not convert.** The formulas between scales are regional and empirical —
a relation calibrated in California does not hold in Indonesia. Applying a
universal formula would produce numbers that look rigorous and have no basis.
Silver **qualifies** each measurement (family, saturation threshold) instead of
transforming it, which makes the comparison rule statable.

### 3. Three states, not two

An event can be `automatic`, `reviewed` — or **removed** from the catalog (false
positive, quarry blast). The feed published it; the catalog no longer contains
it.

### 4. `time` is not `updated`

`time` is the moment of the earthquake, `updated` the moment of the record's last
modification. **Bronze is partitioned on the ingestion date**, immutable by
construction: a reviewed earthquake cannot fall back into a partition already
marked `_SUCCESS`.

---

## Getting started

### 1. Start the stack

```bash
docker compose up -d
```

Eleven services: HDFS (namenode + datanode), Kafka in KRaft mode, Spark
standalone (master + worker), Airflow with Postgres, the USGS producer, the
streaming job, the notebook and the **real-time dashboard**.

The real-time feed starts on its own and feeds Bronze continuously.

### 2. Trigger the batch chain

```bash
make bronze
```

The Bronze DAG ingests the missing monthly batches, then wakes Silver, which
wakes Gold — through Datasets, with no intervention.

### 3. Browse

| Service | URL | Credentials |
|---|---|---|
| **Reporting notebook** | http://localhost:8889 | — |
| Airflow | http://localhost:8099 | `admin` / `admin` |
| HDFS | http://localhost:9871 | — |
| Spark master | http://localhost:8097 | — |

### 4. Stop

```bash
make down       # stop    |    make clean : stop + purge volumes
```

---

## Measured footprint

| Resource | Measurement |
|---|---|
| Memory, full stack at rest | **2.78 GiB** out of 11.37 allocated to Docker |
| Bronze | 11.9 MB catalog + growing feed |
| Silver | 4.7 MB in Delta |
| Images | Spark 2.65 GB · Airflow 2.13 GB · app 223 MB · notebook |

Airflow runs with the **LocalExecutor, without Celery or Redis**: nothing in the
assignment calls for distributed execution for three DAGs whose tasks are HTTP
calls. The heavy work is done by Spark.

## Structure

```
docker-compose.yml        eleven services, one command
Makefile                  make bronze / versions / chain / site / replay / clean
DECISIONS.md              the trade-offs: the choice, the alternative, the reason
scripts/prove_replay.sh   the replay proof, replayable
dags/
  lake_datasets.py        the Datasets that chain the layers
  bronze_catalog_ingestion.py
  silver_events.py
  gold_insights.py
src/
  common/                 config, WebHDFS, USGS client, Spark session, Gold reader
  ingest/                 Kafka producer, stream to Bronze, batch to Bronze
  silver/                 magnitude scales, unified model, Delta versioning
  gold/                   the five reporting tables
notebooks/insights.ipynb  the notebook, executed with its figures
```

## What this data does not allow yet

The **optimal alert threshold** requires observing the same earthquake in its
automatic version *and then* its reviewed version. Only the accumulated feed
allows this, and it has not been running for long: the `alert_reliability` table
exists, its method is written, and its sample grows with every hour of
listening.

This is a limit of observation time, not of the architecture. The notebook
shows it explicitly rather than publishing a rate computed on two events —
that would not be a result, it would be the illusion of a result.
