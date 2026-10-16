# Lab 3 — Spark Structured Streaming sur Wikimedia

Thomas Soubagné - thomassbgn

Consignes : https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/05.structured-streaming/lab_docker_wikistreams.md

## Organisation

```
lab-structured-streaming/
├── compose.yaml              Kafka (KRaft) + Jupyter PySpark
├── admin.py                  création du topic wikistreams
├── wikistream_producer.py    flux Wikimedia → Kafka, avec filtres
├── requirements.txt          dépendances du producteur (hôte)
├── data/  jobs/              montés dans le conteneur
└── notebooks/
    └── wikistream_pyspark.ipynb   consumer Spark (démo + explorations)
```

`notebooks/` est monté dans `/home/jovyan/work`, `jobs/` et `data/` sous
`/home/jovyan`.

## Exécution sous Windows / PowerShell

Prérequis : Docker Desktop démarré, Python 3.11+.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

docker compose up -d
docker logs pyspark_notebook          # lien http://127.0.0.1:8888/lab?token=...
python admin.py                       # crée le topic wikistreams
python wikistream_producer.py         # 10 minutes de fr.wikipedia par défaut
```

Dans JupyterLab, ouvrir `work/wikistream_pyspark.ipynb` puis *Run All*. Les
sorties `console` du consumer apparaissent dans les logs :

```powershell
docker logs -f pyspark_notebook
```

Arrêt : `docker compose down`. Le broker n'a pas de volume, les messages sont
supprimés.

Adaptations par rapport aux fichiers fournis :

- Le conteneur Kafka s'appelle `wikistreams_lab_kafka` : le nom `kafka` était
  déjà pris par un ancien conteneur. Le notebook utilise le nom de service
  `kafka:19092`, qui ne change pas.
- La version du connecteur `spark-sql-kafka` est celle de Spark dans l'image
  (`pyspark.__version__`, 4.2.0 ici) au lieu de 4.1.0 codé en dur.
- `admin.py` attend la réponse du broker et accepte un topic déjà existant.
- pywikibot fonctionne sans compte (`PYWIKIBOT_NO_USER_CONFIG`) et a besoin de
  `requests-sse`.
- Le producteur suit le flux en direct ; `since='20260209'` est remplacé par
  l'option `--since`, Wikimedia ne gardant qu'environ 7 jours d'historique.
  Il attend la livraison des messages (`flush`) au lieu d'un `sleep(60)`.

## Filtres au niveau du producteur (étape 11)

Les filtres utilisent `EventStreams.register_filter` de pywikibot. Les
événements écartés ne sont jamais envoyés dans Kafka.

| Option | Effet | Exemple |
| --- | --- | --- |
| `--server-name` | wikis suivis, ou `all` | `--server-name en.wikipedia.org de.wikipedia.org` |
| `--type` | types de changement | `--type edit new` |
| `--title` | pages précises | `--title "Paris" "Lyon"` |
| `--namespace` | espaces de noms (0 = articles) | `--namespace 0` |
| `--bots-only` / `--humans-only` | type d'auteur | `--humans-only` |
| `--minutes`, `--since`, `--quiet` | durée, rejeu, affichage | `--minutes 5 --quiet` |

Sans option, le comportement de la démo est conservé : modifications de
`fr.wikipedia.org` pendant 10 minutes. Les filtres ont été testés sur un
topic temporaire : `--server-name all --type edit new --bots-only` (77
événements en 15 s, Commons, Wiktionnaire, etc.) et
`--server-name ja.wikipedia.org --namespace 0 --humans-only`.

## Consumer Spark (étapes 8 à 12)

Le notebook garde la démo, puis ajoute des colonnes communes :

- `event_time` : `timestamp_seconds(timestamp)`, l'heure de l'événement côté
  Wikipédia (*event time*), et non l'heure de réception par Spark ;
- `size_delta` : `length.new - length.old`, octets ajoutés ou retirés ;
- un *watermark* de 10 minutes sur `event_time`, qui borne l'état conservé
  et le retard accepté.

| Requête | Fenêtre | Mesures | Sortie |
| --- | --- | --- | --- |
| `demo_bot_edits` | fixe, 1 h | modifications robots / humains | console, `complete` |
| `tumbling_1min` | **fixe (tumbling)**, 1 min | modifications, mineures, octets ajoutés / retirés, taille moyenne, par wiki et type d'auteur | console `update` + mémoire |
| `sliding_10min` | **glissante (overlapping)**, 10 min toutes les 2 min | modifications, contributeurs distincts, % robots, variation nette, plus grosse modification, par wiki | console `update` + mémoire |
| `top_pages` | glissante, 5 min toutes les 1 min | modifications et contributeurs par page, top 3 par wiki en SQL | mémoire |
| `large_edits` | aucune (sans état) | modifications de plus de 5 000 octets | console, `append` |

**Fenêtres fixes ou glissantes.** Avec une fenêtre fixe d'une minute, chaque
modification compte dans une seule ligne : la somme des fenêtres redonne le
total. Avec une fenêtre de 10 minutes qui avance de 2 minutes, chaque
modification appartient à 10 / 2 = 5 fenêtres. Les valeurs se recouvrent et
forment une moyenne mobile, plus lisse que le découpage minute par minute.

Les sinks `memory` servent à afficher les résultats dans le notebook avec
`spark.sql`. Le tri et `LIMIT` ne sont pas possibles directement sur un flux
agrégé en mode `update`, mais le sont sur la table mémoire.
`processAllAvailable()` attend que les messages déjà présents dans Kafka
soient traités avant l'affichage.

## Résultat vérifié le 16 octobre 2026

Producteur lancé 12 minutes sur fr, en, de et es.wikipedia.org, puis
notebook exécuté en entier dans le conteneur (Spark 4.2.0, Kafka 4.1.1).
Les sorties sont enregistrées dans le notebook.

| Wiki | Minutes actives | Modifications | Octets ajoutés | Octets retirés |
| --- | ---: | ---: | ---: | ---: |
| en.wikipedia.org | 13 | 839 | 168 836 | 84 281 |
| fr.wikipedia.org | 13 | 313 | 61 019 | 23 556 |
| de.wikipedia.org | 13 | 248 | 43 756 | 4 933 |
| es.wikipedia.org | 12 | 113 | 51 137 | 903 |

Total : 1 513 événements envoyés et retrouvés par la démo, avec 1 237
modifications humaines et 276 de robots. Les 12 minutes d'envoi touchent 13
fenêtres d'une minute, la première et la dernière étant partielles.

Les sept requêtes tournaient en même temps. La seule trace d'exception dans
les logs est un avertissement de Spark à l'arrêt du kernel, lorsqu'il ne
parvient pas à supprimer ses dossiers temporaires.
