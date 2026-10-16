# WikiStreams lab

Source: [Lab instructions](https://github.com/adaltas/ece-big-data-processing-fall-2026/blob/main/05.structured-streaming/lab_docker_wikistreams.md).

## Run

From this folder in PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
docker compose up -d
.venv\Scripts\python admin.py
docker logs wikistreams_notebook
```

Open the Jupyter link from the logs, then open `work/wikistream_pyspark.ipynb` and run all cells. In another terminal, start the producer:

```powershell
.venv\Scripts\python wikistream_producer.py --duration 10
```

Watch the Spark output:

```powershell
docker logs -f wikistreams_notebook
```

Kafka uses port 9094 on the host, since another lab already uses 9092. Spark connects to `kafka:19092` inside Docker. The notebook image is pinned to the image used for this run. Its Spark version is 4.2.0; the code picks the matching Kafka connector. `PYTHONPATH` points to the Spark files in the image.

## What the queries do

| Query | Window | Result |
| --- | --- | --- |
| `bot_edits_1h` | 1 hour | Bot and human edit counts from the demo |
| `tumbling_edits_1m` | 1 minute | Counts and size changes by wiki and bot |
| `sliding_edits_2m` | 2 minutes, every 30 seconds | Counts, total size change and largest edit |
| `large_page_edits` | 1 minute | Edits of at least 500 bytes, grouped by page |

Tumbling windows do not overlap. Each event belongs to one window. Sliding windows overlap; each event belongs to four windows here. Do not add their counts to get a total.

`net_bytes` is new length minus old length. It can be negative. `changed_bytes` uses the absolute difference. This is a change in page length, not the full edit diff. If a length is missing, Spark skips it in the size sums and averages but still counts the event.

The two-minute watermark limits old state. Very late events may be dropped. Update mode prints rows when their counts change. Each query has its own checkpoint in `data/checkpoints`.

## Producer filters

```powershell
.venv\Scripts\python wikistream_producer.py --server en.wikipedia.org --duration 2
.venv\Scripts\python wikistream_producer.py --title France --duration 2
.venv\Scripts\python wikistream_producer.py --bot no --duration 2
```

The default follows French Wikipedia edits. The page title must match exactly. Filters run before sending the event to Kafka. Events already in Kafka stay there, so changing the producer filter affects new events only.

## Checks and saved results

The notebook was run with real Wikimedia events on 9 October 2026. A full one-minute producer run sent 21 events with no messages left in the queue. All four Spark queries ran without exceptions. The logs include bot counts, page size changes and overlapping windows.

- `notebooks/wikistream_pyspark.executed.ipynb`: executed cells and query status.
- `data/notebook-run.log`: console tables from that run.
- `data/producer.log`: the one-minute producer run.
- `data/window-check.log`: a known event checks one tumbling window, four sliding windows and a 600-byte removal.
- `data/filter-check.log`: a filter with no matching page stops and sends zero events.

To run the window check again:

```powershell
docker exec -u jovyan wikistreams_notebook /opt/conda/bin/python /home/jovyan/jobs/check_windows.py
```

The consumer code is in `jobs/wikistream_consumer.py`. The notebook imports it so both use the same queries. For a terminal-only run, use this instead of running the notebook:

```powershell
docker exec -u jovyan wikistreams_notebook /opt/conda/bin/python /home/jovyan/jobs/wikistream_consumer.py
```

`jobs/start_notebook.py` starts the notebook in a live Jupyter session from the command line. It was used to leave the notebook running for this lab.

Stop the notebook queries in a new cell with:

```python
for query in spark.streams.active:
    query.stop()
```

Stop the Docker services with `docker compose down`. If you change a query's window or grouping, stop it and use a new checkpoint folder before restarting it.
