# pipewatch

> CLI tool to monitor and alert on ETL pipeline health metrics in real time

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install -e .
```

---

## Usage

Start monitoring a pipeline by pointing pipewatch at your metrics endpoint or config file:

```bash
pipewatch monitor --config pipeline.yaml
```

Set alert thresholds and get notified when metrics go out of bounds:

```bash
pipewatch monitor --source postgres://user:pass@host/db \
                  --alert-on error_rate>0.05 \
                  --alert-on lag>300s \
                  --notify slack
```

Check the status of all tracked pipelines at a glance:

```bash
pipewatch status
```

**Example `pipeline.yaml`:**

```yaml
pipelines:
  - name: user_events
    source: kafka://broker:9092/events
    checks:
      - metric: lag
        threshold: "> 500"
      - metric: error_rate
        threshold: "> 0.02"
    notify:
      - type: slack
        webhook: https://hooks.slack.com/...
```

---

## License

[MIT](LICENSE)