# Shopping Sessions — team Mysore Pak

Builders Buildathon, Databricks track. Implements `rules.md` (clean the event/click/link CSVs, join linked IDs, sessionize, attribute, count assists) and writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet.

- `Dockerfile` (root): the submitted entry.
- `reference/`: a correctness-first DuckDB implementation, used to cross-check the submitted entry on generated datasets.

## Run locally

```sh
make fixtures                 # 36 edge-case fixtures, run in the scorer's container flags
make dev                      # 1M-event dev set (needs `generate` from the track's generator.zip)
make run-dev check-dev        # exact diff against data/dev-answer (the track's dev-answer.zip)
```

## Prior code and libraries

- `scripts/`, `fixtures/`, `rules.md` and `reference/`'s skeleton come from the Buildathon's Shopping Sessions DuckDB starter.
- Libraries are pinned in `reference/requirements.txt` and `Cargo.lock`.

MIT licensed (see `LICENSE`).
