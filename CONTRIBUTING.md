# Contributing

Thanks for your interest. This is a lab repository: changes are welcome when they keep it runnable on a
laptop and keep every number in the README reproducible.

## Ground rules

- **Measured numbers only.** If a change affects results, re-run `make backfill`, `make report` and
  `make demo`, regenerate the images (see the README) and commit the new `docs/results/*.json`
  together with the charts. Never edit a results file by hand.
- **Logic lives in `mlops/`, not in the DAG.** Tasks in `dags/churn_monthly.py` stay thin wrappers so
  that the logic can be tested without Airflow.
- **Thresholds are decisions.** Changing a retrain trigger, the promotion margin or a quality rule
  needs an ADR update in `docs/adr/` that explains the reason.
- **One logical change per commit**, with a Conventional Commit subject (`feat:`, `fix:`, `docs:`,
  `perf:`, `test:`, `chore:`) and a body that explains why.
- **No secrets.** `.env` is generated from `.env.example` and is gitignored. Lab credentials only.
- Tests: `make test` must pass. Add a test for every quality rule or drift change.

## Development loop

```bash
make env && make up && make test
make backfill && make status    # about 10 minutes for 12 months
make report && make demo
```

To start over with an empty registry and lake: `docker compose --env-file .env -f docker/compose.yaml down -v`
(this deletes the lab volumes).

## License of contributions

By contributing you agree that your contribution is licensed under the AGPL-3.0, as described in
[LICENSE](LICENSE) and [NOTICE](NOTICE).
