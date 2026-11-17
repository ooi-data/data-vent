# data-vent

Harvests OOI Regional Cabled Array streams from OOINet (M2M) into zarr stores
in the `ooi-data` S3 bucket. Runs as Prefect flows on AWS ECS Fargate.

## Debugging pipeline runs (Prefect Cloud)

If the `prefect` CLI is authenticated to the RCA Cloud workspace, flow-run
logs can be pulled directly — no need to paste dumps for recent runs:

- List failures: `prefect flow-run ls --state Failed --state Crashed`
- Pull logs: `prefect flow-run logs <flow-run-id>` (`--tail -n 50` for the end)
- The `ls` table truncates IDs; get full IDs (and filter/live-tail) via the
  async client `prefect.client.orchestration.get_client()` + `read_flow_runs`
  / `read_logs`.

Retention: only recent runs (~days) keep logs in Prefect Cloud. Older runs
age out — their stdout lives in CloudWatch (ECS Fargate) instead.

To point Claude at a run: give the flow-run ID, the UI URL, or the stream
name + "most recent" / "running now" and it will resolve the ID.

## Store integrity (zarr)

Stores are structurally immutable during normal appends. A refresh
(`refresh=True`) is the only sanctioned full rewrite. Diagnose a suspect store
by reading `.zmetadata` (one S3 GET — sampling the time array over many big
stores times out). Corruption fingerprints:

- **Torn**: time-dimensioned arrays at DIFFERING lengths (some vars extended,
  others not). Cause: crashed mid-append — `to_zarr` writes vars one at a time
  with no transaction. Worsens across later appends. Surfaces downstream as
  `ValueError: conflicting sizes for dimension 'time'` in QAQC `xr.open_zarr`.
- **Doubled**: uniform lengths but a NON-monotonic time axis (resets backward).
  A full re-pulled series appended on top of the existing one. Uniform length
  does not rule this out — must sample `time` and check monotonicity. Cause: a
  refresh payload consumed by an append-mode run.
- **NaT boundary**: a NaN/fill time boundary makes `_update_time_coverage`
  write the literal `"NaT"` into the status json `end_date`, which later breaks
  a daily append with `ParserError: Unknown string format: NaTZ`.

## Interpreting failed runs

A daily append that ends Completed with "Refresh pending; skipping append" is
waiting out that stream's refresh, not stuck. A failed refresh shows up as a
Failed `refresh-sweeper` run naming the stream.

## Config source of truth

Ownership: data-vent owns everything that talks to OOINet and writes data stores;
rca-data-tools owns the instrument registry and everything that reads stores and renders.

Reference designators, stream params, and configs live in rca-data-tools:
https://github.com/OOI-CabledArray/rca-data-tools/tree/main/rca_data_tools/qaqc/params
(installed into the image via the `rca-data-tools @ git+...@main` dependency).
Read them only through the installed package (`rca_data_tools.qaqc.constants`), never
from GitHub raw URLs, so one run never mixes two config versions.

**rca-data-tools changes need an image rebuild.** The deployment's only pull
step clones data-vent; rca-data-tools is resolved at image build time. A push to
rca-data-tools main triggers the rebuild (`build-image.yaml`); until it finishes,
edits to `harvest.yaml` or `maxCoordinateSizes.csv` are not live.

The daily harvest list is rca-data-tools `HARVEST_STREAMS`: every stage 1/2
instrument's `zarrFile` in `sitesDictionary.csv` plus `extra_streams` in
`params/harvest.yaml`, which also sets non-default ECS sizes (`HARVEST_COMPUTE`).
`run_stream_ingest(streams=[...])` takes table names
(`<refDes>-<method>-<stream>`), including archived streams not in the daily list.

## Refreshing a stream (see README)

Launch with `refresh=True` only. A refresh is pending while the status json has
`data_check` set and `data_response` ending `refresh`
(`HarvestStatus.refresh_pending`). Finalize, the 2-day request timeout, an empty
delivery and a missing response file all clear it.

- Not pending: the run makes a new (forced) m2m request. Pending: it processes
  the outstanding request. `force_harvest=True` drops a pending request for a new one.
- Not ready within `check_data`'s ~40 min: the run ends Completed. The hourly
  `refresh-sweeper` flow launches the processing run (tagged `refresh-sweeper`)
  once m2m's `status_url` says ready, or after 2 days so `check_data` settles it.
- Each request gets one sweeper launch. If it fails, the sweeper ends Failed every
  hour until a human relaunches.
- Appends skip a pending stream (`check_requested` returns `REFRESH_PENDING`); its
  start/end dates stay valid throughout.

## Non-time dimension growth (DimensionChangedError)

`_validate_dims` reindexes an incoming dim *down* silently but raises on
*growth* (no in-place reindex since `3f23ff3`, 2026-07-01). A refresh rebuilds
from t0, so the earliest deployment sets the store dims — if a later deployment
is bigger, refresh can never succeed even while daily appends keep working.

Fix: add a row to `maxCoordinateSizes.csv` in rca-data-tools, then refresh so the
store is rewritten at that size. Leave headroom to the next chunk boundary.

- The row and the refresh must land together; the row alone breaks working appends.
- Matching is `inst_key in instrument`, a substring test on the **refdes**: `ADCP`
  also matches `VADCP`, and a row hits **every stream of that instrument**
  (VADCPB301 serves `vadcp_b_velocity_beam` at bin 167 and
  `vadcp_b_velocity_beam5` at bin 100 — one row forces a refresh of both).
