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

A harvest-status json with null (or `NaT`) `end_date` + a clean store is NOT a
stuck state on its own. A refresh nulls the date fields at start, writes to the
temp bucket, and only repopulates the status at finalize. A daily append that
fires mid-refresh reads the null and dies with `NullMetadataError` (None) or
`ParserError: NaTZ` (`"NaT"`) — transient collateral that self-heals when the
refresh finalizes. Only treat it as a real problem if the refresh run itself
landed Failed/Crashed with no matching finalize. Don't flag these transient
append failures as store corruption.

## Config source of truth

Reference designators, stream params, and configs live in rca-data-tools:
https://github.com/OOI-CabledArray/rca-data-tools/tree/main/rca_data_tools/qaqc/params
(installed into the image via the `rca-data-tools @ git+...@main` dependency).

**rca-data-tools changes need an image rebuild.** The deployment's only pull
step clones data-vent; rca-data-tools is resolved at image build time, so
editing `maxCoordinateSizes.csv` without `docker buildx ... --push` is a no-op.

`run_stream_ingest(streams=[...])` resolves `<refdes>/<name>.yaml`, so pass the
**filename stem** — it can differ from the stream name inside (`..._beam_5.yaml`
holds stream `vadcp_b_velocity_beam5`).

## Refreshing a stream (two-phase, see README)

1. `_REQUEST_DATA` — `refresh=ON`, `force_harvest=ON`. Usually ends in
   `DataNotReadyError` after ~40 min; that is the designed outcome.
2. `_GET_DATA` — `refresh=ON`, `force_harvest=OFF`, hours later. Writes the zarr.

Phase 1 nulls `start_date`/`end_date`, so daily appends fail with
`NullMetadataError` between the phases — normal, self-heals at finalize. Only a
problem if phase 2 can never succeed, and nothing times the window out: look for
a days-old `requested_at` still at `process_status: pending`.

Phase 2 never re-requests (`check_requested` just returns `data_check`), so
repeated phase-2 runs re-process the same, possibly stale, payload.

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
