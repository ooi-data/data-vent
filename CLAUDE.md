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
