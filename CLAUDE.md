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
(`refresh=True`) is the only sanctioned full rewrite. When diagnosing a
suspect store, check for a torn append (time-dimensioned arrays at differing
lengths) or a doubled/non-monotonic time axis by reading `.zmetadata` shapes
and sampling the `time` array — both are symptoms of a crashed/mis-moded run,
not something appends self-heal.

## Config source of truth

Reference designators, stream params, and configs live in rca-data-tools:
https://github.com/OOI-CabledArray/rca-data-tools/tree/main/rca_data_tools/qaqc/params
(installed into the image via the `rca-data-tools @ git+...@main` dependency).
