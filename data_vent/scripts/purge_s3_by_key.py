"""
General-purpose S3 key purge: delete objects whose key contains any of one or
more substrings. Dry-run by default.

Lists every object under each --prefix (whole bucket if none given) and keeps a
key for deletion if it contains ANY of the --contains substrings (OR logic). If
no --contains is passed, every key under the prefixes matches.

Dry-run (the default) writes the matched keys to files_to_delete.md for review
and deletes nothing. Pass --execute to actually delete. Always point at the test
bucket first with --bucket ooi-rca-qaqc-test.

Credentials come from AWS_KEY / AWS_SECRET via rca_data_tools get_s3_kwargs.

Examples
--------
Delete throttled SPKIRA span images (the old delete_throttled_span_images run) --
month/year images for the three SPKIRA profilers, one invocation:

    AWS_KEY=... AWS_SECRET=... python data_vent/scripts/purge_s3_by_key.py \\
        --bucket ooi-rca-qaqc-prod \\
        --prefix QAQC_plots/CE04OSPS/CE04OSPS-SF01B-3D-SPKIRA102 \\
        --prefix QAQC_plots/RS01SBPS/RS01SBPS-SF01A-3D-SPKIRA101 \\
        --prefix QAQC_plots/RS03AXPS/RS03AXPS-SF03A-3D-SPKIRA301 \\
        --contains _month_ --contains _year_ \\
        --execute

Delete every object whose key contains a tag (the old purge_s3_by_key run):

    AWS_KEY=... AWS_SECRET=... python data_vent/scripts/purge_s3_by_key.py \\
        --bucket ooi-rca-qaqc-test --contains foo --execute
"""
import click
import s3fs

from rca_data_tools.qaqc.constants import S3_BUCKET
from rca_data_tools.qaqc.utils import get_s3_kwargs


@click.command()
@click.option("--bucket", default=S3_BUCKET, help="Target S3 bucket.")
@click.option(
    "--prefix",
    "prefixes",
    multiple=True,
    default=[""],
    help="Key prefix to scope the listing. Repeatable. Omit to scan the whole bucket.",
)
@click.option(
    "--contains",
    "patterns",
    multiple=True,
    help="Substring a key must contain to match. Repeatable (OR). Omit to match all.",
)
@click.option("--execute", is_flag=True, help="Actually delete (default is dry-run).")
def main(bucket, prefixes, patterns, execute):
    fs = s3fs.S3FileSystem(**get_s3_kwargs())

    keys = set()
    for prefix in prefixes:
        path = f"{bucket}/{prefix}"
        # glob prefix-matches filenames; find recursively walks the whole bucket
        keys.update(fs.glob(f"{path}*") if prefix else fs.find(path))

    if patterns:
        to_delete = [k for k in keys if any(p in k for p in patterns)]
    else:
        to_delete = list(keys)

    if not execute:
        report = f"# {len(to_delete)} objects to delete from {bucket}\n\n"
        report += "\n".join(f"- `{k}`" for k in sorted(to_delete)) + "\n"
        out = "files_to_delete.md"
        with open(out, "w") as f:
            f.write(report)
        print(f"Dry-run. Wrote {len(to_delete)} matched keys to {out}. Re-run with --execute to delete.")
        return

    for k in to_delete:
        print(f"Deleting s3://{k}")
        fs.rm(k)
    print(f"Deleted {len(to_delete)} objects from {bucket}.")


if __name__ == "__main__":
    main()
