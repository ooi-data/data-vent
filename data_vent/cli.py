import typer
import textwrap

from data_vent.config import DATA_BUCKET
from data_vent.producer import fetch_harvest
from data_vent.metadata import cli as metadata_cli, create_data_catalog
from data_vent.stats import cli as stats_cli

app = typer.Typer()
app.add_typer(metadata_cli.app, name="metadata")
app.add_typer(stats_cli.app, name="stats")


@app.command()
def catalog(
    create: bool = False,
    s3_bucket: str = DATA_BUCKET,
    site_branch: str = "gh-pages",
):
    # try:
    if create:
        typer.echo("Creating/Updating data catalog ...")
        create_data_catalog(s3_bucket, site_branch)
    else:
        typer.echo("Please add --create to run catalog creation")
    # except Exception as e:
    #     typer.echo(f"Error found: {e}")
    #     sys.exit(1)


@app.command()
def producer(
    instrument_rd: str,
    refresh: bool = False,
    existing_data_path: str = f"s3://{DATA_BUCKET}",
):
    request_responses = fetch_harvest(instrument_rd, refresh, existing_data_path)

    if len(request_responses) == 0:
        typer.echo("WARNING: No requests to be fetched.")
    else:
        all_resp = []
        for r in request_responses:
            resp_text = textwrap.dedent(
                f"""\
                        {r["stream"]["table_name"]}
                        Refresh: {refresh}
                        Request Range: {r["params"]["beginDT"]} - {r["params"]["endDT"]}
                        Thredds: {r["result"]["thredds_catalog"]}
                        """
            )
            all_resp.append(resp_text)
        typer.echo("\n".join(all_resp))


if __name__ == "__main__":
    app()
