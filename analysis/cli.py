import click


@click.group()
def cli():
    pass


@cli.command()
def run_analysis():
    print("Analysis completed")


@cli.command()
@click.argument("config_dir", required=True, type=click.Path(file_okay=False))
def validate_config(config_dir):
    print(f"Validating config files in: {config_dir}")
