import click


@click.group()
def cli():
    pass


@cli.command()  # @cli, not @click!
@click.option(
    "--symbol",
    type=click.STRING
)
@click.option(
    "--state",
    type=click.STRING
)
def set(symbol, state):

    print(f'Enabling {symbol}')


@cli.command()  # @cli, not @click!
@click.option(
    "--symbol",
    type=click.STRING
)
def disable(symbol):
    print(f'Disable {symbol}')


if __name__ == "__main__":
    cli()
