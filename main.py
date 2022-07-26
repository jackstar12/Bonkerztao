import importlib
import multiprocessing

STOCKS = ['btc', 'eth']


def start_process(symbol, config):
    # Whatever um den einzelnen prozess zu starten
    print(f'Aktie: {symbol}, ORDERS (variable aus config): {config.ORDERS}')


def main():

    for stock in STOCKS:
        stock_config = importlib.import_module(f'bots.{stock}.config')
        try:
            stock_main = importlib.import_module(f'bots.{stock}.main')
        except ImportError:
            pass  # Nix custom
        start_process(stock, stock_config)



if __name__ == '__main__':
    main()
