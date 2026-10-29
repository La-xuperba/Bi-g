"""Run the BINANCE GEMS regular scanner and independent flash monitor."""
import logging
import crypto_signal_bot as core
import flash_pump_bot as flash

if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")
    core.main()
    flash.run()
