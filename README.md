# BINANCE GEMS — Early Pump Signals + Flash Pump Alerts

This upgrade preserves the supplied original `crypto_signal_bot.py` scanner and adds an independent Flash Pump monitor.

## Behavior

- **Regular signals:** only coins returned by the original scanner's `eligible` filter are posted as regular signals. Each eligible coin gets its own Telegram message, a persistent serial number, and a 12-hour default signal cooldown.
- **Flash alerts:** Binance Spot and Gate.io Spot, +5% or more over rolling 5m/15m/30m windows. Alerts require no score and are never labeled as signals. No minimum price filter is used. Same exchange/pair/window has a 60-minute default cooldown.
- A coin may receive both message types if it independently qualifies for both.
- Individual Telegram posts are spaced by random 10–20 seconds.
- CoinGecko market caps and CoinMarketCap fallback from the original scanner are retained. No fixed USD turnover floor is added to the regular scanner.
- Binance Square drafts are saved as separate text posts. Automatic Square publishing is attempted only if `BINANCE_SQUARE_API_URL` is configured to an authorized service matching the documented generic JSON contract in the existing source. This is not asserted to be a direct official Binance endpoint.
- The bot never places trades.

## Files

- `crypto_signal_bot.py` — original early-pump scanner, preserved as the main strategy.
- `flash_pump_bot.py` — independent Binance/Gate.io Flash Pump detector and Telegram alerts.
- `run_bot.py` — orchestrator, serial numbering, individual signal posts, delays and cooldown.
- `.github/workflows/binance-gems.yml` — scheduled/manual GitHub Actions workflow.
- `requirements.txt` — Python dependencies.

## GitHub setup

Upload the project files into the root of your repository, preserving `.github/workflows/binance-gems.yml`.

In **Settings → Secrets and variables → Actions**, add:

Required:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional market-cap data:
- `COINGECKO_API_KEY`
- `COINMARKETCAP_API_KEY`

Optional Square adapter (only if you have an authorized service that accepts this exact contract):
- `BINANCE_SQUARE_API_URL`
- `BINANCE_SQUARE_API_TOKEN`

Then open **Actions → BINANCE GEMS Early Signals + Flash Alerts → Run workflow**.

## Important monitoring limitation

The flash monitor checks up to `FLASH_PUMP_MAX_PAIRS` pairs per exchange, ranked by 24h quote turnover (default 250), to stay within public API limits. It has **no minimum price filter**, but a pair outside this scan budget may be missed. Increase the value cautiously; scanning every listed pair via individual candle requests can hit exchange limits or exceed GitHub Actions runtime. For exhaustive, near-real-time coverage, use a continuously running WebSocket monitor on a persistent host. GitHub scheduled jobs can also start late, so this is not guaranteed real-time detection.

The workflow runs every 5 minutes, but the exchange scan itself can take time. A fast pump may start and finish between scans. A five-minute candle is not guaranteed to be caught just because the workflow schedule is five minutes.

## Verification

The Python files should be syntax-checked and tests run before enabling live posting. Real Telegram delivery and Square publication require the user's own credentials and cannot be verified from this package alone. Square posts remain drafts unless an authorized publishing integration is configured.
