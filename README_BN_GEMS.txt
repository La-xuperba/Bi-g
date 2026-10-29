BINANCE GEMS updated starter files

Upload/replace these files in your GitHub repository:
  - crypto_signal_bot.py
  - flash_pump_bot.py
  - run_bot.py
  - requirements.txt
  - .github/workflows/binance-gems.yml

GitHub Actions Secrets required:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
  - BINANCE_SQUARE_OPENAPI_KEY (only if Square OpenAPI is enabled)
  - COINGECKO_API_KEY (optional; free Demo key; keyless fallback is attempted)

Notes:
- The trading universe is Binance Spot USDT pairs only.
- CoinGecko market-cap matching uses unique symbols only to avoid ambiguous symbol mappings.
- CoinGecko's public API has rate limits; this bot requests up to four bulk pages per scan.
- Tokenized-stock exclusion is a maintained ticker blocklist and may need updating.
- Square endpoint/authentication may change; confirm the API response and a test post before relying on automatic posting.
- The score is a screening heuristic, not a prediction or guarantee of profit.
- Do not paste API secrets into source files.
