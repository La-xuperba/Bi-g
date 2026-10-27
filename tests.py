"""Lightweight offline tests for flash alert logic and separation."""
import unittest
from flash_pump_bot import pct_change, format_flash_post

class FlashPumpTests(unittest.TestCase):
    def test_exact_five_percent(self):
        self.assertAlmostEqual(pct_change(100, 105), 5.0)

    def test_negative_change(self):
        self.assertAlmostEqual(pct_change(100, 94), -6.0)

    def test_invalid_open(self):
        self.assertIsNone(pct_change(0, 105))

    def test_flash_post_has_no_score_or_signal_serial(self):
        post = format_flash_post({"exchange":"Binance Spot", "pair":"ABCUSDT", "window":"5m", "minutes":5, "change":5.6, "price":0.1234})
        self.assertNotIn("Score:", post)
        self.assertNotIn("SIGNAL #", post)
        self.assertIn("5m", post)
        self.assertIn("5.60%", post)

if __name__ == "__main__":
    unittest.main()
