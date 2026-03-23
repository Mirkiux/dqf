"""Pytest configuration — force non-interactive matplotlib backend for all tests."""

import matplotlib

matplotlib.use("Agg")
