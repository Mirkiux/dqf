"""Figure factories for longitudinal check results.

Each factory function accepts pre-computed data and returns a zero-argument
callable that builds and returns a ``matplotlib.Figure``.  Figures use the
``matplotlib.figure.Figure`` API directly (no pyplot state machine) so they
are safe for concurrent rendering and embedding in HTML reports.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import numpy as np
from matplotlib.figure import Figure

if TYPE_CHECKING:
    import pandas as pd


# ── Colour helpers ─────────────────────────────────────────────────────────────

_GREEN = "#2ca02c"
_RED = "#d62728"
_BLUE = "steelblue"
_GREY = "#aaaaaa"

# tab10 palette — avoids runtime cm.tab10 attribute lookup that mypy cannot resolve
_TAB10 = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]


def _result_color(passed: bool) -> str:
    return _GREEN if passed else _RED


# ── TrendCheck ─────────────────────────────────────────────────────────────────


def trend_figure(
    timeseries_df: pd.DataFrame,
    tau: float,
    p_value: float,
    p_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Line chart of the period-level metric with a linear trend overlay."""
    _df = timeseries_df[["period", "metric"]].copy()
    _tau, _p, _thr, _passed, _var = tau, p_value, p_threshold, passed, variable_name

    def _make() -> Figure:
        labels = [str(v) for v in _df["period"]]
        values = _df["metric"].to_numpy(float)
        fig = Figure(figsize=(8, 4))
        ax = fig.add_subplot(1, 1, 1)
        ax.plot(labels, values, marker="o", color=_BLUE, linewidth=1.5, label="metric")
        if len(values) >= 2:
            x = np.arange(len(values))
            m, b = np.polyfit(x, values, 1)
            ax.plot(
                labels,
                m * x + b,
                linestyle="--",
                color=_result_color(_passed),
                linewidth=1.5,
                label=f"trend (τ={_tau:.3f}, p={_p:.4f})",
            )
        ax.set_title(f"{_var} — Trend  (p threshold ≤ {_thr})")
        ax.set_xlabel("Period")
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.legend(fontsize=8)
        fig.tight_layout()
        return fig

    return _make


# ── StructuralBreakCheck ───────────────────────────────────────────────────────


def structural_break_figure(
    timeseries_df: pd.DataFrame,
    cusum_stat: float,
    cusum_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Two-panel chart: metric over time (top) and normalised CUSUM (bottom)."""
    _df = timeseries_df[["period", "metric"]].copy()
    _stat, _thr, _passed, _var = cusum_stat, cusum_threshold, passed, variable_name

    def _make() -> Figure:
        labels = [str(v) for v in _df["period"]]
        values = _df["metric"].to_numpy(float)
        mean = float(values.mean())
        std = float(values.std(ddof=1)) if len(values) > 1 else 1.0
        cusum_norm = np.cumsum(values - mean) / (std if std > 0 else 1.0)

        fig = Figure(figsize=(8, 6))

        ax1 = fig.add_subplot(2, 1, 1)
        ax1.plot(labels, values, marker="o", color=_BLUE, linewidth=1.5)
        ax1.axhline(mean, linestyle="--", color=_GREY, linewidth=0.8, label=f"μ = {mean:.3f}")
        ax1.set_title(f"{_var} — Structural Break  (CUSUM = {_stat:.3f}, threshold = {_thr}σ)")
        ax1.set_ylabel("metric")
        ax1.tick_params(axis="x", rotation=45, labelsize=8)
        ax1.legend(fontsize=8)

        ax2 = fig.add_subplot(2, 1, 2)
        ax2.plot(
            labels,
            cusum_norm,
            marker="o",
            color=_result_color(_passed),
            linewidth=1.5,
            label="CUSUM / σ",
        )
        ax2.axhline(_thr, linestyle="--", color=_RED, linewidth=0.8)
        ax2.axhline(-_thr, linestyle="--", color=_RED, linewidth=0.8, label=f"± {_thr}σ")
        ax2.set_xlabel("Period")
        ax2.set_ylabel("CUSUM / σ")
        ax2.tick_params(axis="x", rotation=45, labelsize=8)
        ax2.legend(fontsize=8)

        fig.tight_layout()
        return fig

    return _make


# ── SeasonalityCheck ───────────────────────────────────────────────────────────


def seasonality_figure(
    groups: dict[int, list[float]],
    stat: float,
    p_value: float,
    p_threshold: float,
    season_length: int,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Box plots of metric values grouped by seasonal position."""
    _groups = {k: list(v) for k, v in groups.items()}
    _stat, _p, _thr = stat, p_value, p_threshold
    _sl, _passed, _var = season_length, passed, variable_name

    def _make() -> Figure:
        positions = sorted(_groups.keys())
        data = [_groups[pos] for pos in positions]
        labels = [str(pos + 1) for pos in positions]  # 1-indexed season labels
        n = len(positions)
        fig = Figure(figsize=(max(6, n * 0.7), 4))
        ax = fig.add_subplot(1, 1, 1)
        bp = ax.boxplot(data, patch_artist=True)
        face_color = _result_color(_passed)
        for patch in bp["boxes"]:
            patch.set_facecolor(face_color)
            patch.set_alpha(0.6)
        ax.set_xticks(range(1, n + 1))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_title(
            f"{_var} — Seasonality  (H = {_stat:.3f}, p = {_p:.4f}, threshold p ≤ {_thr})"
        )
        ax.set_xlabel(f"Season position  (cycle length = {_sl})")
        ax.set_ylabel("metric")
        fig.tight_layout()
        return fig

    return _make


# ── ChiSquaredDriftCheck ───────────────────────────────────────────────────────


def chisquared_drift_figure(
    df: pd.DataFrame,
    half: int,
    min_p: float,
    p_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Stacked bar chart of category proportions per period with baseline/test split."""
    _df = df.copy()
    _half, _min_p, _thr, _passed, _var = half, min_p, p_threshold, passed, variable_name

    def _make() -> Figure:
        periods = sorted(_df["period"].unique())
        categories = sorted(_df["category"].astype(str).unique())
        n_periods = len(periods)
        n_cats = len(categories)

        prop_matrix = np.zeros((n_cats, n_periods))
        for j, p in enumerate(periods):
            sub = _df[_df["period"] == p]
            cats = sub["category"].astype(str)
            cnts = sub["count"].astype(int)
            cat_counts = dict(zip(cats, cnts, strict=False))
            total = sum(cat_counts.values()) or 1
            for i, cat in enumerate(categories):
                prop_matrix[i, j] = cat_counts.get(cat, 0) / total

        colors = [_TAB10[i % len(_TAB10)] for i in range(n_cats)]
        fig = Figure(figsize=(max(6, n_periods * 0.6), 5))
        ax = fig.add_subplot(1, 1, 1)
        labels = [str(p) for p in periods]
        bottom = np.zeros(n_periods)
        for i, cat in enumerate(categories):
            ax.bar(labels, prop_matrix[i], bottom=bottom, color=colors[i], label=cat, width=0.6)
            bottom += prop_matrix[i]

        # Vertical line between baseline and test window
        ax.axvline(x=_half - 0.5, linestyle="--", color="black", linewidth=1.0, label="split")
        ax.set_ylim(0, 1)
        ax.set_title(
            f"{_var} — Chi-Squared Drift  (min p = {_min_p:.4f}, threshold p = {_thr})"
        )
        ax.set_xlabel("Period")
        ax.set_ylabel("Proportion")
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        if n_cats <= 10:
            ax.legend(loc="upper right", fontsize=7)
        fig.tight_layout()
        return fig

    return _make


# ── KSDriftCheck ───────────────────────────────────────────────────────────────


def ks_drift_figure(
    period_values: dict[str, list[float]],
    periods_sorted: list[str],
    half: int,
    min_p: float,
    p_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Empirical CDF per period — baseline periods in blue, test periods in result colour."""
    _pv = {k: list(v) for k, v in period_values.items()}
    _periods = list(periods_sorted)
    _half, _min_p, _thr, _passed, _var = half, min_p, p_threshold, passed, variable_name

    def _make() -> Figure:
        n = len(_periods)
        fig = Figure(figsize=(8, 4))
        ax = fig.add_subplot(1, 1, 1)
        legend_added: set[str] = set()

        for i, p in enumerate(_periods):
            vals = _pv.get(p, [])
            if len(vals) < 2:
                continue
            vals_sorted = sorted(vals)
            ecdf_y = np.arange(1, len(vals_sorted) + 1) / len(vals_sorted)
            is_baseline = i < _half
            color = _BLUE if is_baseline else _result_color(_passed)
            alpha = 0.35 + 0.45 * (i / max(n - 1, 1))
            legend_key = "baseline" if is_baseline else "test"
            label = legend_key if legend_key not in legend_added else None
            if label:
                legend_added.add(legend_key)
            ax.step(
                vals_sorted,
                ecdf_y,
                where="post",
                color=color,
                alpha=alpha,
                linewidth=1.2,
                label=label,
            )

        ax.set_title(f"{_var} — KS Drift  (min p = {_min_p:.4f}, threshold p ≤ {_thr})")
        ax.set_xlabel("value")
        ax.set_ylabel("ECDF")
        ax.legend(fontsize=8)
        fig.tight_layout()
        return fig

    return _make


# ── ProportionDriftCheck ───────────────────────────────────────────────────────


def proportion_drift_figure(
    df: pd.DataFrame,
    half: int,
    min_p: float,
    p_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Period-level proportion line chart with baseline mean and ±2σ band."""
    _df = df.copy()
    _half, _min_p, _thr, _passed, _var = half, min_p, p_threshold, passed, variable_name

    def _make() -> Figure:
        labels = [str(p) for p in _df["period"]]
        proportions = (_df["positive"] / _df["n"].clip(lower=1)).tolist()

        fig = Figure(figsize=(8, 4))
        ax = fig.add_subplot(1, 1, 1)

        baseline_props = proportions[:_half]
        if baseline_props:
            bm = float(np.mean(baseline_props))
            bs = float(np.std(baseline_props, ddof=1)) if len(baseline_props) > 1 else 0.0
            ax.axhline(
                bm,
                linestyle="--",
                color=_BLUE,
                linewidth=0.9,
                label=f"baseline mean = {bm:.3f}",
            )
            if bs > 0:
                ax.fill_between(
                    range(len(labels)),
                    bm - 2 * bs,
                    bm + 2 * bs,
                    alpha=0.12,
                    color=_BLUE,
                    label="baseline ±2σ",
                )

        ax.plot(
            labels,
            proportions,
            marker="o",
            color=_result_color(_passed),
            linewidth=1.5,
            label="proportion",
        )
        ax.axvline(x=_half - 0.5, linestyle="--", color="black", linewidth=1.0, label="split")
        ax.set_ylim(0, 1)
        ax.set_title(
            f"{_var} — Proportion Drift  (min p = {_min_p:.4f}, threshold p ≤ {_thr})"
        )
        ax.set_xlabel("Period")
        ax.set_ylabel("Proportion")
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        ax.legend(fontsize=8)
        fig.tight_layout()
        return fig

    return _make


# ── DistributionDriftCheck ─────────────────────────────────────────────────────


def distribution_drift_figure(
    timeseries_df: pd.DataFrame,
    reference: np.ndarray,
    current: np.ndarray,
    psi: float,
    psi_threshold: float,
    passed: bool,
    variable_name: str,
) -> Callable[[], Figure]:
    """Left: metric time series split by reference/current window.
    Right: histogram overlay of reference vs current distributions."""
    _df = timeseries_df[["period", "metric"]].copy()
    _ref = reference.copy()
    _cur = current.copy()
    _psi, _thr, _passed, _var = psi, psi_threshold, passed, variable_name

    def _make() -> Figure:
        fig = Figure(figsize=(10, 4))

        # Left: time series
        ax1 = fig.add_subplot(1, 2, 1)
        labels = [str(v) for v in _df["period"]]
        values = _df["metric"].to_numpy(float)
        n = len(values)
        split = max(1, n // 2)
        ax1.plot(labels[:split], values[:split], marker="o", color=_BLUE, linewidth=1.5,
                 label="reference")
        if split < n:
            # Connector between last reference and first test point
            ax1.plot(labels[split - 1:split + 1], values[split - 1:split + 1],
                     color=_GREY, linewidth=1.0)
        ax1.plot(labels[split:], values[split:], marker="o",
                 color=_result_color(_passed), linewidth=1.5, label="current")
        ax1.axvline(x=split - 0.5, linestyle="--", color="black", linewidth=0.8)
        ax1.set_title(f"{_var} — metric over time")
        ax1.set_xlabel("Period")
        ax1.tick_params(axis="x", rotation=45, labelsize=8)
        ax1.legend(fontsize=8)

        # Right: histogram overlay
        ax2 = fig.add_subplot(1, 2, 2)
        combined = np.concatenate([_ref, _cur])
        n_bins = min(10, max(3, len(combined) // 3))
        ax2.hist(_ref, bins=n_bins, alpha=0.5, color=_BLUE, density=True, label="reference")
        ax2.hist(_cur, bins=n_bins, alpha=0.5, color=_result_color(_passed), density=True,
                 label="current")
        ax2.set_title(f"Distribution  (PSI = {_psi:.4f}, threshold = {_thr})")
        ax2.set_xlabel("metric value")
        ax2.set_ylabel("density")
        ax2.legend(fontsize=8)

        fig.tight_layout()
        return fig

    return _make
