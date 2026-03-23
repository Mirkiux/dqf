# Checks

dqf checks fall into two families:

- **[Cross-sectional](cross_sectional.md)** — operate on a materialised DataFrame snapshot
- **[Longitudinal](longitudinal.md)** — issue an aggregation SQL query and analyse the resulting time series

Both families share a common base class hierarchy:

::: dqf.BaseCheck

::: dqf.BaseCrossSectionalCheck

::: dqf.BaseLongitudinalCheck
