from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from dqf.metadata.base import MetadataBuilderPipeline
from dqf.variable import Variable

if TYPE_CHECKING:
    from dqf.datasets.variables import VariablesDataset

# Each rule is stored as (insertion_index, priority, predicate, factory).
# insertion_index ensures equal-priority rules keep registration order after sort.
_Rule = tuple[int, int, Callable[[Variable], bool], Callable[[], MetadataBuilderPipeline]]


class MetadataResolver:
    """Registry that maps Variables to MetadataBuilderPipelines via predicate rules.

    Mirrors :class:`~dqf.resolver.CheckSuiteResolver` for the metadata layer.
    Rules are registered with a priority and a predicate; when resolving a
    variable, rules are evaluated in descending priority order and the first
    matching predicate wins.

    Rules with equal priority are evaluated in registration order (FIFO).

    Examples
    --------
    Use the pre-configured default resolver::

        from dqf.defaults import build_default_metadata_resolver

        resolver = build_default_metadata_resolver()
        pipeline = resolver.resolve(variable)
        pipeline.profile(dataset, variable)

    Register a custom rule at a higher priority than the defaults::

        resolver = build_default_metadata_resolver()
        resolver.register(
            predicate=lambda v: v.name == "credit_score",
            pipeline_factory=lambda: MetadataBuilderPipeline([
                ("nullability", NullabilityProfileBuilder()),
                ("distribution", DistributionShapeBuilder()),
            ]),
            priority=50,
        )
    """

    def __init__(self) -> None:
        self._rules: list[_Rule] = []
        self._counter: int = 0

    def register(
        self,
        predicate: Callable[[Variable], bool],
        pipeline_factory: Callable[[], MetadataBuilderPipeline],
        priority: int = 0,
    ) -> None:
        """Add a rule to the resolver.

        Parameters
        ----------
        predicate:
            Function that receives a :class:`~dqf.variable.Variable` and
            returns ``True`` if this rule should handle it.
        pipeline_factory:
            Zero-argument callable that returns a new
            :class:`~dqf.metadata.base.MetadataBuilderPipeline`.
            Called fresh on every match so each variable gets its own
            pipeline instance.
        priority:
            Higher values are evaluated first.  Default is ``0``.
        """
        self._rules.append((self._counter, priority, predicate, pipeline_factory))
        self._counter += 1
        # Sort descending by priority, then ascending by insertion index (stable FIFO)
        self._rules.sort(key=lambda r: (-r[1], r[0]))

    def resolve(self, variable: Variable) -> MetadataBuilderPipeline:
        """Return the metadata pipeline for *variable* from the first matching rule.

        Raises
        ------
        ValueError
            If no registered rule matches the variable.
        """
        for _, _, predicate, factory in self._rules:
            if predicate(variable):
                return factory()
        raise ValueError(
            f"No rule matched variable '{variable.name}'. "
            "Register a catch-all rule (predicate=lambda v: True) as a fallback."
        )

    def resolve_all(self, dataset: VariablesDataset) -> dict[str, MetadataBuilderPipeline]:
        """Resolve every variable in *dataset.variables* and return a name→pipeline mapping."""
        return {v.name: self.resolve(v) for v in dataset.variables}
