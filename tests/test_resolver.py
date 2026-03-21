"""Tests for CheckSuiteResolver (plan 7)."""

from __future__ import annotations

import pytest

from dqf.checks.pipeline import CheckPipeline
from dqf.enums import DataType
from dqf.resolver import CheckSuiteResolver
from dqf.variable import Variable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_variable(name: str = "x", **kwargs: object) -> Variable:
    defaults: dict[str, object] = {"name": name, "dtype": DataType.NUMERIC_CONTINUOUS}
    defaults.update(kwargs)
    return Variable(**defaults)  # type: ignore[arg-type]


def make_pipeline(tag: str = "default") -> CheckPipeline:
    """Return a tagged CheckPipeline (tag stored in a custom attribute for assertions)."""
    p = CheckPipeline([])
    p._tag = tag  # type: ignore[attr-defined]
    return p


def always_true(_: Variable) -> bool:
    return True


def always_false(_: Variable) -> bool:
    return False


# ---------------------------------------------------------------------------
# TestCheckSuiteResolverRegisterAndResolve
# ---------------------------------------------------------------------------


class TestCheckSuiteResolverRegisterAndResolve:
    def test_resolve_single_rule_matches(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline("a"))
        result = r.resolve(make_variable())
        assert isinstance(result, CheckPipeline)

    def test_resolve_raises_value_error_when_no_match(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_false, lambda: make_pipeline())
        with pytest.raises(ValueError):
            r.resolve(make_variable())

    def test_resolve_error_message_contains_variable_name(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_false, lambda: make_pipeline())
        with pytest.raises(ValueError, match="my_var"):
            r.resolve(make_variable("my_var"))

    def test_resolve_empty_resolver_raises_value_error(self) -> None:
        r = CheckSuiteResolver()
        with pytest.raises(ValueError):
            r.resolve(make_variable())

    def test_resolve_returns_fresh_pipeline_per_call(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline())
        p1 = r.resolve(make_variable())
        p2 = r.resolve(make_variable())
        assert p1 is not p2


# ---------------------------------------------------------------------------
# TestCheckSuiteResolverPriority
# ---------------------------------------------------------------------------


class TestCheckSuiteResolverPriority:
    def test_higher_priority_wins(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline("low"), priority=0)
        r.register(always_true, lambda: make_pipeline("high"), priority=10)
        result = r.resolve(make_variable())
        assert result._tag == "high"  # type: ignore[attr-defined]

    def test_lower_priority_is_fallback(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline("fallback"), priority=0)
        r.register(always_false, lambda: make_pipeline("specific"), priority=10)
        result = r.resolve(make_variable())
        assert result._tag == "fallback"  # type: ignore[attr-defined]

    def test_equal_priority_preserves_registration_order(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline("first"), priority=5)
        r.register(always_true, lambda: make_pipeline("second"), priority=5)
        result = r.resolve(make_variable())
        assert result._tag == "first"  # type: ignore[attr-defined]

    def test_priority_ordering_independent_of_registration_order(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline("low"), priority=0)
        r.register(always_true, lambda: make_pipeline("high"), priority=100)
        result = r.resolve(make_variable())
        assert result._tag == "high"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# TestCheckSuiteResolverResolveAll
# ---------------------------------------------------------------------------


class TestCheckSuiteResolverResolveAll:
    def test_resolve_all_empty_list(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline())
        assert r.resolve_all([]) == {}

    def test_resolve_all_single_variable(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline())
        v = make_variable("score")
        result = r.resolve_all([v])
        assert "score" in result
        assert isinstance(result["score"], CheckPipeline)

    def test_resolve_all_multiple_variables(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline())
        variables = [make_variable(f"v{i}") for i in range(3)]
        result = r.resolve_all(variables)
        assert len(result) == 3

    def test_resolve_all_keys_are_variable_names(self) -> None:
        r = CheckSuiteResolver()
        r.register(always_true, lambda: make_pipeline())
        variables = [make_variable("alpha"), make_variable("beta"), make_variable("gamma")]
        result = r.resolve_all(variables)
        assert set(result.keys()) == {"alpha", "beta", "gamma"}

    def test_resolve_all_raises_if_any_variable_unmatched(self) -> None:
        r = CheckSuiteResolver()
        # Only matches variables named "known"
        r.register(lambda v: v.name == "known", lambda: make_pipeline())
        variables = [make_variable("known"), make_variable("unknown")]
        with pytest.raises(ValueError, match="unknown"):
            r.resolve_all(variables)


# ---------------------------------------------------------------------------
# TestCheckSuiteResolverPredicateReceivesVariable
# ---------------------------------------------------------------------------


class TestCheckSuiteResolverPredicateReceivesVariable:
    def test_predicate_receives_correct_variable(self) -> None:
        received: list[Variable] = []

        def capturing_predicate(v: Variable) -> bool:
            received.append(v)
            return True

        r = CheckSuiteResolver()
        r.register(capturing_predicate, lambda: make_pipeline())
        v = make_variable("target")
        r.resolve(v)
        assert received[0] is v

    def test_predicate_can_inspect_metadata(self) -> None:
        r = CheckSuiteResolver()
        r.register(
            lambda v: v.metadata.get("key") == "numeric",
            lambda: make_pipeline("numeric_pipeline"),
        )
        r.register(always_true, lambda: make_pipeline("fallback"), priority=-1)

        v_numeric = make_variable("num")
        v_numeric.metadata["key"] = "numeric"

        v_other = make_variable("other")

        assert r.resolve(v_numeric)._tag == "numeric_pipeline"  # type: ignore[attr-defined]
        assert r.resolve(v_other)._tag == "fallback"  # type: ignore[attr-defined]
