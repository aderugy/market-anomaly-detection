"""Turn decimal odds into probabilities by removing the bookmaker margin."""

from collections.abc import Sequence


def implied_probabilities(odds: Sequence[float]) -> list[float]:
    """Raw implied probabilities 1/odds (they sum to more than 1: the margin)."""
    if any(o <= 1 for o in odds):
        raise ValueError(f"decimal odds must be > 1, got {list(odds)}")
    return [1 / o for o in odds]


def margin(odds: Sequence[float]) -> float:
    """Bookmaker margin: sum of implied probabilities minus 1."""
    return sum(implied_probabilities(odds)) - 1


def devig_multiplicative(odds: Sequence[float]) -> list[float]:
    """Fair probabilities, assuming the margin is spread proportionally to each probability."""
    q = implied_probabilities(odds)
    s = sum(q)
    return [x / s for x in q]
