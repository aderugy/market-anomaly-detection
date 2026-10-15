import pytest

from market_anomaly_detection.core.devig import devig_multiplicative, margin


def test_probabilities_sum_to_one():
    p = devig_multiplicative([2.10, 3.40, 3.60])
    assert sum(p) == pytest.approx(1.0)


def test_margin_example():
    assert margin([2.10, 3.40, 3.60]) == pytest.approx(0.0479, abs=1e-3)


def test_favourite_keeps_highest_probability():
    p = devig_multiplicative([1.50, 4.00, 6.50])
    assert p[0] == max(p)


def test_rejects_invalid_odds():
    with pytest.raises(ValueError):
        devig_multiplicative([1.0, 2.0])
