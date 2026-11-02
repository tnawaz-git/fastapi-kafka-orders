import pytest
from pydantic import ValidationError

from app.schemas import OrderCreate


def make(**overrides) -> OrderCreate:
    return OrderCreate(**{"customer_email": "a@b.co", "sku": "SKU-MOUSE", "quantity": 1, **overrides})


@pytest.mark.parametrize("quantity", [1, 11, 1_000_000])
def test_quantity_accepted(quantity):
    assert make(quantity=quantity).quantity == quantity


@pytest.mark.parametrize("quantity", [0, -1, 1_000_001, 10**23])
def test_quantity_rejected(quantity):
    with pytest.raises(ValidationError):
        make(quantity=quantity)


def test_unicode_email_accepted():
    assert make(customer_email="josé@exämple.com").customer_email == "josé@exämple.com"
