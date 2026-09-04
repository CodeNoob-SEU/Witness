"""Golden runtime-debug target with a value-flow bug hidden behind feature flags."""

from __future__ import annotations

from typing import TypedDict


class LineItem(TypedDict):
    sku: str
    amount: float
    feature: str


class Order(TypedDict):
    order_id: str
    items: list[LineItem]


GOLDEN_ORDER: Order = {
    "order_id": "order-golden-001",
    "items": [
        {"sku": "MRI-SEQUENCE", "amount": 99.0, "feature": "bill_research"},
    ],
}
GOLDEN_FEATURE_FLAGS = {"bill_research": False}


def unit_price(subtotal: float, item_count: int) -> float:
    """Return the average price; the caller incorrectly permits zero items."""

    return subtotal / item_count


def price_order(order: Order, feature_flags: dict[str, bool]) -> float:
    """Price only enabled items, reproducing the intentional divide-by-zero bug."""

    billable_items = [
        item for item in order["items"] if feature_flags.get(item["feature"], False)
    ]
    subtotal = sum(item["amount"] for item in order["items"])
    item_count = len(billable_items)
    return unit_price(subtotal, item_count)


def reproduce() -> float:
    """Run the deterministic golden failure used by the DAP demo."""

    return price_order(GOLDEN_ORDER, GOLDEN_FEATURE_FLAGS)


if __name__ == "__main__":
    reproduce()
