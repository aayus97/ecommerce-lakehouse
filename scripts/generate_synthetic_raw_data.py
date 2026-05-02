"""Generate deterministic large raw CSV inputs for prod-like pipeline testing."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import random


COUNTRIES = (
    "France",
    "Germany",
    "Spain",
    "Italy",
    "Netherlands",
    "Belgium",
    "Portugal",
    "United Kingdom",
)
PRODUCT_CATEGORIES = (
    "Electronics",
    "Home Office",
    "Stationery",
    "Kitchen",
    "Sports",
    "Beauty",
    "Books",
    "Toys",
)
STATUSES = ("completed", "completed", "completed", "cancelled", "returned")
INVALID_STATUSES = ("pending_review", "lost", "unknown")


@dataclass(frozen=True)
class GenerationSummary:
    output_dir: Path
    orders_initial: int
    orders_batch_2: int
    order_updates: int
    order_new_records: int
    customers: int
    products: int
    invalid_orders_estimate: int


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def percentage(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 100:
        raise argparse.ArgumentTypeError("must be between 0 and 100")
    return parsed


def generate_raw_data(
    output_dir: Path,
    orders: int,
    customers: int,
    products: int,
    start_date: date,
    days: int,
    batch_ratio: float,
    update_ratio: float,
    invalid_rate: float,
    seed: int,
) -> GenerationSummary:
    if customers < 1 or products < 1 or orders < 1 or days < 1:
        raise ValueError("orders, customers, products, and days must be positive")
    if batch_ratio < 0 or batch_ratio >= 1:
        raise ValueError("batch_ratio must be at least 0 and less than 1")
    if update_ratio < 0 or update_ratio > 1:
        raise ValueError("update_ratio must be between 0 and 1")
    if invalid_rate < 0 or invalid_rate > 1:
        raise ValueError("invalid_rate must be between 0 and 1")

    output_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)
    customer_ids = list(range(100_001, 100_001 + customers))
    product_ids = list(range(200_001, 200_001 + products))

    _write_customers(output_dir / "customers.csv", customer_ids, start_date, rng)
    product_prices = _write_products(output_dir / "products.csv", product_ids, rng)

    batch_new_records = int(orders * batch_ratio)
    initial_records = orders - batch_new_records
    update_records = min(int(initial_records * update_ratio), initial_records)

    invalid_order_ids = _choose_invalid_order_ids(orders, invalid_rate, rng)
    _write_orders(
        output_dir / "orders.csv",
        order_ids=range(1, initial_records + 1),
        customer_ids=customer_ids,
        product_ids=product_ids,
        product_prices=product_prices,
        start_date=start_date,
        days=days,
        invalid_order_ids=invalid_order_ids,
        rng=rng,
    )

    update_ids = rng.sample(range(1, initial_records + 1), update_records)
    batch_2_ids = [*update_ids, *range(initial_records + 1, orders + 1)]
    _write_orders(
        output_dir / "orders_batch_2.csv",
        order_ids=batch_2_ids,
        customer_ids=customer_ids,
        product_ids=product_ids,
        product_prices=product_prices,
        start_date=start_date,
        days=days,
        invalid_order_ids=invalid_order_ids,
        rng=rng,
        price_adjustment=1.03,
    )

    _write_bad_batch(
        output_dir / "orders_bad_batch.csv",
        start_order_id=orders + 1,
        customer_ids=customer_ids,
        product_ids=product_ids,
        start_date=start_date,
    )

    return GenerationSummary(
        output_dir=output_dir,
        orders_initial=initial_records,
        orders_batch_2=len(batch_2_ids),
        order_updates=update_records,
        order_new_records=batch_new_records,
        customers=customers,
        products=products,
        invalid_orders_estimate=len(invalid_order_ids),
    )


def _write_customers(path: Path, customer_ids: list[int], start_date: date, rng) -> None:
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            ["customer_id", "customer_name", "email", "country", "signup_date"]
        )
        for customer_id in customer_ids:
            signup_date = start_date - timedelta(days=rng.randint(1, 730))
            writer.writerow(
                [
                    customer_id,
                    f"Customer {customer_id}",
                    f"customer{customer_id}@synthetic.example",
                    rng.choice(COUNTRIES),
                    signup_date.isoformat(),
                ]
            )


def _write_products(path: Path, product_ids: list[int], rng) -> dict[int, float]:
    prices = {}
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["product_id", "product_name", "category", "unit_cost"])
        for product_id in product_ids:
            unit_cost = round(rng.uniform(3.0, 450.0), 2)
            prices[product_id] = round(unit_cost * rng.uniform(1.2, 2.6), 2)
            writer.writerow(
                [
                    product_id,
                    f"Synthetic Product {product_id}",
                    rng.choice(PRODUCT_CATEGORIES),
                    f"{unit_cost:.2f}",
                ]
            )
    return prices


def _choose_invalid_order_ids(orders: int, invalid_rate: float, rng) -> set[int]:
    invalid_count = int(orders * invalid_rate)
    if invalid_count == 0:
        return set()
    return set(rng.sample(range(1, orders + 1), invalid_count))


def _write_orders(
    path: Path,
    order_ids,
    customer_ids: list[int],
    product_ids: list[int],
    product_prices: dict[int, float],
    start_date: date,
    days: int,
    invalid_order_ids: set[int],
    rng,
    price_adjustment: float = 1.0,
) -> None:
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "status",
            ]
        )
        for order_id in order_ids:
            product_id = rng.choice(product_ids)
            row = [
                order_id,
                rng.choice(customer_ids),
                product_id,
                (start_date + timedelta(days=rng.randrange(days))).isoformat(),
                rng.randint(1, 5),
                f"{product_prices[product_id] * price_adjustment:.2f}",
                rng.choice(STATUSES),
            ]
            if order_id in invalid_order_ids:
                _inject_invalid_value(row, customer_ids, product_ids, rng)
            writer.writerow(row)


def _inject_invalid_value(
    row: list[object],
    customer_ids: list[int],
    product_ids: list[int],
    rng,
) -> None:
    invalid_case = rng.choice(
        ("unknown_customer", "unknown_product", "quantity", "price", "status")
    )
    if invalid_case == "unknown_customer":
        row[1] = max(customer_ids) + 10_000
    elif invalid_case == "unknown_product":
        row[2] = max(product_ids) + 10_000
    elif invalid_case == "quantity":
        row[4] = 0
    elif invalid_case == "price":
        row[5] = "-1.00"
    elif invalid_case == "status":
        row[6] = rng.choice(INVALID_STATUSES)


def _write_bad_batch(
    path: Path,
    start_order_id: int,
    customer_ids: list[int],
    product_ids: list[int],
    start_date: date,
) -> None:
    with path.open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "status",
            ]
        )
        writer.writerow(
            [
                start_order_id,
                customer_ids[0],
                product_ids[0],
                start_date.isoformat(),
                0,
                "25.00",
                "completed",
            ]
        )
        writer.writerow(
            [
                start_order_id + 1,
                max(customer_ids) + 99_999,
                product_ids[0],
                start_date.isoformat(),
                1,
                "25.00",
                "completed",
            ]
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic prod-like raw CSV data under data/raw."
    )
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--orders", type=positive_int, default=100_000)
    parser.add_argument("--customers", type=positive_int, default=10_000)
    parser.add_argument("--products", type=positive_int, default=1_000)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--days", type=positive_int, default=90)
    parser.add_argument("--batch-ratio", type=percentage, default=20.0)
    parser.add_argument("--update-ratio", type=percentage, default=5.0)
    parser.add_argument("--invalid-rate", type=percentage, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = generate_raw_data(
        output_dir=args.output_dir,
        orders=args.orders,
        customers=args.customers,
        products=args.products,
        start_date=args.start_date,
        days=args.days,
        batch_ratio=args.batch_ratio / 100,
        update_ratio=args.update_ratio / 100,
        invalid_rate=args.invalid_rate / 100,
        seed=args.seed,
    )
    print(f"Synthetic raw data written to {summary.output_dir}")
    print(f"customers.csv rows: {summary.customers}")
    print(f"products.csv rows: {summary.products}")
    print(f"orders.csv rows: {summary.orders_initial}")
    print(
        "orders_batch_2.csv rows: "
        f"{summary.orders_batch_2} "
        f"({summary.order_new_records} new, {summary.order_updates} updates)"
    )
    print(f"approximate invalid unique orders: {summary.invalid_orders_estimate}")


if __name__ == "__main__":
    main()
