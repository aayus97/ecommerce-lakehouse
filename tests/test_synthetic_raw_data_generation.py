import csv
from datetime import date

from scripts.generate_synthetic_raw_data import generate_raw_data


def read_rows(path):
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def test_generate_raw_data_writes_expected_files_and_counts(tmp_path):
    summary = generate_raw_data(
        output_dir=tmp_path,
        orders=100,
        customers=10,
        products=5,
        start_date=date(2026, 1, 1),
        days=10,
        batch_ratio=0.2,
        update_ratio=0.1,
        invalid_rate=0.05,
        seed=7,
    )

    customers = read_rows(tmp_path / "customers.csv")
    products = read_rows(tmp_path / "products.csv")
    orders = read_rows(tmp_path / "orders.csv")
    batch_2 = read_rows(tmp_path / "orders_batch_2.csv")
    bad_batch = read_rows(tmp_path / "orders_bad_batch.csv")

    assert summary.customers == 10
    assert summary.products == 5
    assert summary.orders_initial == 80
    assert summary.orders_batch_2 == 28
    assert summary.order_new_records == 20
    assert summary.order_updates == 8
    assert len(customers) == 10
    assert len(products) == 5
    assert len(orders) == 80
    assert len(batch_2) == 28
    assert len(bad_batch) == 2


def test_generate_raw_data_is_deterministic_for_same_seed(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"

    kwargs = {
        "orders": 20,
        "customers": 4,
        "products": 3,
        "start_date": date(2026, 1, 1),
        "days": 5,
        "batch_ratio": 0.25,
        "update_ratio": 0.2,
        "invalid_rate": 0.1,
        "seed": 99,
    }
    generate_raw_data(output_dir=first, **kwargs)
    generate_raw_data(output_dir=second, **kwargs)

    for file_name in (
        "customers.csv",
        "products.csv",
        "orders.csv",
        "orders_batch_2.csv",
        "orders_bad_batch.csv",
    ):
        assert (first / file_name).read_text() == (second / file_name).read_text()
