from pyspark.sql.functions import col, concat, lit, regexp_replace, split, substring, when


PII_FIELDS = {"customer_name", "email"}


def mask_email(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip().lower()
    if "@" not in cleaned:
        return "***"

    local_part, domain = cleaned.split("@", 1)
    if not local_part or not domain:
        return "***"

    return f"{local_part[0]}***@{domain}"


def mask_name(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = " ".join(value.strip().split())
    if not cleaned:
        return ""

    return " ".join(f"{part[0]}***" for part in cleaned.split(" "))


def mask_customer_record(record: dict) -> dict:
    masked = dict(record)
    if "email" in masked:
        masked["email"] = mask_email(masked["email"])
    if "customer_name" in masked:
        masked["customer_name"] = mask_name(masked["customer_name"])
    return masked


def mask_customer_columns(dataframe):
    masked = dataframe
    if "email" in masked.columns:
        email_domain = split(col("email"), "@").getItem(1)
        masked = masked.withColumn(
            "email",
            when(col("email").isNull(), None)
            .when(
                col("email").contains("@"),
                concat(substring(col("email"), 1, 1), lit("***@"), email_domain),
            )
            .otherwise(lit("***")),
        )
    if "customer_name" in masked.columns:
        masked = masked.withColumn(
            "customer_name",
            when(col("customer_name").isNull(), None).otherwise(
                regexp_replace(col("customer_name"), r"(?<=\b\w)\w+", "***")
            ),
        )
    return masked
