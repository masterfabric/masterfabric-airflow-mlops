"""Feature definitions: one entity (subscriber), one monthly feature view, one feature service per model."""
from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, ValueType
from feast.types import Float64, Int64, String

subscriber = Entity(name="subscriber", join_keys=["subscriber_id"], value_type=ValueType.STRING,
                    description="A mobile subscriber (synthetic CRM id)")

# One row per subscriber and month, stamped with the first day of the month (UTC): the state the source
# system reported for that month. Written by mlops/features.py from the landed snapshots that passed the gate.
monthly_source = FileSource(
    name="subscriber_monthly_snapshots",
    path="data/subscriber_features.parquet",
    timestamp_field="event_timestamp",
)

subscriber_monthly = FeatureView(
    name="subscriber_monthly",
    entities=[subscriber],
    ttl=timedelta(days=31),          # a subscriber missing from a month must not inherit last month's values
    schema=[
        Field(name="tenure_months", dtype=Int64),
        Field(name="monthly_fee", dtype=Float64),
        Field(name="data_gb", dtype=Float64),
        Field(name="voice_min", dtype=Float64),
        Field(name="dropped_call_rate", dtype=Float64),
        Field(name="complaints_90d", dtype=Int64),
        Field(name="support_calls_30d", dtype=Int64),
        Field(name="payment_delay_days", dtype=Int64),
        Field(name="contract_months_left", dtype=Int64),
        Field(name="device_age_months", dtype=Int64),
        Field(name="family_lines", dtype=Int64),
        Field(name="discount_pct", dtype=Int64),
        Field(name="nps", dtype=Float64),
        Field(name="plan", dtype=String),
        Field(name="region", dtype=String),
        Field(name="device_5g", dtype=Int64),
    ],
    online=True,
    source=monthly_source,
    tags={"owner": "mlops-lab", "source": "landing/subscribers"},
)

churn_model = FeatureService(name="churn_model", features=[subscriber_monthly],
                             description="The 16 inputs of the churn classifier")
