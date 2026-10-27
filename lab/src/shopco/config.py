"""Scale ladder, dataset constants and path layout.

Everything that decides "how big" or "where" lives here, so the pipeline code
never hard-codes a size or a bucket. See local/adr/0001 for the scale ladder.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class Scale:
    name: str
    days: int
    events_per_day: int
    customers: int
    products: int
    gen_partitions: int  # partitions used while generating one day of events

    @property
    def events(self) -> int:
        return self.days * self.events_per_day

    @property
    def cutoff_date(self) -> dt.date:
        """Features use data strictly before this date; the rest is the label window."""
        feature_days = int(self.days * CUTOFF_FRACTION)
        feature_days = max(1, min(self.days - 1, feature_days))
        return START_DATE + dt.timedelta(days=feature_days)

    @property
    def end_date(self) -> dt.date:
        return START_DATE + dt.timedelta(days=self.days)


# ShopCo production is ~2B events/month (~67M/day). The ladder below is
# "same shape, different size": every rung has junk keys, a bot, skewed users
# and skewed products, so the lessons (shuffle, skew, broadcast) show up at
# every size.
SCALES: dict[str, Scale] = {
    # pytest: seconds
    "test": Scale("test", days=2, events_per_day=50_000, customers=5_000, products=500, gen_partitions=2),
    # first run on a laptop: ~1 minute
    "smoke": Scale("smoke", days=2, events_per_day=1_000_000, customers=100_000, products=5_000, gen_partitions=8),
    # "huge" for a laptop: 500M events (~10 ShopCo days at half volume), ~6 GB of Parquet,
    # ~30 GB if pandas tried to hold it. Measured: 60M events took 17 s end to end on 12 cores.
    "local": Scale("local", days=10, events_per_day=50_000_000, customers=5_000_000, products=50_000, gen_partitions=48),
    # the real thing: one ShopCo month, run on Dataproc Serverless only
    "cloud": Scale("cloud", days=30, events_per_day=67_000_000, customers=20_000_000, products=50_000, gen_partitions=256),
}

START_DATE = dt.date(2026, 9, 1)
CUTOFF_FRACTION = 2 / 3  # first two thirds of the days feed features; the last third is the label window

# Junk keys that create the skew story from the course's M3
BOT_USER_ID = 0  # real customers are 1..N; the load-test bot is user 0
NULL_USER_FRACTION = 0.005  # anonymous events
BOT_EVENT_FRACTION = 0.03  # 3% of all events come from one key

EVENT_TYPE_THRESHOLDS = (("purchase", 0.05), ("cart", 0.15))  # else "view"

# (country, share, region). Shares sum to 1.0; the US is deliberately 40% (M4's per-country exercise).
COUNTRIES = (
    ("US", 0.40, "AMER"),
    ("IN", 0.12, "APAC"),
    ("GB", 0.08, "EMEA"),
    ("DE", 0.07, "EMEA"),
    ("BR", 0.06, "AMER"),
    ("CA", 0.05, "AMER"),
    ("FR", 0.05, "EMEA"),
    ("JP", 0.04, "APAC"),
    ("AU", 0.04, "APAC"),
    ("MX", 0.03, "AMER"),
    ("NL", 0.02, "EMEA"),
    ("SE", 0.02, "EMEA"),
    ("ZA", 0.02, "EMEA"),
)
N_CATEGORIES = 20


@dataclass(frozen=True)
class Paths:
    """Where data and run artefacts live. Works for local dirs and gs:// URIs alike."""

    data_root: str
    runs_root: str

    def _d(self, name: str) -> str:
        return f"{self.data_root.rstrip('/')}/{name}"

    @property
    def events(self) -> str:
        return self._d("events")

    @property
    def customers(self) -> str:
        return self._d("customers")

    @property
    def products(self) -> str:
        return self._d("products")

    @property
    def features(self) -> str:
        return self._d("customer_features")

    @property
    def training_set(self) -> str:
        return self._d("training_set")

    @property
    def tmp(self) -> str:
        return self._d("tmp")

    def run_dir(self, run_id: str) -> str:
        return f"{self.runs_root.rstrip('/')}/{run_id}"
