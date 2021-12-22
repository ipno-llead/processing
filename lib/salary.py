from pandas.api.types import CategoricalDtype

HOURLY = "hourly"
DAILY = "daily"
WEEKLY = "weekly"
BIWEEKLY = "bi-weekly"
MONTHLY = "monthly"
YEARLY = "yearly"


cat_type = CategoricalDtype(
    categories=[HOURLY, DAILY, WEEKLY, BIWEEKLY, MONTHLY, YEARLY], ordered=True
)
