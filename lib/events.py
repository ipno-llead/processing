from datetime import datetime

import pandas as pd
from pandas.api.types import CategoricalDtype

from lib.clean import clean_date, clean_datetime, float_to_int_str
from lib.uid import gen_uid, ensure_uid_unique
from lib.exceptions import InvalidEventKindException, InvalidEventDateException
from lib.columns import rearrange_event_columns

OFFICER_LEVEL_1_CERT = "officer_level_1_cert"
OFFICER_PC_12_QUALIFICATION = "officer_pc_12_qualification"
OFFICER_RANK = "officer_rank"
OFFICER_DEPT = "officer_dept"
OFFICER_HIRE = "officer_hire"
OFFICER_LEFT = "officer_left"
OFFICER_PAY_PROG_START = "officer_pay_prog_start"
OFFICER_PAY_EFFECTIVE = "officer_pay_effective"

COMPLAINT_INCIDENT = "complaint_incident"
COMPLAINT_RECEIVE = "complaint_receive"
ALLEGATION_CREATE = "allegation_create"
INVESTIGATION_COMPLETE = "investigation_complete"
SUSPENSION_START = "suspension_start"
SUSPENSION_END = "suspension_end"

APPEAL_FILE = "appeal_file"
APPEAL_HEARING = "appeal_hearing"
APPEAL_RENDER = "appeal_render"

UOF_INCIDENT = "uof_incident"
UOF_RECEIVE = "uof_receive"
UOF_ASSIGNED = "uof_assigned"
UOF_COMPLETED = "uof_completed"
UOF_CREATED = "uof_created"
UOF_DUE = "uof_due"


cat_type = CategoricalDtype(categories=[
    OFFICER_LEVEL_1_CERT,
    OFFICER_PC_12_QUALIFICATION,
    OFFICER_RANK,
    OFFICER_DEPT,
    OFFICER_HIRE,
    OFFICER_LEFT,
    OFFICER_PAY_PROG_START,
    OFFICER_PAY_EFFECTIVE,
    COMPLAINT_INCIDENT,
    COMPLAINT_RECEIVE,
    ALLEGATION_CREATE,
    INVESTIGATION_COMPLETE,
    SUSPENSION_START,
    SUSPENSION_END,
    APPEAL_FILE,
    APPEAL_HEARING,
    APPEAL_RENDER,
    UOF_INCIDENT,
    UOF_RECEIVE,
    UOF_ASSIGNED,
    UOF_COMPLETED,
    UOF_CREATED,
    UOF_DUE,
], ordered=True)


class Builder(object):
    """
    Builder build a DataFrame out of event records.

    Methods
    -------
    append(parse_date=None, parse_datetime=None,
           strptime_format=None, ignore_bad_date=False, **kwargs)
        Append an event and optionally parse datetime of the event

    to_frame()
        Create a DataFrame out of collected events
    """

    def __init__(self):
        self._records = []

    def _extract_date(self, fields, raw_date, strptime_format=None):
        if strptime_format is not None:
            dt = datetime.strptime(raw_date, strptime_format)
            fields["year"] = dt.year
            fields["month"] = dt.month
            fields["day"] = dt.day
        else:
            fields["year"], fields["month"], fields["day"] = clean_date(
                raw_date)
        fields["raw_date"] = raw_date

    def _extract_datetime(self, fields, raw_datetime, strptime_format=None):
        if strptime_format is not None:
            dt = datetime.strptime(raw_datetime, strptime_format)
            fields["year"] = dt.year
            fields["month"] = dt.month
            fields["day"] = dt.day
            fields["time"] = dt.strftime("%H:%M")
        else:
            fields["year"], fields["month"], fields["day"], fields["time"] = clean_datetime(
                raw_datetime)
        fields["raw_date"] = raw_datetime

    def append(
            self, event_kind, raw_date_str=None, raw_datetime_str=None,
            strptime_format=None, ignore_bad_date=False, **kwargs):
        """Append an event and optionally parse datetime of the event.

        If `raw_date_str` is passed then Builder try to extract `year`, `month`, `day` from it. If `raw_datetime_str`
        is passed instead then Builder try to extract `year`, `month`, `day` and `time` from it. If `strptime_format`
        is passed along with `raw_date_str` or `raw_datetime_str` then the date is parsed with that format. By default
        if `year` isn't passed-in or can't be extracted from `raw_date_str` or `raw_datetime_str` then an error is
        raised. If `ignore_bad_date` is True then an error isn't raised and the event is simply ignored. Any other
        keyword arguments passed to this method will become a column in the final DataFrame.

        Parameters
        ----------
        event_kind : str
            Kind of event
        raw_date_str : str, optional
            Raw date string to extract `year`, `month` and `day` from
        raw_datetime_str : str, optional
            Raw datetime string to extract `year`, `month`, `day` and `time` from
        strptime_format : str, optional
            Format string to extract datetime with.
        ignore_bad_date : bool, optional
            If True then ignore events with bad date instead of raising error

        Raises
        ------
        InvalidEventKindException
            If `event_kind` is invalid
        InvalidEventDateException
            If `year` isn't passed in or can't be extracted from raw date string.
        """
        if event_kind not in cat_type.categories:
            raise InvalidEventKindException(event_kind)
        kwargs["kind"] = event_kind
        if raw_date_str is not None:
            self._extract_date(kwargs, raw_date_str, strptime_format)
        elif raw_datetime_str is not None:
            self._extract_datetime(kwargs, raw_datetime_str, strptime_format)
        if pd.isnull(kwargs["year"]) or kwargs["year"] == "":
            if ignore_bad_date:
                return
            raise InvalidEventDateException(
                "year column cannot be empty:\n\t%s" % kwargs)
        self._records.append(kwargs)

    def _assign_kwargs_func(self, cols, kwargs_funcs, flatten_date_cols, kind, obj):
        if "parse_date" in obj:
            col = "%s_date" % obj["prefix"]
            strptime_format = None if obj["parse_date"] is True else obj["parse_date"]
            kwargs_funcs[kind] = lambda row: [
                ("raw_date_str", row[col]),
                ("strptime_format", strptime_format)
            ]
            flatten_date_cols.append(col)
        elif "parse_datetime" in obj:
            col = "%s_datetime" % obj["prefix"]
            strptime_format = None if obj["parse_datetime"] is True else obj["parse_datetime"]
            kwargs_funcs[kind] = lambda row: [
                ("raw_datetime_str", row[col]),
                ("strptime_format", strptime_format)
            ]
            flatten_date_cols.append(col)
        else:
            col_pairs = []
            for event_col in ['year', 'month', 'day', 'time', 'raw_date']:
                col = '%s_%s' % (obj["prefix"], event_col)
                if col in cols:
                    col_pairs.append((col, event_col))
                    flatten_date_cols.append(col)

            kwargs_funcs[kind] = lambda row: [
                (event_col, row[col]) for col, event_col in col_pairs
            ]

    def extract_events(self, df, event_dict):
        """Extract event records from a DataFrame.

        Multiple kinds of event can be extracted. Each defined as a single key in `event_dict`.
        Each value in `event_dict` is prefix of columns to extract.

        Parameters
        ----------
        df
            DataFrame to extract events from
        event_dict
            Dictionary of event kinds to extract. E.g.:
            {
                events.COMPLAINT_INCIDENT: "occur",
                events.COMPLAINT_RECEIVE: "receive",
                ...
            }
        """
        cols = set(df.columns)
        kwargs_funcs = dict()
        flatten_date_cols = []
        for kind, obj in event_dict.items():
            self._assign_kwargs_func(
                cols, kwargs_funcs, flatten_date_cols, kind, obj)

        for idx, row in df.iterrows():
            common_fields = row.drop(flatten_date_cols).to_dict()
            for kind, obj in event_dict.items():
                if "parse_date" in obj:
                    anchor_col = '%s_date' % obj['prefix']
                elif "parse_datetime" in obj:
                    anchor_col = '%s_datetime' % obj['prefix']
                else:
                    anchor_col = '%s_year' % obj['prefix']
                if row[anchor_col] == '' or pd.isnull(row[anchor_col]):
                    continue
                if 'keep' in obj:
                    fields = dict(
                        [(k, v) for k, v in common_fields.items() if k in obj['keep']])
                elif 'drop' in obj:
                    fields = dict(
                        [(k, v) for k, v in common_fields.items() if k not in obj['drop']])
                else:
                    fields = dict(list(common_fields.items()))
                fields = dict(
                    list(fields.items()) + kwargs_funcs[kind](row))
                self.append(kind, **fields)

    def to_frame(self, id_cols, output_duplicated_events=False):
        """Create a DataFrame out of collected events.

        This also ensure all event kinds are valid and generate `event_uid` column from `id_cols`

        Parameters
        ----------
        id_cols
            List of columns to generate `event_uid` with.

        Raises
        ------
        NonUniqueEventIDException
            If event_uid is not unique.
        """
        df = pd.DataFrame.from_records(self._records)\
            .pipe(float_to_int_str, ["year", "month", "day"])\
            .pipe(gen_uid, id_cols, "event_uid")
        df.loc[:, 'kind'] = df.kind.astype(cat_type)
        df = rearrange_event_columns(df)
        ensure_uid_unique(df, 'event_uid', output_duplicated_events)
        return df
