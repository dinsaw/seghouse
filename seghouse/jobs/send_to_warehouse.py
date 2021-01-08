import gzip
import json
import logging
from dataclasses import dataclass
from os import listdir
from os.path import isfile, join

import humps
import pandas as pd

from ..config import default_table_structure
from ..config import event_fields
from ..config.configuration import AppConf
from ..util import json_util, dataframe_util
from ..warehouse import factory as whf, warehouse as wh


@dataclass()
class EventDataFrames:
    """Class for keeping different types of dataframes."""

    tracks: pd.DataFrame
    identities: pd.DataFrame
    pages: pd.DataFrame
    screens: pd.DataFrame
    groups: pd.DataFrame
    aliases: pd.DataFrame

    def __post_init__(self):
        for df in [
            self.tracks,
            self.identities,
            self.pages,
            self.screens,
            self.groups,
            self.aliases,
        ]:
            if not dataframe_util.empty(df):
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["received_at"] = pd.to_datetime(df["received_at"])

        self.tracks["original_event"] = self.tracks["event"]
        self.tracks["event"] = self.tracks["event"].apply(
            lambda x: humps.decamelize(x.replace(" ", "")).lower()
        )

    def summary(self):
        return f"""
        tracks = {dataframe_util.row_count(self.tracks)}, 
        identities = {dataframe_util.row_count(self.identities)}, 
        pages = {dataframe_util.row_count(self.pages)}, 
        screens = {dataframe_util.row_count(self.screens)}, 
        groups = {dataframe_util.row_count(self.groups)}, 
        aliases = {dataframe_util.row_count(self.aliases)}"""

    def set_extra_timestamps(self, extra_timestamps: dict):
        for df in [
            self.tracks,
            self.identities,
            self.pages,
            self.screens,
            self.groups,
            self.aliases,
        ]:
            if not dataframe_util.empty(df):
                columns = df.columns.values
                for ts_name, tz in extra_timestamps.items():
                    if ts_name in columns:
                        raise Exception(f"Column with {ts_name} already exist")
                    logging.info(f"Creating new timestamp {ts_name} for zone {tz}")
                    df[ts_name] = df["timestamp"].dt.tz_convert(tz).dt.tz_localize(None)
                    # logging.info(f"{df['timestamp']}")
                    # logging.info(f"{df[ts_name]}")
                    # raise Exception("99")


class SendToWarehouseJob:
    """ Handles whole process to send files to warehouse """

    app_conf: AppConf
    source_dir: str
    warehouse_namespace: str
    warehouse_schema: str
    warehouses: list[wh.Warehouse]
    non_null_columns: list[str]

    def __init__(self, app_conf: AppConf, source_dir: str, warehouse_namespace: str):
        self.app_conf = app_conf
        self.source_dir = source_dir
        self.warehouse_namespace = warehouse_namespace
        self.warehouse_schema = humps.decamelize(self.warehouse_namespace)
        self.warehouses = []
        for warehouse_conf in app_conf.warehouses:
            self.warehouses.append(whf.get_warehouse(warehouse_conf))
        self.non_null_columns = [event_fields.RECEIVED_AT, event_fields.TIMESTAMP, event_fields.MESSAGE_ID] + list(
            self.app_conf.extra_timestamps.keys())

    def execute(self):
        file_names = [
            f for f in listdir(self.source_dir) if isfile(join(self.source_dir, f))
        ]
        file_paths = [self.source_dir + "/" + x for x in file_names]

        logging.info(f"Files to be sent to warehouses are : {file_paths}")

        self.process(file_paths)

    def process(self, file_paths):
        for file_path in file_paths:
            file_df = self.process_file(file_path)

            logging.info(f"Removing columns = {self.app_conf.skip_fields}")
            file_df = file_df.drop(columns=self.app_conf.skip_fields)

            event_data_frames = self.break_down_by_type(file_df)

            event_data_frames.set_extra_timestamps(self.app_conf.extra_timestamps)
            self.store(event_data_frames)
            self.clean_up()

    def store(self, event_data_frames: EventDataFrames):
        self.store_identities(event_data_frames.identities)
        self.store_tracks(event_data_frames.tracks)
        self.store_screens(event_data_frames.screens)
        self.store_pages(event_data_frames.pages)
        self.store_groups(event_data_frames.groups)
        self.store_aliases(event_data_frames.aliases)

    def clean_up(self):
        for warehouse in self.warehouses:
            warehouse.close()

    def store_identities(self, identities_df):
        if not dataframe_util.empty(identities_df):
            col_types = dataframe_util.get_datatypes(identities_df)
            logging.debug(f"Col, Types = {col_types}")

            identities_df = dataframe_util.mark_nan_to_none(identities_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "identities",
                default_table_structure.IDENTITIES,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "identities", identities_df)

            self.store_users(identities_df)

    def store_users(self, identities_df):
        users_df = identities_df.copy()
        users_df['ver'] = users_df['received_at'].astype(int)

        col_types = dataframe_util.get_datatypes(users_df)
        logging.debug(f"Col, Types = {col_types}")

        self.ensure_users_table_structure(
            self.warehouse_schema,
            default_table_structure.USERS,
            col_types,
        )
        for warehouse in self.warehouses:
            warehouse.insert_df(self.warehouse_schema, "users", users_df)

    def ensure_users_table_structure(self, schema, default_structure, col_types):
        table = "users"
        users_non_null_columns = self.non_null_columns + ['ver', 'user_id']
        logging.debug(f"default_structure = {default_structure}")
        for warehouse in self.warehouses:
            warehouse.create_schema(schema)
            warehouse.create_users_table(schema, default_structure, users_non_null_columns)

            table_col_types = warehouse.describe_table(schema, table)
            for col_name, col_type in col_types.items():
                if col_name not in table_col_types:
                    warehouse.add_column(schema, table, col_name, col_type, users_non_null_columns)

    def store_tracks(self, tracks_df):
        if not dataframe_util.empty(tracks_df):
            selected_col_df = self.select_columns(
                tracks_df,
                list(default_table_structure.TRACKS.keys()) + list(self.app_conf.extra_timestamps.keys()),
                default_table_structure.TRACKS_ALLOWED_FIELD_PREFIXES,
            )
            col_types = dataframe_util.get_datatypes(selected_col_df)
            logging.debug(f"Col, Types = {col_types}")

            selected_col_df = dataframe_util.mark_nan_to_none(selected_col_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "tracks",
                default_table_structure.TRACKS,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "tracks", selected_col_df)

            self.store_individual_events(tracks_df)

    def store_individual_events(self, tracks_df):
        all_events = sorted(tracks_df["event"].unique())
        for event in all_events:
            event_df = tracks_df[tracks_df["event"] == event].copy()
            event_col_types = dataframe_util.get_datatypes(event_df)
            logging.debug(f"Event = {event}, Col, Types = {event_col_types}")
            event_df = dataframe_util.mark_nan_to_none(event_df, event_col_types)
            self.ensure_table_structure(
                self.warehouse_schema,
                event,
                default_table_structure.TRACKS,
                event_col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, event, event_df)

    def store_screens(self, screens_df):
        if not dataframe_util.empty(screens_df):
            col_types = dataframe_util.get_datatypes(screens_df)
            logging.info(f"Col, Types = {col_types}")

            screens_df = dataframe_util.mark_nan_to_none(screens_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "screens",
                default_table_structure.SCREENS,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "identities", screens_df)

    def store_pages(self, pages_df):
        if not dataframe_util.empty(pages_df):
            col_types = dataframe_util.get_datatypes(pages_df)
            logging.info(f"Col, Types = {col_types}")

            pages_df = dataframe_util.mark_nan_to_none(pages_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "pages",
                default_table_structure.PAGES,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "identities", pages_df)

    def store_groups(self, groups_df):
        if not dataframe_util.empty(groups_df):
            col_types = dataframe_util.get_datatypes(groups_df)
            logging.debug(f"Col, Types = {col_types}")

            groups_df = dataframe_util.mark_nan_to_none(groups_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "groups",
                default_table_structure.GROUPS,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "identities", groups_df)

    def store_aliases(self, aliases_df):
        if not dataframe_util.empty(aliases_df):
            col_types = dataframe_util.get_datatypes(aliases_df)
            logging.debug(f"Col, Types = {col_types}")

            aliases_df = dataframe_util.mark_nan_to_none(aliases_df, col_types)

            self.ensure_table_structure(
                self.warehouse_schema,
                "aliases",
                default_table_structure.ALIAS,
                col_types,
            )
            for warehouse in self.warehouses:
                warehouse.insert_df(self.warehouse_schema, "identities", aliases_df)

    def ensure_table_structure(self, schema, table, default_structure, col_types):
        logging.debug(f"default_structure = {default_structure}")
        for warehouse in self.warehouses:
            warehouse.create_schema(schema)
            warehouse.create_table(schema, table, default_structure, self.non_null_columns)

            table_col_types = warehouse.describe_table(schema, table)
            for col_name, col_type in col_types.items():
                if col_name not in table_col_types:
                    warehouse.add_column(schema, table, col_name, col_type, self.non_null_columns)

    @staticmethod
    def select_columns(df, keep_columns, keep_columns_with_prefixes):
        col_names = df.columns.values
        selected_col_names = set()
        for col_name in col_names:
            if col_name in keep_columns:
                selected_col_names.add(col_name)

            if col_name.startswith(keep_columns_with_prefixes):
                selected_col_names.add(col_name)
        logging.debug(f"selected_col_names = {selected_col_names}")
        return df[selected_col_names]

    @staticmethod
    def process_file(file_path):
        data = []

        if file_path.endswith(".gz"):
            opener = gzip.open
        else:
            opener = open

        with opener(file_path, "r") as f:
            for line in f:
                event_json = json.loads(line)
                snake_cased_event_json = humps.decamelize(event_json)
                data.append(snake_cased_event_json)

        logging.debug(
            f"first 5 event json objects = {json.dumps(data[0:5], indent=4, default=str)}"
        )

        flattened_data = []
        for d in data:
            flattened_data.append(json_util.flatten_json(d))

        logging.debug(
            f"first 5 flattened event json objects = {json.dumps(flattened_data[0:5], indent=4, default=str)}"
        )

        df = pd.DataFrame(flattened_data)
        return df

    @staticmethod
    def break_down_by_type(df):
        logging.info(f"Type break_downs = {df.groupby(['type']).count()}")
        event_data_frames = EventDataFrames(
            tracks=df[df["type"] == "track"].copy(),
            identities=df[df["type"] == "identify"].copy(),
            pages=df[df["type"] == "page"].copy(),
            screens=df[df["type"] == "screen"].copy(),
            groups=df[df["type"] == "group"].copy(),
            aliases=df[df["type"] == "alias"].copy(),
        )
        logging.info(f"Event Data Frames Summary = {event_data_frames.summary()}")
        return event_data_frames
