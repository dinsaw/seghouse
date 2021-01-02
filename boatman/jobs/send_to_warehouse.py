import gzip
import json
import humps
from ..util import json_util
import pandas as pd
from ..config.configuration import BoatmanConf
from ..warehouse import factory as whf, warehouse as wh
import logging
from os import listdir
from os.path import isfile, join
from dataclasses import dataclass, field


@dataclass()
class EventDataFrames:
    """Class for keeping different types of dataframes."""

    tracks: pd.DataFrame
    identities: pd.DataFrame
    pages: pd.DataFrame
    screens: pd.DataFrame
    groups: pd.DataFrame
    aliases: pd.DataFrame

    def summary(self):
        return f"""
        tracks = {len(self.tracks.index)}, 
        identities = {len(self.identities.index)}, 
        pages = {len(self.pages.index)}, 
        screens = {len(self.screens.index)}, 
        groups = {len(self.groups.index)}, 
        aliases = {len(self.aliases.index)}"""



class SendToWarehouseJob:
    """ Handles whole process to send files to warehouse """

    boatman_conf: BoatmanConf
    source_dir: str
    app: str
    warehouse_schema: str
    warehouses: list[wh.Warehouse]

    def __init__(self, boatman_conf: BoatmanConf, source_dir: str, app: str):
        self.boatman_conf = boatman_conf
        self.source_dir = source_dir
        self.app = app
        self.warehouse_schema = 'clickstream_' + self.app
        self.warehouses = []
        for warehouse_conf in boatman_conf.warehouses:
            self.warehouses.append(whf.get_warehouse(warehouse_conf))

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
            event_data_frames = self.break_down_by_type(file_df)
            store(event_data_frames)

    def store(event_data_frames: EventDataFrames):
        pass

    def process_file(self, file_path):
        data = []

        if file_path.endswith('.gz'):
            opener = gzip.open
        else:
            opener = open

        with opener(file_path, "r") as f:
            for line in f:
                event_json = json.loads(line)
                snake_cased_event_json = humps.decamelize(event_json)
                data.append(snake_cased_event_json)

        logging.info(
            f"first 5 event json objects = {json.dumps(data[0:5], indent=4, default=str)}"
        )

        flattened_data = []
        for d in data:
            flattened_data.append(json_util.flatten_json(d))

        logging.info(
            f"first 5 flattened event json objects = {json.dumps(flattened_data[0:5], indent=4, default=str)}"
        )

        df = pd.DataFrame(flattened_data)
        return df

    def break_down_by_type(self, df):
        logging.info(f"Type break_downs = {df.groupby(['type']).count()}")
        event_data_frames = EventDataFrames(
            tracks=df[df["type"] == "track"],
            identities=df[df["type"] == "identify"],
            pages=df[df["type"] == "page"],
            screens=df[df["type"] == "screen"],
            groups=df[df["type"] == "group"],
            aliases=df[df["type"] == "alias"],
        )
        logging.info(f"Event Data Frames Summary = {event_data_frames.summary()}")
        return event_data_frames
