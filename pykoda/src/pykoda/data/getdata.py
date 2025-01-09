import gzip
import json
import tarfile
import warnings
import datetime
import contextlib
import requests
import os
import subprocess
import time

import numpy as np

import ey
from google.protobuf import json_format
import pandas as pd
from joblib import Parallel, delayed

import tqdm  # 循环进度条
import shutil

from .. import config
from . import gtfs_realtime_pb2


def _is_json(series: pd.Series) -> bool:
    try:
        return isinstance(series[0][0], dict)
    except TypeError:
        return False


def unpack_jsons(df: pd.DataFrame) -> pd.DataFrame:
    keys_to_sanitise = []
    for k in list(df.keys()):
        # If the content is a json, unpack and remove
        if df[k].dtype == np.dtype('O') and _is_json(df[k]):
            keys_to_sanitise.append(k)

    if keys_to_sanitise:
        indexes = []
        unpacked = {k: [] for k in keys_to_sanitise}
        for ix in df.index:
            for k in keys_to_sanitise:
                this_unpack = pd.json_normalize(df[k][ix])
                unpacked[k].append(this_unpack)
                indexes.extend(ix for _ in range(len(this_unpack)))

        df.drop(keys_to_sanitise, axis='columns', inplace=True)

        unpacked_series = []
        for k in keys_to_sanitise:
            this_df = pd.concat(unpacked[k], axis='index').reset_index(drop=True)
            this_df.rename(columns={curr_name: '_'.join((k, curr_name)) for curr_name in this_df.keys()},
                           inplace=True)
            unpacked_series.append(this_df)

        repeated = df.iloc[indexes].reset_index(drop=True)
        df = pd.concat([repeated] + unpacked_series, axis='columns')

    for k in df.keys():
        if df[k].dtype == np.dtype('O') and _is_json(df[k]):
            warnings.warn(RuntimeWarning(f'There are extra json in column {k}'))
    return df


def _get_data_path(company: str, feed: str, date: str, hour: (int, str)) -> str:  # 缓存文件的路径
    return os.path.join(config.CACHE_DIR, f"{company}_{feed}_{date.replace('-', '_')}_{hour}.feather")


def _parse_gtfs(gtfsrt: bytes) -> pd.DataFrame:  # 读入GTFS处理为json格式
    # Read in to a FeedMessage class, of the GTFS-RT format
    msg = gtfs_realtime_pb2.FeedMessage()
    # pbfile = gzip.decompress(gtfsrt)
    pbfile = gtfsrt
    msg.ParseFromString(pbfile)

    msg_json = json_format.MessageToJson(msg)
    msg_dict = json.loads(msg_json)
    df = pd.json_normalize(msg_dict.get('entity', dict()), sep='_')
    df = unpack_jsons(df)
    df.reset_index(drop=True, inplace=True)
    return df


def normalize_keys(df: pd.DataFrame) -> None:   # 把GTFS的键名映射为一致的名称
    """Reformat the name of the keys to a consistent format, according to GTFS"""
    renames = {'tripUpdate_trip_tripId': 'trip_id', 'tripUpdate_trip_startDate': 'start_date',
               'tripUpdate_trip_directionId': 'direction_id', 'tripUpdate_trip_routeId': 'route_id',
               'tripUpdate_trip_scheduleRelationship': 'schedule_relationship',
               'tripUpdate_trip_startTime': 'start_time',
               'tripUpdate_timestamp': 'timestamp', 'tripUpdate_vehicle_id': 'vehicle_id',
               'stopSequence': 'stop_sequence', 'stopId': 'stop_id',
               'scheduleRelationship': 'schedule_relationship2',
               'vehicle_trip_tripId': 'trip_id', 'vehicle_trip_scheduleRelationship': 'schedule_relationship',
               'vehicle_timestamp': 'timestamp', 'vehicle_vehicle_id': 'vehicle_id',
               'vehicle_trip_startTime': 'start_time', 'vehicle_trip_startDate': 'start_date',
               'vehicle_trip_routeId': 'route_id', 'vehicle_trip_directionId': 'direction_id',
               'tripUpdate_stopTimeUpdate_stopSequence': 'stop_sequence',
               'tripUpdate_stopTimeUpdate_stopId': 'stop_id',
               'tripUpdate_stopTimeUpdate_arrival_delay': 'arrival_delay',
               'tripUpdate_stopTimeUpdate_arrival_time': 'arrival_time',
               'tripUpdate_stopTimeUpdate_departure_delay': 'departure_delay',
               'tripUpdate_stopTimeUpdate_departure_time': 'departure_time',
               'tripUpdate_stopTimeUpdate_arrival_uncertainty': 'arrival_uncertainty',
               'tripUpdate_stopTimeUpdate_departure_uncertainty': 'departure_uncertainty',
               'alert_activePeriod_start': 'period_start', 'alert_activePeriod_end': 'period_end',
               'alert_informedEntity_routeId': 'route_id', 'alert_informedEntity_stopId': 'stop_id',
               'alert_informedEntity_trip_tripId': 'trip_id',
               'alert_informedEntity_trip_scheduleRelationship': 'schedule_relationship',
               'alert_headerText_translation_text': 'header_text',
               'alert_descriptionText_translation_text': 'description_text',
               }
    df.rename(columns=renames, inplace=True)


def sanitise_array(df: pd.DataFrame) -> None:
    normalize_keys(df)

    # Remove columns and rows with all NaNs
    df.dropna(axis=0, how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)

    # Remove old indexes
    df.drop(columns='level_0', inplace=True, errors='ignore')

    # Remove duplicated entries, ignoring timpestamps and index
    keys = list(df.keys())
    with contextlib.suppress(ValueError):
        keys.remove('timestamp')
        keys.remove('index')

        # These may be updated in the database, so ignore as well
        keys.remove('arrival_delay')
        keys.remove('arrival_time')
        keys.remove('departure_delay')
        keys.remove('departure_time')
        keys.remove('arrival_uncertainty')
        keys.remove('departure_uncertainty')

    df.drop_duplicates(subset=keys, inplace=True, keep='last')


def download_file(task):
    url = task.inputs['url']
    output = task.outputs['file']
    with open(output, 'wb') as f_out:
        with requests.get(url, stream=True) as req:
            for chunk in req.iter_content(chunk_size=128):
                f_out.write(chunk)


def bz2_to_tar(bz2_file):
    folder = os.path.dirname(bz2_file)
    base_name = os.path.splitext(os.path.basename(bz2_file))[0]
    decompressed_file = os.path.join(folder, base_name)
    tar_file = os.path.join(folder, f"{base_name}.tar")

    try:
        # 调用系统工具 7z 解压 .bz2 文件
        # 注意：7z 参数里 "e" 与 "x" 的区别：
        #  - "e"：解压为单个文件，若原本是多文件/目录，可能结构会被打平
        #  - "x"：保持原有目录结构
        subprocess.run(["7z", "e", "-o" + decompressed_file, bz2_file], check=True)

        # 将解压的文件（或目录）重新封装为 .tar
        with tarfile.open(tar_file, 'w') as tar:
            tar.add(decompressed_file, arcname=os.path.basename(decompressed_file))

        return tar_file
    except subprocess.CalledProcessError as e:
        print(f"System tool failed: {e}")
        return None
    finally:
        # 删除解压后的临时文件（或文件夹）
        if os.path.exists(decompressed_file):
            time.sleep(0.5)  # 等待片刻，避免文件刚刚解压后仍被占用

            if os.path.isdir(decompressed_file):
                # 如果解压得到的是目录，需要用 rmtree
                shutil.rmtree(decompressed_file)
                print(f"Removed temporary directory: {decompressed_file}")
            else:
                # 如果是文件，用 os.remove / os.unlink
                os.remove(decompressed_file)
                print(f"Removed temporary file: {decompressed_file}")



def get_data(date: str, hour: (int, str), feed: str, company: str, output_file: (str, None) = None) -> None:
    if output_file is None:
        output_file = _get_data_path(company, feed, date, hour)

    print('Getting', output_file)

    # admit both _ and -
    date = date.replace('_', '-')

    data_date = datetime.date.fromisoformat(date)

    # ------------------------------------------------------------------------
    # Create data dir
    # ------------------------------------------------------------------------
    ey.shell('mkdir [o:datafolder:data]')

    # ------------------------------------------------------------------------
    # Download data
    # ------------------------------------------------------------------------
    if config.API_VERSION == 1:
        koda_url = f"https://koda.linkoping-ri.se/KoDa/api/v0.1?company={company}&feed={feed}&date={date}"  # todo: 改为stockholm的
    else:
        koda_url = f'https://koda.linkoping-ri.se/KoDa/api/v2/gtfs-rt/{company}/{feed}?date={date}&hour={hour}&key={config.API_KEY}'
    out_path = os.path.join(config.CACHE_DIR, f'{company}-{feed}-{date}.bz2').lower()
    download = ey.func(download_file, inputs={'url': koda_url}, outputs={'file': out_path})

    # Check the file:
    with open(download.outputs['file'], 'rb') as f:
        start = f.read(10)
        if b'error' in start:
            msg = start + f.read(70)
            msg = msg.strip(b'{}" ')
            raise ValueError('API returned the following error message:', msg)

    # Select the list of files to extract:
    #tar_file_name = os.path.join(download.outputs['file'], "Cache")
    tar_file_name1 = download.outputs['file']
    tar_file_name = bz2_to_tar(tar_file_name1)
    print(tar_file_name)

    # ------------------------------------------------------------------------
    # GTFS to file
    # ------------------------------------------------------------------------

    def merge_files(task):
        tar = tarfile.open(tar_file_name)
        #_prefix = f'mnt/kodashare/KoDa_NiFi_data/{company}/{feed}/{data_date.year}/' \
          #        f'{str(data_date.month).zfill(2)}/{str(data_date.day).zfill(2)}/{str(hour).zfill(2)}/'
        _prefix = f'{company}-tripupdates-{data_date.year}-{data_date.month}-01T10'
        print(_prefix)
        gzfiles = [name for name in tar.getnames() if name.endswith(".pb") and 'Duplicate' not in name] #if name.startswith(_prefix) and 'Duplicate' not in name]
        # print(f"Files found in tar archive: {gzfiles}")


        # Extract each file and pass it to the parsing function
        parsed_files = Parallel(n_jobs=config.N_CPU, verbose=0)(
            delayed(_parse_gtfs)(tar.extractfile(gtfsfile).read()) for gtfsfile in gzfiles)
        tar.close()
        merged_df = pd.concat(parsed_files)

        # Force casts:
        castings = dict()
        for k in merged_df.keys():
            if 'timestamp' in k:  # Timestamps should be ints, not strings
                castings[k] = np.int64
            elif k == 'id':
                castings[k] = np.int64

        merged_df.dropna(how='all', inplace=True)  # Remove rows of only NaNs
        merged_df = merged_df.astype(castings)

        # Remove dots from column names
        rename = dict((k, k.replace('.', '_')) for k in merged_df.keys() if '.' in k)
        merged_df.rename(columns=rename, inplace=True)

        # Clean up duplicates, fix keys, etc
        sanitise_array(merged_df)

        if merged_df.empty:  # Feather does not support a DF without columns, so add a dummy one
            merged_df['_'] = np.zeros(len(merged_df), dtype=np.bool_)

        # Save to file
        merged_df.reset_index(inplace=True)
        merged_df.to_feather(task.outputs['outfile'], compression='zstd', compression_level=9)

    ey.func(merge_files, outputs={'outfile': output_file})


def get_range(start_date, end_date, start_hour, end_hour, feed, company) -> None:
    warnings.warn(DeprecationWarning('Use get_data_range instead'))
    date_range = pd.date_range(start=start_date, end=end_date)
    hour_range = range(start_hour, end_hour + 1)

    for date in tqdm.tqdm(date_range):
        print("Date: ", date.strftime("%Y-%m-%d"))
        for hour in tqdm.tqdm(hour_range, leave=False, desc='Hours for ' + date.strftime("%Y-%m-%d")):
            get_data(f'{date.year:0>4}-{date.month:0>2}-{date.day:0>2}', hour, feed, company)
