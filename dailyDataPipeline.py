import numpy as np
import requests
from datetime import datetime, timedelta

# 不要使用 from pykoda.datautils import get_data_range, load_static_data
# 而是直接 import pykoda
import pykoda
# DBSCAN
from sklearn.cluster import DBSCAN

import os
import zipfile
import io
import pandas as pd
import time
import shutil


def clean_cache_folder(folder_name: str):
    """
    如果指定文件夹存在，则删除该文件夹下的所有内容（文件与子文件夹）。
    """
    cache_path = os.path.join(os.getcwd(), folder_name)
    if os.path.isdir(cache_path):
        for file_or_dir in os.listdir(cache_path):
            file_or_dir_path = os.path.join(cache_path, file_or_dir)
            try:
                # 如果是文件或软链接直接删除，如果是文件夹则整棵删除
                if os.path.isfile(file_or_dir_path) or os.path.islink(file_or_dir_path):
                    os.unlink(file_or_dir_path)
                elif os.path.isdir(file_or_dir_path):
                    shutil.rmtree(file_or_dir_path)
            except Exception as e:
                print(f"删除 {file_or_dir_path} 失败，原因：{e}")
    else:
        print(f"未发现 {folder_name} 文件夹，无需删除。")


def combine_with_existing_zip(
        new_data: pd.DataFrame,
        zip_path: str = os.path.join(os.getcwd(), "merged_output.zip"),
        csv_name: str = "merged_output.csv"
) -> pd.DataFrame:
    """
    读取已有 merged_output.zip (如果存在),
    将其中的 merged_output.csv 读入 DataFrame,
    再与 new_data 合并后重新写回 zip。

    返回：合并后的最终 DataFrame
    """
    # 如果 zip 存在，先读旧数据
    if os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open(csv_name) as f:
                old_df = pd.read_csv(f)
        # 合并 (拼接行)
        final_df = pd.concat([old_df, new_data], ignore_index=True)
        print(f"已从 {zip_path} 读取到 {len(old_df)} 行旧数据，并合并新数据 {len(new_data)} 行...")
    else:
        # 如果 zip 不存在，说明是第一次创建
        final_df = new_data.copy()
        print(f"未检测到已有文件 {zip_path}，本次直接写入 {len(new_data)} 行新数据...")

    # （可选）做去重/排序/其他清洗
    # final_df.drop_duplicates(...)
    # final_df.sort_values(...)
    # ...

    # 将合并后的 DataFrame 写回 zip
    buffer = io.StringIO()
    final_df.to_csv(buffer, index=False)
    buffer.seek(0)

    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, buffer.getvalue())

    print(f"已将合并的数据写入到 {zip_path} 中 (文件名: {csv_name})，共 {len(final_df)} 行。")
    return final_df


def run_pipeline(
    company: str = "sl",
    feed: str = "TripUpdates",
    start_date: str = None,
    end_date: str = None,
    start_hour: int = 0,
    end_hour: int = 23,
    output_dir: str = os.path.join(os.getcwd(), "output_csv")
):
    """
    运行完整的数据获取、清洗、天气数据合并等流程，但不在本地生成任何中间 CSV。
    仅在最后输出一个 zip 文件 (merged_output.zip)，其中包含一个最终的 CSV。
    """

    # 如果没有指定日期，则默认取“昨天”的数据
    if not start_date or not end_date:
        today = datetime.today().date()
        yesterday = today - timedelta(days=1)
        start_date = start_date or str(yesterday)
        end_date = end_date or str(yesterday)

    print(f"[{datetime.now()}] 开始执行数据 Pipeline: {start_date} ~ {end_date} (hours={start_hour}-{end_hour})")

    #----------------------------------------------------------------------
    # 1. 从 pykoda 拉取实时数据 (in-memory)
    #----------------------------------------------------------------------
    realtime_data = pykoda.datautils.get_data_range(
        feed=feed,
        company=company,
        start_date=start_date,
        start_hour=start_hour,
        end_date=end_date,
        end_hour=end_hour,
        merge_static=False
    )

    if realtime_data.empty:
        print("没有获取到实时数据，流程结束。")
        return

    print("成功获取实时数据，前5条：")
    print(realtime_data.head())

    #----------------------------------------------------------------------
    # 2. 数据清洗 (in-memory)
    #----------------------------------------------------------------------
    # 时间戳转换
    if "timestamp" in realtime_data.columns:
        realtime_data['timestamp'] = pd.to_datetime(realtime_data['timestamp'], unit='s')
    if 'arrival_time' in realtime_data.columns:
        realtime_data['arrival_time'] = pd.to_datetime(realtime_data['arrival_time'], unit='s')
    if 'departure_time' in realtime_data.columns:
        realtime_data['departure_time'] = pd.to_datetime(realtime_data['departure_time'], unit='s')

    # 转换ID列为字符串
    if 'vehicle_id' in realtime_data.columns:
        realtime_data['vehicle_id'] = realtime_data['vehicle_id'].astype(str)
    if 'trip_id' in realtime_data.columns:
        # trip_id 可能是浮点数，这里统一转回字符串
        realtime_data['trip_id'] = realtime_data['trip_id'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) else None
        )

    # 删除不需要的列 (示例，可根据实际情况定制)
    columns_to_remove = [
        'start_time', 'schedule_relationship','route_id','direction_id',
        'arrival_uncertainty', 'departure_uncertainty',
        'tripUpdate_stopTimeUpdate_scheduleRelationship',
        'start_date','timestamp','vehicle_id'
    ]
    existing_cols = [c for c in columns_to_remove if c in realtime_data.columns]
    cleaned_data = realtime_data.drop(columns=existing_cols, errors='ignore')

    print("完成实时数据清洗，准备与静态数据合并...")

    #----------------------------------------------------------------------
    # 3. 加载静态数据 & 聚类
    #----------------------------------------------------------------------
    static_data = pykoda.datautils.load_static_data(
        company=company,
        date=start_date,
        remove_unused_stations=True
    )
    stops_data = static_data.stops
    stops_data.reset_index(inplace=True)

    # 只保留需要的列
    stops_data = stops_data[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]

    # 使用 DBSCAN 进行聚类
    coords = stops_data[['stop_lat', 'stop_lon']].values
    db = DBSCAN(eps=0.01, min_samples=1, metric='euclidean').fit(coords)
    stops_data['group_id'] = db.labels_

    #----------------------------------------------------------------------
    # 4. 获取天气数据 (in-memory)
    #----------------------------------------------------------------------
    def get_historical_weather(lat, lon, start_d, end_d):
        variables = "temperature_2m,precipitation,snowfall,snow_depth,wind_speed_10m,cloud_cover_low"
        url = (
            f"https://archive-api.open-meteo.com/v1/era5?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={start_d}&end_date={end_d}"
            f"&hourly={variables}"
        )
        resp = requests.get(url)
        if resp.status_code == 200:
            jd = resp.json()
            return pd.DataFrame({
                'datetime': jd['hourly']['time'],
                'temperature': jd['hourly']['temperature_2m'],
                'precipitation': jd['hourly']['precipitation'],
                'snowfall': jd['hourly']['snowfall'],
                'snow_depth': jd['hourly']['snow_depth'],
                'wind_speed': jd['hourly']['wind_speed_10m'],
                'cloud_cover': jd['hourly']['cloud_cover_low']
            })
        else:
            print(f"获取天气数据失败: {resp.status_code}")
            return pd.DataFrame()

    group_centers = stops_data.groupby('group_id')[['stop_lat', 'stop_lon']].mean().reset_index()
    group_weather_data = []

    for _, row in group_centers.iterrows():
        grp_id = row['group_id']
        lat = row['stop_lat']
        lon = row['stop_lon']
        weather_df = get_historical_weather(lat, lon, start_date, end_date)
        if not weather_df.empty:
            weather_df['group_id'] = grp_id
            group_weather_data.append(weather_df)

    if len(group_weather_data) == 0:
        print("天气数据为空，无法完成合并。")
        return

    # 将各组天气拼接起来
    group_weather_data = pd.concat(group_weather_data, ignore_index=True)
    stops_weather = stops_data.merge(group_weather_data, on='group_id', how='left')

    #----------------------------------------------------------------------
    # 5. 实时数据 (cleaned_data) 与天气数据 (stops_weather) 合并
    #----------------------------------------------------------------------
    # 先给 cleaned_data 统一加一个 date_hour 字段
    # （注意：如果 cleaned_data 里没有 datetime 列，需先创建空列）
    if 'datetime' in cleaned_data.columns:
        cleaned_data['datetime'] = pd.to_datetime(cleaned_data['datetime'])
    else:
        cleaned_data['datetime'] = pd.NaT

    cleaned_data['date_hour'] = cleaned_data['datetime'].dt.strftime('%Y-%m-%d %H')

    # 同理，给 stops_weather 也加一个 date_hour
    stops_weather['datetime'] = pd.to_datetime(stops_weather['datetime'])
    stops_weather['date_hour'] = stops_weather['datetime'].dt.strftime('%Y-%m-%d %H')

    # 根据 stop_id + date_hour 进行合并
    if 'stop_id' in cleaned_data.columns and 'stop_id' in stops_weather.columns:
        merged = pd.merge(cleaned_data, stops_weather, on=['stop_id', 'date_hour'], how='left')
    else:
        # 如果没有 stop_id，就只能保留实时数据部分
        merged = cleaned_data.copy()

    # 去除重复的 datetime_y 等无关列
    if 'datetime_y' in merged.columns:
        merged.drop(columns=['datetime_y'], inplace=True, errors='ignore')

    #----------------------------------------------------------------------
    # 6. 将最终合并结果合并进 zip
    #----------------------------------------------------------------------
    # 如果你希望 zip 文件保存在指定文件夹，可以使用 os.path.join(output_dir, "merged_output.zip")
    # 这里演示直接用当前文件夹即可
    os.makedirs(output_dir, exist_ok=True)
    zip_path = os.path.join(output_dir, "merged_output.zip")

    final_df = combine_with_existing_zip(
        new_data=merged,
        zip_path=zip_path,           # 最终输出的 zip 文件路径
        csv_name="merged_output.csv" # zip 里面的 CSV 文件名
    )

    print(f"已将 {len(merged)} 条新数据合并到 {zip_path}，合并后总计 {len(final_df)} 条。")
    return final_df


if __name__ == "__main__":
    start_time = time.time()  # 开始计时

    clean_cache_folder("Cache")
    run_pipeline(
        company="sl",
        feed="TripUpdates",
        start_date=None,  # 不指定则默认取昨天
        end_date=None,    # 不指定则默认取昨天
        start_hour=0,
        end_hour=23
    )
    clean_cache_folder("Cache")

    end_time = time.time()  # 结束计时
    elapsed_time = end_time - start_time
    print(f"Execution time: {elapsed_time:.2f} seconds")
