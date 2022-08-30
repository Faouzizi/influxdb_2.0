import pandas as pd
import influxdb

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import config


def df_2_influxdb(df):
    """
    Send dataframe to influxdb
    :type df: pd.DataFrame
    :param df: pd df to be stored in InfluxDB
    :return: 'Success' as result
    """
    output_message = "Error"
    try:
        client = InfluxDBClient(url="http://localhost:8086",
                                token=config.token,
                                org=config.org)
        write_api = client.write_api(
            write_options=SYNCHRONOUS
        )
        write_api.write(config.bucket,
                        record=df,
                        data_frame_measurement_name=measurement,
                        data_frame_tag_columns=['ticker'])
        output_message = 'Success'
    except Exception as e:
        print('Error:', e)
    finally:
        try:
            client.close()
        except:
            pass
    return output_message



def line_interface(df,
                   measurement,
                   field_columns,
                   tag_columns,
                   numeric_precision='full',
                   batch=False):
    client = influxdb.DataFrameClient()
    lines = client._convert_dataframe_to_lines(df,
                                               measurement,
                                               field_columns=field_columns,
                                               tag_columns=tag_columns,
                                               numeric_precision=numeric_precision)
    client.close()
    if not batch:
        lines = ('\n'.join(lines)) + '\n'
    return lines


if __name__ == '__main__':
    measurement = 'medium_demo'
    df = pd.read_csv('./data/df.csv')
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df_2_influxdb(df)
