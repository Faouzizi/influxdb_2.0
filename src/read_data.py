from influxdb_client import InfluxDBClient

import config

def read_data_from_influx(query):
    """
    Read data from influxdb
    :param query: influxql query
    :return: dataframe
    """
    with InfluxDBClient(url="http://localhost:8086",
                        token=config.token,
                        org=config.org) as client:
        try:
            df = client.query_api().query_data_frame(query)
        except Exception as e:
            print(e.message())
    return df


if __name__ == '__main__':
    query = """from(bucket: "medium")
               |> range(start: -50d, stop: now())
               |> filter(fn: (r) => r["_measurement"] == "medium_demo")
               |> filter(fn: (r) => r["_field"] == "Dividendes")
               """
    df = read_data_from_influx(query)
