#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 * @Author: chenjb
 * @Date: 2020/8/11 14:02
 * @Last Modified by:   chenjb
 * @Last Modified time: 2020/8/11 14:02
 * @Desc: clickhouse封装
"""
import aiochclient as aiochclient
import aiohttp as aiohttp
from clickhouse_driver import connect, Client
from typing import Any, List
import json

from lib.load_conf import conf
from lib.common import DateEncoder


class ClickhouseClient:
    """
    clickhouse封装
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str) -> None:
        self.conn = Client(host=host, port=port, database=database, user=user, password=password)

    def close(self):
        self.conn.disconnect()

    def query(self, query: str, params: Any = None) -> List[dict]:
        """
        查询一条结果
        :param query: 查询语句
        :param params: 查询参数
        :return: List[dict]
        """
        try:
            data = self.conn.execute_iter(query, params, with_column_types=True)
            columns = [column[0] for column in next(data)]
            temp = []
            for row in data:
                temp.append(json.dumps(dict(zip(columns, [value for value in row])), cls=DateEncoder))
        except Exception as e:
            raise e
        finally:
            self.close()
        return temp


def create_client():
    host = conf.get('clickhouse', 'HOST')  # type: str
    port = int(conf.get('clickhouse', 'PORT'))  # type: int
    user = conf.get('clickhouse', 'USER')  # type: str
    password = conf.get('clickhouse', 'PASSWORD')  # type: str
    database = conf.get('clickhouse', 'DATABASE')  # type: str
    return ClickhouseClient(host, port, database, user, password)


click_house = create_client()


if __name__ == '__main__':
    print(click_house.query("select d_url.virus_behaviors from default.ptd_blackdata_processed3 limit 1"))


