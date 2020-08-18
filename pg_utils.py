#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 * @Author: chenjb
 * @Date: 2020/8/11 15:14
 * @Last Modified by:   chenjb
 * @Last Modified time: 2020/8/11 15:14
 * @Desc: pgsql封装
"""

from datetime import datetime

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, RealDictRow
from typing import Optional

from lib.load_conf import conf


class PgClient:
    """
    pgsql封装
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str, minconn: int = 0, maxconn: int = 0) -> None:
        try:
            # minconn初始化时，链接池中至少创建的空闲的链接，0表示不创建
            # maxconn链接池中最多闲置的链接，0不限制
            #  postgresql支持TCP_KEEPLIVE机制。有三个系统变量tcp_keepalives_idle，tcp_keepalives_interval，tcp_keepalives_count来设置postgresql如何处理死连接,
            # 对于每个连接，postgresql会对这个连接空闲tcp_keepalives_idle秒后，主动发送tcp_keeplive包给客户端，以侦探客户端是否还活着，
            #  当发送tcp_keepalives_count个侦探包，每个侦探包在tcp_keepalives_interval秒内没有回应，postgresql就认为这个连接是死的。于是切断这个死连接。
            #  在postgresql,这三个参数都设为0将使用操作系统的默认值，在linux下，tcp_keepalives_idle一般是2个小时，也就是2个小时后，服务器才可以自动关掉死连接。
            self.connect_pool = pool.SimpleConnectionPool(host=host, port=int(port), user=user, password=password,
                                                          database=database, keepalives=1,
                                                          keepalives_idle=30, keepalives_interval=10,
                                                          keepalives_count=5, minconn=int(minconn), maxconn=int(maxconn))
        except Exception as e:
            raise e

    def get_connect(self):
        """
        获取conn和cursor
        :return: conn, cursor
        """
        conn = self.connect_pool.getconn()  # type: psycopg2.connect
        cursor = conn.cursor(cursor_factory=RealDictCursor)  # type: RealDictCursor
        return conn, cursor

    def close_connect(self, conn: psycopg2.connect, cursor: RealDictCursor) -> None:
        """
        关闭连接
        :param conn: ；连接池对象
        :param cursor:  游标
        :return: None
        """
        cursor.close()
        self.connect_pool.putconn(conn)

    def close_all(self) -> None:
        """
        关闭全部连接
        :return: None
        """
        self.connect_pool.closeall()

    def query_one(self, sql: str, value: Optional[tuple] = None) -> RealDictRow:
        """
        查询一条结果
        :param sql: sql语句
        :param value: 格式化sql值
        :return:  RealDict类型
        """
        conn, cursor = self.get_connect()
        try:
            cursor.execute(sql, value)
            res = cursor.fetchone()
        except Exception as e:
            raise e
        finally:
            self.close_connect(conn, cursor)
        return res

    def query(self, sql: str, value: Optional[tuple] = None) -> RealDictRow:
        """
        查询多条结果
        :param sql: sql语句
        :param value: 格式化sql值
        :return: RealDict
        """
        conn, cursor = self.get_connect()
        try:
            cursor.execute(sql, value)
            res = cursor.fetchall()
        except Exception as e:
            raise e
        finally:
            self.close_connect(conn, cursor)
        return res

    def execsql(self, sql: str, value: Optional[tuple] = None) -> bool:
        """
        执行 增删改操作
        :param sql: sql语句
        :param value: 格式化sql值
        :return: true操作成功 false 操作失败
        """
        res = False
        conn, cursor = self.get_connect()
        try:
            cursor.execute(sql, value)
            res = True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.commit()
            self.close_connect(conn, cursor)
        return res

    def execsql_return(self,  sql: str, value: Optional[tuple] = None) -> RealDictRow:
        """
        执行添加sql语句，并返回当前添加的数据id，使用此方法时注意insert语句的末尾加上 returning id
        :param sql: sql语句
        :param value: 格式化sql值
        :return: tuper or None
        """
        conn, cursor = self.get_connect()
        try:
            cursor.execute(sql, value)
            res = cursor.fetchone()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.commit()
            self.close_connect(conn, cursor)
        return res


def create_db_pool():
    host = conf.get('pg_database', 'HOST')  # type: str
    port = int(conf.get('pg_database', 'PORT'))  # type: int
    user = conf.get('pg_database', 'USER')  # type: str
    password = conf.get('pg_database', 'PASSWORD')  # type: str
    database = conf.get('pg_database', 'DATABASE')  # type: str
    minconn = int(conf.get('pg_database', 'MINCONN'))  # type: int
    maxconn = int(conf.get('pg_database', 'MAXCONN'))  # type: int
    return PgClient(host=host, port=port, user=user, password=password, database=database, minconn=minconn, maxconn=maxconn)


pg = create_db_pool()


if __name__ == '__main__':
    # sql = """select event_id, alarm_id from h_threat_alarm_event where alarm_id = %s"""
    # res = pg.query_one(sql, ('T200608000239', ))
    # print(type(res))
    # sql = """insert into h_alarm_record(alarm_id, record_info, r_person, r_time) values(%s,%s,%s, %s)"""
    # res = pg.execsql(sql, ('T200119002088', 'hahaha', '6666', datetime.now()))
    # print(res)
    # sql = """insert into h_alarm_record(alarm_id, record_info, r_person, r_time) values(%s,%s,%s, %s) returning r_id"""
    # res = pg.execsql_return(sql, ('T200119002088', 'hahaha', '7777', datetime.now()))
    # print(res)
    pass