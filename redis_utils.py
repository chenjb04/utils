#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
 * @Author: chenjb
 * @Date: 2020/8/12 16:50
 * @Last Modified by:   chenjb
 * @Last Modified time: 2020/8/12 16:50
 * @Desc:
"""
import redis

from lib.load_conf import conf


class RedisClient:
    def __init__(self, host: str, port: int, db: str, password: str, decode_responses: bool = True) -> None:
        try:
            self.connect_pool = redis.ConnectionPool(host=host, port=port, db=db, password=password,
                                                     decode_responses=decode_responses)
        except Exception as e:
            raise e

    def get_connect(self) -> redis.Redis:
        """
        获取redis连接对象
        :return:
        """
        return redis.Redis(connection_pool=self.connect_pool)

    def close(self):
        """
        关闭连接
        :return:
        """
        redis_store = self.get_connect()
        redis_store.close()

    def __getattr__(self, command):
        def _(*args):
            return getattr(self.get_connect(), command)(*args)

        return _


def create_redis_pool():
    host = conf.get('redis', 'HOST')  # type: str
    port = int(conf.get('redis', 'PORT'))  # type: int
    password = conf.get('redis', 'PASSWORD')  # type: str
    db = conf.get('redis', 'DB')  # type: str
    redis_pool = RedisClient(host=host, port=port, password=password, db=db)
    return redis_pool.get_connect()


redis_store = create_redis_pool()


if __name__ == '__main__':
    print(redis_store.smembers('fda1aeb5f6e83f4cb5ada27cb012df2d'))