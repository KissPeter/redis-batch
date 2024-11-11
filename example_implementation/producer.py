#!/usr/bin/env python3
from time import sleep

from common import STREAM, get_random_wait_time
from redis import Redis

if __name__ == "__main__":
    redis_conn = Redis(decode_responses=True,ssl=True,ssl_cert_reqs=None, username="default",password="Acj-AAIncDE3NDYwYWEzNTUwNjk0YmI3ODAxM2VhMDc2MTA4NzczM3AxNTE0NTQ", host="honest-hedgehog-51454.upstash.io")
    iteration = 0
    while True:
        sample_data = {"iteration": iteration, "message": "stuff goes here"}
        print(f" {iteration}. Adding message to steam: {sample_data}")
        redis_conn.xadd(name=STREAM, fields=sample_data)
        sleep(get_random_wait_time())
        iteration += 1
