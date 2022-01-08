import pytest
from redis import Redis

from redis_batch_test.test_utils import STREAM, GROUP, \
    set_logger, TEST_DATASET


class TestBase:
    redis_conn = Redis(decode_responses=True)
    logger = set_logger()

    @pytest.fixture(autouse=True)
    def prepare_redis(self):
        if self.redis_conn.xlen(name=STREAM):
            self.logger.info(f'Trim {STREAM}')
            self.redis_conn.xtrim(STREAM, maxlen=0)
        for test_data in TEST_DATASET:
            self.logger.debug(f"Add  test data: {test_data}")
            self.redis_conn.xadd(name=STREAM, fields=test_data)
        assert self.redis_conn.xlen(name=STREAM) == len(TEST_DATASET)
        yield
        self.redis_conn.xtrim(STREAM, maxlen=0)
        for consumer in self.redis_conn.xinfo_consumers(name=STREAM, groupname=GROUP):
            self.logger.debug(f'Delete consumer {consumer}')
            self.redis_conn.xgroup_delconsumer(name=STREAM,
                                               groupname=GROUP,
                                               consumername=consumer.get("name"))
