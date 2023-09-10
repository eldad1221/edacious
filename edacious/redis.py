import edacious
from redis import Redis, ConnectionPool
from redis_streams.consumer import Consumer, RedisMsg


class EventListener(edacious.EventListener):

    def __int__(self, *args, **kwargs):
        """
        :param args:
        :param kwargs:
            sqs_url: AWS SQS url
        :return:
        """

        self._host = kwargs.get('host')
        self._port = kwargs.get('port')
        # self._user = kwargs.get('user')
        self._stream_name = kwargs.get('stream_name')
        self._consumer_group_name = kwargs.get('consumer_group_name')

        self._redis_pool = ConnectionPool(
            host=self._host, port=self._port, db=0,
            decode_responses=kwargs.get('decode_responses', True),
            max_connections=kwargs.get('max_connections', 10)
        )

        self._consumer = Consumer(
            redis_conn=self.get_redis(),
            stream=self._stream_name,
            consumer_group=self._consumer_group_name,
            batch_size=10,
            max_wait_time_ms=1000,
        )

        super().__init__(*args, **kwargs)

    def get_redis(self) -> Redis:
        return Redis(connection_pool=self._redis_pool, charset="utf-8")

    def fetch(self) -> list:
        return self._consumer.get_items()

    def event_handling_error(self, event: dict):
        pass

    def event_handling_done(self, event: RedisMsg):
        self._consumer.remove_item_from_stream(item_id=event.msgid)
