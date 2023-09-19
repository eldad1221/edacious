import json
import boto3
import edacious
from quickbelog import Log

# Create SQS client
sqs = boto3.client('sqs')

MESSAGE_ATTRIBUTE_NAMES = 'MessageAttributeNames'
MESSAGE_ATTRIBUTES = 'MessageAttributes'


class EventListener(edacious.EventListener):

    def __init__(self, sqs_url: str, visibility_timeout: int = 0, max_messages_to_fetch: int = 10):
        """
        :param sqs_url: AWS SQS url
        :param visibility_timeout: Visibility timeout when receiving messages is seconds
        :return:
        """

        self._sqs_url = sqs_url
        self._visibility_timeout = visibility_timeout
        self._max_messages_to_fetch = max_messages_to_fetch
        Log.info(f'Will listen to SQS {self._sqs_url}')
        super().__init__(tuple(), {})

    def fetch(self) -> list:
        response = sqs.receive_message(
            QueueUrl=self._sqs_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=self._max_messages_to_fetch,
            MessageAttributeNames=['All'],
            VisibilityTimeout=self._visibility_timeout,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:
            events = []
            for msg in response['Messages']:

                try:
                    event = json.loads(msg.get('Body'))
                    event[edacious.EVENT_TYPE_KEY] = self.get_event_type(msg=msg)
                    event['ReceiptHandle'] = msg['ReceiptHandle']
                    events.append(event)
                except json.JSONDecodeError:
                    Log.warning(f'Can not parse message body, ignoring this message. Got {msg.get("Body")}')
            return events
        else:
            return []

    @staticmethod
    def get_event_type(msg: dict) -> str:
        even_type = None
        if MESSAGE_ATTRIBUTE_NAMES in msg:
            even_type = msg.get(
                        'MessageAttributes',
                        {}
                    ).get(edacious.EVENT_TYPE_KEY, {}).get('StringValue')
        elif MESSAGE_ATTRIBUTES in msg.get('Body', {}):
            even_type = msg.get(
                'MessageAttributes',
                {}
            ).get(edacious.EVENT_TYPE_KEY, {}).get('Value')
        return even_type

    def event_handling_error(self, event: dict):
        pass

    def event_handling_done(self, event: dict):
        # Delete received message from queue
        receipt_handle = event['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=self._sqs_url,
            ReceiptHandle=receipt_handle
        )
