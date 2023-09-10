import boto3
import edacious

# Create SQS client
sqs = boto3.client('sqs')


class EventListener(edacious.EventListener):

    def __int__(self, *args, **kwargs):
        """
        :param args:
        :param kwargs:
            sqs_url: AWS SQS url
        :return:
        """

        self._sqs_url = kwargs.get('sqs_url')
        self._visibility_timeout = kwargs.get('visibility_timeout')
        super().__init__(*args, **kwargs)

    def fetch(self) -> list:
        response = sqs.receive_message(
            QueueUrl=self._sqs_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'event-type'
            ],
            VisibilityTimeout=self._visibility_timeout,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:
            return response['Messages']

    def event_handling_error(self):
        pass

    def event_handling_done(self, event: dict):
        # Delete received message from queue
        receipt_handle = event['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=self._sqs_url,
            ReceiptHandle=receipt_handle
        )
