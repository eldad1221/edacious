import os
from quickbelog import Log
from dotenv import load_dotenv
from edacious import event_handler
from edacious.sqs import EventListener

load_dotenv()
SQS_URL = os.getenv('SQS_URL')
Log.set_log_level(20)


@event_handler(event_type='hello-world')
def my_handler(event: dict):
    print(f'Handler function got {event}')


if __name__ == '__main__':

    listener = EventListener(sqs_url=SQS_URL, visibility_timeout=60, max_messages_to_fetch=10)
    listener.set_seconds_to_wait(seconds=0.5)
    listener.run()
