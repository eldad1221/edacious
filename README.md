# Edacious an Event Derive Architecture framework

Implementing an EDA usually requires a queue mechanism. Currently support Redis streams and Amazon Web Services SQS.

Event must be a dict that contain data to be processed.
Every event must have an event type (str) attribute, reserve key is `event-type`.

To process an event the event type has to be associated to one or more event handlers.

Event handler is a function that receive an event (dict) and process it, here is an example for running event listener based on Amazon Web Services SQS.

    from edacious.sqs import EventListener

    SQS_URL = 'https://sqs.my-region.amazonaws.com/123456789012/my-app-sqs'


    @event_handler(event_type='hello-world')
    def my_handler(event: dict):
        print(event)


    if __name__ == '__main__':

    listener = EventListener(sqs_url=SQS_URL, visibility_timeout=60, max_messages_to_fetch=10)
    listener.set_seconds_to_wait(seconds=2.5)
    listener.run()

