from bluesky.callbacks import CallbackCounter


class EventCounter(CallbackCounter):
    def __init__(self):
        super().__init__()
        self('event', {})

    def __call__(self, name, doc):
        if name == 'event':
            super().__call__(name, doc)
