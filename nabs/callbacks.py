from bluesky.callbacks import CallbackCounter


class EventCounter(CallbackCounter):
    def __call__(self, name, doc):
        if name == 'event':
            super().__call__(name, doc)
