from asyncio import Future
from copy import copy
from threading import RLock, Event, Thread

from bluesky.plan_stubs import drop, wait_for
from bluesky.preprocessors import plan_mutator


class EnsureGoodReads:
    """
    Ensure that reads only happen under good conditions.

    Plan preprocessor that takes note of trigger, read, and save messages while
    monitoring a boolean signal. When the boolean signal is high (1), messages
    proceed as normal. When the boolean signal goes low (0), we wait for it to
    go high for a minimum amount of time, then retry any trigger/read calls
    that were pending. Potentially bad readings are dropped.
    """
    def __init__(self, ok_sig, sleep_time=0):
        self.ok_sig = ok_sig
        self.sleep_time = float(sleep_time)
        self.lock = RLock()
        self.not_ok_flag = Event()
        self.future = None
        self.msg_cache = []

        ok_sig.subscribe(self.ok_cb)

    def ok_cb(self, *args, **kwargs):
        with self.lock:
            # Ok to read
            if kwargs['value']:
                if self.future is not None:
                    self.not_ok_flag.clear()
                    self.run_ok_thread()
            # Not ok to read
            else:
                if self.future is None:
                    self.future = Future()
                self.not_ok_flag.set()

    def run_ok_thread(self):
        t = Thread(target=self.ok_thread, args=())
        t.start()

    def ok_thread(self):
        if not self.not_ok_flag.wait(timeout=self.sleep_time):
            self.future.set_result('Done')

    def __call__(self, plan):
        yield from plan_mutator(plan, self.msg_proc)

    def msg_proc(self, msg):
        if msg.command == 'save':
            self.msg_cache = []
        elif msg.command in ['trigger', 'create', 'read']:
            self.msg_cache.append(msg)
            if not self.ok_sig.get():
                return (self.wait_and_retry, None)
        return (None, None)

    def wait_and_retry(self):
        try:
            yield from drop()
        except Exception:
            pass

        yield from wait_for([self.future])

        # Recursion alert... but this is probably ok.
        # Watch the replayed message for bad state too.
        cache = copy(self.msg_cache)
        self.msg_cache = []
        yield from plan_mutator(cache, self.msg_proc)
