import asyncio
import logging
import time
from threading import RLock, Event

from bluesky.plan_stubs import drop, save, null, wait_for
from bluesky.preprocessors import plan_mutator
from bluesky.utils import make_decorator

logger = logging.getLogger(__name__)


class SuspendPreprocessor:
    """
    Base preprocessor that suspends on specific commands based on a signal.

    Parameters
    ----------
    signal: ``Signal``
        The signal to subscribe to, whose value determines when to suspend.

    commands: ``list of str``, optional
        The commands to suspend on. If omitted, we'll suspend on all commands.

    sleep: ``int`` or ``float``, optional
        The amount of time to wait after `should_resume` returns ``True``
        before ending the suspension. If `should_suspend` return ``True`` at
        any time during this wait period, we will cancel the resumption and
        return to the suspended state.
    """
    def __init__(self, signal, *, commands=None, sleep=0,
                 pre_plan=null, post_plan=null, follow_plan=null):
        self._sig = signal
        self._cmd = commands
        self._sleep = sleep
        self._suspend_active = False
        self._resume_ts = None
        self._suspend_ev = Event()
        self._ok_future = asyncio.Future()
        self._ok_future.set_result('ok')
        self._rlock = RLock()
        self._subid = None

    def should_suspend(self, value):
        """
        Returns ``True`` if we should suspend.

        Parameters
        ----------
        value: signal value
            The value reported by a signal callback.
        """
        raise NotImplementedError()

    def should_resume(self, value):
        """
        Returns ``True`` if we should resume.

        Parameters
        ----------
        value: signal value
            The value reported by a signal callback.
        """
        return not self.should_suspend(value)

    def _update(self, *, value, **kwargs):
        """
        Update routine for when the signal's value changes.

        If we're running normally but we should_suspend, we'll trigger the
        suspend state. If suspended but we should_resume, we'll start a timer
        of length sleep to clear the suspend state. This will be interrupted if
        another value change leads us to should_suspend.
        """
        with self._rlock:
            if self._suspend_ev.is_set():
                if self.should_resume(value):
                    self._suspend_ev.clear()
                    self._run_release_thread()
            else:
                if self.should_suspend(value):
                    logger.info('Suspending due to bad %s value=%s',
                                self._sig.name, value)
                    loop = asyncio.get_event_loop()
                    self._ok_future = loop.create_future()
                    self._suspend_ev.set()

    def _run_release_thread(self):
        logger.info('%s suspension is over, waiting for %ss then resuming.',
                    self._sig.name, self._sleep)
        loop = asyncio.get_event_loop()
        loop.run_in_executor(None, self._release_thread)

    def _release_thread(self):
        logger.debug('Worker waiting to release suspender...')
        if self._suspend_ev.wait(timeout=self._sleep):
            logger.debug('Worker canceling suspender release')
        else:
            logger.debug('Worker releasing suspender')
            self._ok_future.set_result('ok')

    def __call__(self, plan):
        """
        Mutate plan to call self._msg_proc on each msg that comes through.
        """
        logger.debug('Running plan with suspender')
        if self._subid is None:
            self._subid = self._sig.subscribe(self._update,
                                              event_type=self._sig.SUB_VALUE)
        try:
            yield from plan_mutator(plan, self._msg_proc)
        finally:
            if self._subid is not None:
                self._sig.unsubscribe(self._subid)
                self._subid = None

    def _msg_proc(self, msg):
        """
        At each msg, decide if we should wait for a suspension to lift.
        """
        logger.debug('enter msg_proc')
        with self._rlock:
            if self._cmd is None or msg.command in self._cmd:
                if not self._ok_future.done() and not self._suspend_active:
                    logger.debug('saw msg_proc(%s), suspend now', msg)

                    def new_gen():
                        self._suspend_active = True
                        yield from wait_for([self._ok_future])
                        self._suspend_active = False
                        logger.info('Resuming plan')
                        yield msg

                    return new_gen(), None
            return None, None


class BeamSuspender(SuspendPreprocessor):
    """
    Suspend readings on beam drop.

    Parameters
    ----------
    beam_stats: ``BeamStats``
        A ``pcdsdevices.beam_stats.BeamStats`` object.

    min_beam: ``float``, optional keyword-only
        The minimum allowable beam level. If the beam average drops below
        this level, we will suspend trigger/create/read events and replay
        unfinished event bundles. The default is 0.1.

    avg: ``int``, optional keyword-only
        The number of gas detector shots to average over.

    sleep: ``int`` or ``float``, optional
        The amount of time to wait after `should_resume` returns ``True``
        before ending the suspension. If `should_suspend` return ``True`` at
        any time during this wait period, we will cancel the resumption and
        return to the suspended state.
    """
    def __init__(self, beam_stats, *, min_beam=0.1, avg=120, sleep=5):
        super().__init__(beam_stats.mj_avg, sleep=sleep,
                         commands=('trigger', 'create', 'read'))
        self.averages = avg
        self.min_beam = min_beam

    def should_suspend(self, value):
        if value < self.min_beam:
            return True
        return False

    @property
    def averages(self, avg):
        return self._sig.averages

    @averages.setter
    def averages(self, avg):
        self._sig.averages = avg


class DropWrapper:
    """
    Replaces ``save`` messages with ``drop`` if the event is bad.

    Parameters
    ----------
    filters: ``dict``, optional
        A dictionary mapping from read key to function of one argument. This
        is an "is_bad_value(value)" function that should return ``True`` if the
        value is bad.

    max_dt: ``float``, optional
        If provided, we'll ``drop`` events if the time from before the first
        read to after the last read is greater than this number.
    """
    def __init__(self, filters=None, max_dt=None):
        self.filters = filters
        self.max_dt = max_dt

    def __call__(self, plan):
        yield from plan_mutator(plan, self._msg_proc)

    def _msg_proc(self, msg):
        if msg.command == 'create':
            self.ret = {}
            self.first_read_time = None
            self.last_read_time = None
            return None, None
        elif msg.command == 'read':
            return self._cache_read(msg), None
        elif msg.command == 'save':
            return self._filter_save(), None

    def _cache_read(self, msg):
        if self.first_read_time is None:
            self.first_read_time = time.time()
        ret = yield msg
        self.ret.update(ret)
        self.last_read_time = time.time()
        return ret

    def _filter_save(self):
        dt = self.last_read_time - self.first_read_time
        if self.max_dt is not None and dt > self.max_dt:
            logger.info(('Event took %ss to bundle, readings are desynced. '
                         'Dropping'), dt)
            return (yield from drop())
        elif self.filters is not None:
            for key, filt in self.filters.items():
                try:
                    value = self.ret[key]
                except KeyError:
                    logger.debug('Read bundle did not have filter key %s', key)
                    value = None
                if value is not None and filt[value]:
                    logger.info('Event had bad value %s=%s. Dropping',
                                key, value)
                    return (yield from drop())
        return (yield from save())


def drop_wrapper(plan, filters, max_dt):
    """
    Replaces ``save`` messages with ``drop`` if the event is bad.

    Parameters
    ----------
    plan: ``plan``
        The plan to wrap.

    filters: ``dict``, optional
        A dictionary mapping from read key to function of one argument. This
        is an "is_bad_value(value)" function that should return ``True`` if the
        value is bad.

    max_dt: ``float``, optional
        If provided, we'll ``drop`` events if the time from before the first
        read to after the last read is greater than this number.
    """
    yield from DropWrapper(filters, max_dt)(plan)


drop_decorator = make_decorator(drop_wrapper)
