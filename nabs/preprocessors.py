import asyncio
import logging
from threading import RLock, Event

from bluesky.plan_stubs import drop, null, wait_for
from bluesky.preprocessors import plan_mutator

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

    pre_plan: ``plan`` with no arguments, optional
        If provided, we'll follow this plan before waiting for the suspension
        to expire. This can be used to clean up before a suspension.

    post_plan: ``plan`` with no arguments, optional
        If provided, we'll follow this plan before waiting for the suspension
        to expire. This can be used to restore state after a suspension.

    follow_plan: ``plan`` with no arguments, optional
        This is a second post-plan, that in itself can be suspended. This can
        be used by a subclass to retry previous commands before proceeding,
        while allowing these commands to be suspended if conditions change.
    """
    def __init__(self, signal, *, commands=None, sleep=0,
                 pre_plan=null, post_plan=null, follow_plan=null):
        self._sig = signal
        self._cmd = commands
        self._sleep = sleep
        self._pre_plan = pre_plan
        self._post_plan = post_plan
        self._follow_plan = follow_plan
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
        yield from plan_mutator(plan, self._msg_proc)
        if self._subid is not None:
            self._sig.unsubscribe(self._subid)
            self._subid = None

    def _msg_proc(self, msg):
        """
        At each msg, decide if we should wait for a suspension to lift.
        """
        with self._rlock:
            if self._cmd is None or msg.command in self._cmd:
                if not self._ok_future.done() and not self._suspend_active:
                    logger.debug('saw msg_proc(%s), suspend now', msg)

                    def new_gen():
                        self._suspend_active = True
                        yield from self._pre_plan()
                        yield from wait_for([self._ok_future])
                        yield from self._post_plan()
                        logger.info('Resuming plan')
                        self._suspend_active = False
                        yield from self._follow_plan()
                        yield msg

                    return new_gen(), None
            return None, None


class SuspendAndDropReadings(SuspendPreprocessor):
    """
    SuspendPreprocessor that drops and repeats potentially bad readings.

    This can be used to pause a plan during bad conditions, drop open event
    bundles, and redo the readings once conditions return to normal

    Parameters
    ----------
    signal: ``Signal``
        The signal to subscribe to, whose value determines when to suspend.

    sleep: ``int`` or ``float``, optional
        The amount of time to wait after `should_resume` returns ``True``
        before ending the suspension. If `should_suspend` return ``True`` at
        any time during this wait period, we will cancel the resumption and
        return to the suspended state.
    """
    def __init__(self, signal, *, sleep=0):
        super().__init__(signal, commands=('trigger', 'create', 'read'),
                         sleep=sleep, pre_plan=self._try_drop,
                         follow_plan=self._retry)
        self._msg_cache = []

    def _try_drop(self):
        try:
            yield from drop()
        except Exception:
            yield from null()

    def _retry(self):
        for msg in self._msg_cache:
            yield msg

    def _msg_proc(self, msg):
        if msg.command in self._cmd:
            self._msg_cache.append(msg)
        elif msg.command in ('save', 'drop'):
            self._msg_cache = []
        return super()._msg_proc(msg)


class BeamDropSuspender(SuspendAndDropReadings):
    """
    Drop bad readings, suspend on beam drop.

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
        super().__init__(beam_stats.mj_avg, sleep=sleep)
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
