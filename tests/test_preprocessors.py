import asyncio
import logging

import pytest
from bluesky.plan_stubs import abs_set, sleep
from ophyd.signal import Signal

from nabs.preprocessors import SuspendPreprocessor

logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def sig():
    return Signal(value=0, name='sig')


class NonZeroSuspender(SuspendPreprocessor):
    """
    Suspend on nonzero signal
    """
    def should_suspend(self, value):
        return value


def basic_plan(n, sleep_time):
    for i in range(n):
        yield from sleep(sleep_time)


def wait_for_future(future):
    """
    Use in tests without RE to avoid race conditions
    """
    logger.debug('waiting for future')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait([future], loop=loop))
    logger.debug('done waiting for future')


def assert_suspend_release(plan, susp, sig, follow=True):
    sig.put(1)
    assert susp._suspend_ev.is_set()
    assert next(plan).command == 'null'      # pre_plan
    assert next(plan).command == 'wait_for'  # suspend
    sig.put(0)
    wait_for_future(susp._ok_future)
    assert next(plan).command == 'null'      # post_plan
    if follow:
        assert next(plan).command == 'null'  # follow_plan


def assert_plan_done(plan):
    with pytest.raises(StopIteration):
        next(plan)


@pytest.mark.timeout(3)
def test_suspend_basic(sig):
    logger.debug('test_suspend_basic')
    susp = NonZeroSuspender(sig, sleep=0.1)

    def test_plan():
        yield from basic_plan(3, 1)

    # Dry run
    for msg in susp(test_plan()):
        assert msg.command == 'sleep'

    # Suspend before the second sleep
    plan = susp(test_plan())
    assert next(plan).command == 'sleep'
    assert_suspend_release(plan, susp, sig)
    assert next(plan).command == 'sleep'
    assert next(plan).command == 'sleep'
    assert_plan_done(plan)


@pytest.mark.timeout(3)
def test_suspend_cmd(sig):
    logger.debug('test_suspend_cmd')
    susp = NonZeroSuspender(sig, commands=['set'])

    def test_plan():
        yield from basic_plan(3, 1)
        yield from abs_set(sig, 0)

    # Dry run
    cmds = [msg.command for msg in list(susp(test_plan()))]
    assert cmds == ['sleep', 'sleep', 'sleep', 'set']

    # Trip the suspender immediately, make sure we skip suspending until set
    plan = susp(test_plan())
    sig.put(1)
    for i in range(3):
        assert next(plan).command == 'sleep'
    assert_suspend_release(plan, susp, sig)
    assert next(plan).command == 'set'
    assert_plan_done(plan)


@pytest.mark.timeout(3)
def test_suspend_in_follow_up(sig):
    logger.debug('test_suspend_in_follow_up')
    susp = NonZeroSuspender(sig)

    def test_plan():
        yield from basic_plan(2, 1)

    plan = susp(test_plan())
    assert next(plan).command == 'sleep'
    assert_suspend_release(plan, susp, sig, follow=False)
    # Now we're right before follow-up plan, suspend again
    assert_suspend_release(plan, susp, sig)
    assert next(plan).command == 'null'      # follow_plan 2
    assert next(plan).command == 'sleep'     # back to plan
    assert_plan_done(plan)


def test_needs_override():
    logger.debug('test_needs_override')
    susp = SuspendPreprocessor(None)
    with pytest.raises(NotImplementedError):
        susp.should_suspend(None)
