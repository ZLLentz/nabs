"""
nabs.plans

Much like bluesky.plans, this module contain full standalone plans that can be
used to take full individual runs using a RunEngine.

Plans preceded by "daq_" incorporate standard daq step scan args and behavior.
"""
import time
from collections import defaultdict

import bluesky.plan_stubs as bps
import bluesky.plans as bp
import bluesky.preprocessors as bpp

from .preprocessors import daq_during_wrapper, daq_step_scan_decorator


def infinite_scan(detectors, motor, points, duration=None,
                  per_step=None, md=None):
    """
    Bluesky plan that moves a motor among points until interrupted.

    Parameters
    ----------
    detectors: list of readables
        Objects to read into Python in the scan.

    motor: settable
        Object to move in the scan.

    points: list of floats
        Positions to move between in the scan.

    duration: float
        If provided, the time to run in seconds. If omitted, we'll run forever.
    """
    if per_step is None:
        per_step = bps.one_nd_step

    if md is None:
        md = {}

    md.update(motors=[motor.name])
    start = time.time()

    # @bpp.stage_decorator(list(detectors) + [motor])
    @bpp.reset_positions_decorator()
    @bpp.run_decorator(md=md)
    def inner():
        # Where last position is stored
        pos_cache = defaultdict(lambda: None)
        while duration is None or time.time() - start < duration:
            for pt in points:
                step = {motor: pt}
                yield from per_step(detectors, step, pos_cache)

    return (yield from inner())


def delay_scan(daq, time_motor, time_points, sweep_time, duration=None):
    """
    Bluesky plan that sets up and executes the delay scan.

    Parameters
    ----------
    daq: Daq
        The daq

    time_motor: DelayNewport
        The movable device in seconds

    time_points: list of float
        The times in second to move between

    sweep_time: float
        The duration we take to move from one end of the range to the other.

    duration: float
        If provided, the time to run in seconds. If omitted, we'll run forever.
    """

    spatial_pts = []
    for time_pt in time_points:
        pseudo_tuple = time_motor.PseudoPosition(delay=time_pt)
        real_tuple = time_motor.forward(pseudo_tuple)
        spatial_pts.append(real_tuple.motor)

    space_delta = abs(spatial_pts[0] - spatial_pts[1])
    velo = space_delta/sweep_time

    yield from bps.abs_set(time_motor.motor.velocity, velo)

    scan = infinite_scan([], time_motor, time_points, duration=duration)

    if daq is not None:
        yield from daq_during_wrapper(scan)
    else:
        yield from bp.scan


def daq_delay_scan():
    raise NotImplementedError  # TODO


# The bottom of this file contains thin wrappers around bluesky built-ins
# These exist to mimic an older hutch python API,
# easing the transition to bluesky

# TODO just count with docstring and DAQ
# TODO n-dimensional ascan with docstring and DAQ
# TODO n-dimensional list scan with docstring and DAQ
# ^ These three cover 99.9% of use-cases
# The rest are just for full legacy familiarity

@bpp.reset_positions_decorator
@daq_step_scan_decorator
def daq_ascan(motor, start, end, nsteps):
    """
    One-dimensional daq scan with absolute positions.

    This moves a motor from start to end in nsteps steps, taking data in the
    DAQ at every step, and returning the motor to its original position at
    the end of the scan.

    Parameters
    ----------
    motor : Movable
        A movable object to scan.

    start : int or float
        The first point in the scan.

    end : int or float
        The last point in the scan.

    nsteps : int
        The number of points in the scan.

    events : int, optional
        Number of events to take at each step. If omitted, uses the
        duration argument or the last configured value.

    duration : int or float, optional
        Duration of time to spend at each step. If omitted, uses the events
        argument or the last configured value.

    record : bool, optional
        Whether or not to record the run in the DAQ. Defaults to True because
        we don't want to accidentally skip recording good runs.

    use_l3t : bool, optional
        Whether or not the use the l3t filter for the events argument. Defaults
        to False to avoid confusion from unconfigured filters.
    """

    yield from bp.scan([], motor, start, end, nsteps)


@bpp.relative_set_decorator
@bpp.reset_positions_decorator
@daq_step_scan_decorator
def daq_dscan(motor, start, end, nsteps):
    """
    One-dimensional daq scan with relative (delta) positions.

    This moves a motor from current_pos + start to current_pos + end
    in nsteps steps, taking data in the DAQ at every step, and
    returning the motor to its original position at the end of the scan.

    Parameters
    ----------
    motor : Movable
        A movable object to scan.

    start : int or float
        The first point in the scan, relative to the current position.

    end : int or float
        The last point in the scan, relative to the current position.

    nsteps : int
        The number of points in the scan.

    events : int, optional
        Number of events to take at each step. If omitted, uses the
        duration argument or the last configured value.

    duration : int or float, optional
        Duration of time to spend at each step. If omitted, uses the events
        argument or the last configured value.

    record : bool, optional
        Whether or not to record the run in the DAQ. Defaults to True because
        we don't want to accidentally skip recording good runs.

    use_l3t : bool, optional
        Whether or not the use the l3t filter for the events argument. Defaults
        to False to avoid confusion from unconfigured filters.
    """

    yield from bp.scan([], motor, start, end, nsteps)


@bpp.reset_positions_decorator
@daq_step_scan_decorator
def daq_a2scan(m1, a1, b1, m2, a2, b2, nsteps):
    """
    Two-dimensional daq scan with absolute positions.

    This moves two motors from start to end in nsteps steps, taking data in
    the DAQ at every step, and returning the motors to their original positions
    at the end of the scan.

    Parameters
    ----------
    m1 : Movable
        The first movable object to scan.

    a1 : int or float
        The first point in the scan for m1.

    b1 : int or float
        The last point in the scan for m1.

    m2 : Movable
        The second movable object to scan.

    a2 : int or float
        The first point in the scan for m2.

    b2 : int or float
        The last point in the scan for m2.

    nsteps : int
        The number of points in the scan.

    events : int, optional
        Number of events to take at each step. If omitted, uses the
        duration argument or the last configured value.

    duration : int or float, optional
        Duration of time to spend at each step. If omitted, uses the events
        argument or the last configured value.

    record : bool, optional
        Whether or not to record the run in the DAQ. Defaults to True because
        we don't want to accidentally skip recording good runs.

    use_l3t : bool, optional
        Whether or not the use the l3t filter for the events argument. Defaults
        to False to avoid confusion from unconfigured filters.
    """

    yield from bp.scan([], m1, a1, b1, m2, a2, b2, nsteps)


@bpp.reset_positions_decorator
@daq_step_scan_decorator
def daq_a3scan(m1, a1, b1, m2, a2, b2, m3, a3, b3, nsteps):
    """
    Three-dimensional daq scan with absolute positions.

    This moves three motors from start to end in nsteps steps, taking data in
    the DAQ at every step, and returning the motors to their original positions
    at the end of the scan.

    Parameters
    ----------
    m1 : Movable
        The first movable object to scan.

    a1 : int or float
        The first point in the scan for m1.

    b1 : int or float
        The last point in the scan for m1.

    m2 : Movable
        The second movable object to scan.

    a2 : int or float
        The first point in the scan for m2.

    b2 : int or float
        The last point in the scan for m2.

    m3 : Movable
        The third movable object to scan.

    a3 : int or float
        The first point in the scan for m3.

    b3 : int or float
        The last point in the scan for m3.

    nsteps : int
        The number of points in the scan.

    events : int, optional
        Number of events to take at each step. If omitted, uses the
        duration argument or the last configured value.

    duration : int or float, optional
        Duration of time to spend at each step. If omitted, uses the events
        argument or the last configured value.

    record : bool, optional
        Whether or not to record the run in the DAQ. Defaults to True because
        we don't want to accidentally skip recording good runs.

    use_l3t : bool, optional
        Whether or not the use the l3t filter for the events argument. Defaults
        to False to avoid confusion from unconfigured filters.
    """

    yield from bp.scan([], m1, a1, b1, m2, a2, b2, m3, a3, b3, nsteps)


@bpp.reset_positions_decorator
@daq_step_scan_decorator
def daq_list_scan(motor, pos_list):
    """
    One-dimensional daq scan with a list of positions

    This moves a motor through pos_list, taking data in the
    DAQ at every step, and returning the motor to its original position at
    the end of the scan.

    Parameters
    ----------
    motor : Movable
        A movable object to scan.

    pos_list : list of int or float
        The points to include in the scan.

    events : int, optional
        Number of events to take at each step. If omitted, uses the
        duration argument or the last configured value.

    duration : int or float, optional
        Duration of time to spend at each step. If omitted, uses the events
        argument or the last configured value.

    record : bool, optional
        Whether or not to record the run in the DAQ. Defaults to True because
        we don't want to accidentally skip recording good runs.

    use_l3t : bool, optional
        Whether or not the use the l3t filter for the events argument. Defaults
        to False to avoid confusion from unconfigured filters.
    """

    yield from bp.list_scan([], motor, pos_list)
