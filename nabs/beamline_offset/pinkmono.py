"""
Goal:
- moving to mono twice shouldn't repeat successful moves
- different sessions should agree as to whether we have done the pink/mono move
- removed devices should be ignored
- should be able to return devices to pink position

* print and log all moves: start, goal, end, note failures


user-facing:

beamline_mono_offsets <dict of states_device:offset, special key "default"
                       and special value "default">
- contains state devices with motors attached, mapped to offset value or None
- 'default' means the default offset
- special key 'default' for the default offset
- in general, string values reference other keys

beamline_to_mono()
- sync with presets database
- beamline_save_pink_ref()
- move all inserted to "mono_ref" preset

beamline_to_pink()
- sync with presets database
- for all inserted motors with "pink_ref" state, move to "pink_ref" state
- beamline_clear_pink_ref() for successful moves

beamline_save_pink_ref()
- for all inserted motors that do not have an active "pink_ref" preset, save current position as "pink_ref" and an offset position as "mono_ref"

beamline_clear_pink_ref(<opts>)
- deactivate all/some "pink_ref" and "mono_ref" presets
"""
import logging

logger = logging.getLogger(__name__)
beamline_mono_offsets = {'default': 7.5}

def beamline_to_mono(offsets=None):
    if offsets is None:
        offsets = beamline_mono_offsets

    devices = [dev for dev in offsets.keys() if not isinstance(dev, str)]

    logger.info('Synchronizing preset database')
    preset_sync(devices)
    logger.info('Saving pink references')
    beamline_save_pink_ref(offsets=offsets)

    moved_motors = []
    for dev in devices:
        if dev.inserted:
            motor = find_motor(dev)
            try:
                motor.mv_mono_ref()
                moved_motors.append(motor)
            except AttributeError:
                pass
    if not moved_motors:
        logger.info('No motors to move, exiting')
        return
    logger.info('Waiting for motor moves to complete')
    for motor in moved_motors:
        try:
            motor.wait()
        except AttributeError:
            logger.warning('Motor %s does not have a wait method.', motor.name)
    logger.info('Done')


def beamline_to_pink(offsets=None):
    if offsets is None:
        offsets = beamline_mono_offsets

    devices = [dev for dev in offsets.keys() if not isinstance(dev, str)]

    logger.info('Synchronizing preset database')
    preset_sync(devices)

    moved_motors = []
    for dev in devices:
        if dev.inserted:
            motor = find_motor(dev)
            try:
                motor.mv_pink_ref()
                moved_motors.append(motor)
            except AttributeError:
                pass
    if not moved_motors:
        logger.info('No motors to move, exiting')
        return
    logger.info('Waiting for motor moves to complete')
    for motor in moved_motors:
        try:
            motor.wait()
        except AttributeError:
            logger.warning('Motor %s does not have a wait method.', motor.name)
    logger.info('Clearing previous pink references')
    beamline_clear_pink_ref(moved_motors)


def beamline_save_pink_ref(offsets=None):
    if offsets is None:
        offsets = beamline_mono_offsets

    for dev, offset in offsets.items():
        # Skip position group values
        if isinstance(dev, str):
            logger.info('Skipping string device %s', dev)
            continue
        # Skip removed devices
        if not dev.inserted:
            logger.info('Skipping not-inserted device %s', dev.name)
            continue
        # Skip devices with an active pink_ref
        motor = find_motor(dev)
        try:
            motor.presets.positions.pink_ref
            logger.info('Skipping device with active pink ref %s', dev.name)
            continue
        except AttributeError:
            pass
        # Identify which position group we use
        while isinstance(offset, str):
            offset = offsets[offset]
        # Save the presets
        pink = motor.position
        mono = pink + offset
        text = 'For %s, saving pink_ref=%s, mono_ref=%s'
        logger.info(text, motor.name, pink, mono)
        motor.presets.add_hutch('pink_ref', pink,
                                'automatic preset for ccm beamline shift')
        motor.presets.add_hutch('mono_ref', mono,
                                'automatic preset for ccm beamline shift')

def beamline_clear_pink_ref(motors=None):
    if motors is None:
        motors = [find_motor(dev) for dev in beamline_mono_offsets.keys()
                  if not isinstance(dev, str)]

    for motor in motors:
        for preset_name in ('pink_ref', 'mono_ref'):
            try:
                preset_obj = getattr(motor.presets.positions, preset_name)
                text = 'Clearing old pink_ref and mono_ref on %s'
                logger.info(text, motor.name)
                preset_obj.deactivate()
            except AttributeError:
                pass


def preset_sync(devices):
    for states_device in devices:
        motor = find_motor(states_device)
        motor.presets.sync()


candidate_attrs = ('motor', 'y_motor', 'states_motor')


def find_motor(device):
    for attr in candidate_attrs:
        try:
            return getattr(device, attr)
        except Exception:
            pass
    raise TypeError(f'Device {device} has no motor!')
