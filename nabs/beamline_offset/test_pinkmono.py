from pathlib import Path

from ophyd import Component as Cpt, Device, Signal
from pcdsdevices.interface import setup_preset_paths
from pcdsdevices.sim import FastMotor

from pinkmono import (beamline_to_mono, beamline_to_pink,
                      beamline_mono_offsets)


class TestStates(Device):
    motor = Cpt(FastMotor)

    def __init__(self, prefix, *, name, inserted=True, **kwargs):
        self.inserted = bool(inserted)
        super().__init__(self, prefix, name=name, **kwargs)


def setup_test_presets():
    presets = Path(__file__).abspath.parent / 'test_presets'
    setup_presets_path(hutch=presets)


def get_test_hardware():
    ipm = TestStates('IPM', name='ipm', inserted=True)
    pim = TestStates('PIM', name='pim', inserted=True)
    out = TestStates('OUT', name='out', inserted=False)
    return dict(ipm=ipm, pim=pim, out=out)


def setup_test_offsets(offsets):
    beamline_mono_offsets.update(offsets)


if __name__ == '__main__':
    setup_test_presets()
    hw = get_test_hardware()
    setup_test_offsets(hw)
    globals().update(hw)
