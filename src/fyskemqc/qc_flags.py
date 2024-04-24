from dataclasses import dataclass, field
from typing import Sequence

from fyskemqc.qc_flag import QcFlag
from fyskemqc.qc_flag_tuple import QcField, QcFlagTuple


@dataclass
class QcFlags:
    incoming: QcFlag = QcFlag.NO_QC_PERFORMED
    _automatic: QcFlagTuple = field(
        default_factory=lambda: QcFlagTuple((QcFlag.NO_QC_PERFORMED,))
    )
    manual: QcFlag = QcFlag.NO_QC_PERFORMED

    def __post_init__(self):
        self.incoming = self.incoming or QcFlag.NO_QC_PERFORMED
        self._automatic = self.automatic or QcFlagTuple(
            (QcFlag.NO_QC_PERFORMED,) * len(QcField)
        )
        self.manual = self.manual or QcFlag.NO_QC_PERFORMED

    def get_field(self, field_name: QcField):
        return self.automatic[field_name]

    @property
    def automatic(self):
        return self._automatic

    @automatic.setter
    def automatic(self, value: Sequence):
        self._automatic = QcFlagTuple(value)

    @property
    def total(self):
        if self.manual:
            return self.manual

        # Get the total value for all automatic flags
        automatic = min(
            self._automatic, key=QcFlag.key_function, default=QcFlag.NO_QC_PERFORMED
        )

        # Remove NO_QC_PERFORMED and return the worst remaining value
        return min(
            [flag for flag in (self.incoming, automatic) if flag],
            key=QcFlag.key_function,
            default=QcFlag.NO_QC_PERFORMED,
        )

    def __str__(self):
        return (
            f"{self.incoming.value}_"
            f"{''.join(str(flag.value) for flag in self.automatic)}_"
            f"{self.manual.value}"
        )

    @classmethod
    def from_string(cls, value: str):
        if not value:
            return cls()

        incoming, automatic, manual = value.split("_")

        incoming = QcFlag(int(incoming))
        automatic = QcFlagTuple(QcFlag(flag) for flag in map(int, automatic))
        manual = QcFlag(int(manual))

        return cls(incoming, automatic, manual)
