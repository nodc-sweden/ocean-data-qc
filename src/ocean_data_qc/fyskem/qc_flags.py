from dataclasses import dataclass, field
from typing import Sequence

from ocean_data_qc.fyskem.qc_flag import QcFlag
from ocean_data_qc.fyskem.qc_flag_tuple import QcField, QcFlagTuple


@dataclass
class QcFlags:
    _incoming: QcFlag = QcFlag.NO_QUALITY_CONTROL
    _automatic: QcFlagTuple = field(
        default_factory=lambda: QcFlagTuple((QcFlag.NO_QUALITY_CONTROL,) * len(QcField))
    )
    _manual: QcFlag = QcFlag.NO_QUALITY_CONTROL
    _total: QcFlag = QcFlag.NO_QUALITY_CONTROL

    def __post_init__(self):
        self._incoming = self._incoming or QcFlag.NO_QUALITY_CONTROL
        self._automatic = self._automatic or QcFlagTuple(
            (QcFlag.NO_QUALITY_CONTROL,) * len(QcField)
        )
        self._manual = self._manual or QcFlag.NO_QUALITY_CONTROL
        self._update_total()

    def get_field(self, field_name: QcField):
        return self.automatic[field_name]

    def get_field_name(self, field: int):
        return QcField(field).name

    @property
    def incoming(self) -> QcFlag:
        return self._incoming

    @incoming.setter
    def incoming(self, value: QcFlag):
        self._incoming = value
        self._update_total()

    @property
    def automatic(self) -> QcFlagTuple:
        return self._automatic

    @automatic.setter
    def automatic(self, value: Sequence):
        self._automatic = QcFlagTuple(value)
        self._update_total()

    @property
    def total_automatic(self) -> QcFlag:
        return min(
            self._automatic,
            key=QcFlag.key_function,
            default=QcFlag.NO_QUALITY_CONTROL,
        )

    @property
    def total_automatic_source(self):
        min_flag = self.total_automatic
        return [
            field for field, flag in zip(QcField, self._automatic) if flag == min_flag
        ]

    @property
    def total_automatic_name(self):
        min_flag = self.total_automatic
        return [
            QcField(field).name
            for field, flag in zip(QcField, self._automatic)
            if flag == min_flag
        ]

    @property
    def manual(self) -> QcFlag:
        return self._manual

    @manual.setter
    def manual(self, value: QcFlag):
        self._manual = value
        self._update_total()

    @property
    def total(self) -> QcFlag:
        return self._total

    def _update_total(self):
        if self.manual != QcFlag.NO_QUALITY_CONTROL:
            self._total = self.manual
            return

        flags = [self.incoming, *list(self.automatic)]

        self._total = min(
            flags,
            key=QcFlag.key_function,
            default=QcFlag.NO_QUALITY_CONTROL,
        )

    def __str__(self):
        return (
            f"{self.incoming.value}_"
            f"{''.join(str(flag.value) for flag in self.automatic)}_"
            f"{self.manual.value}_"
            f"{self.total.value}"
        )

    @classmethod
    def from_string(cls, value: str):
        if not value:
            return cls()

        incoming, automatic, manual, _ = value.split("_")

        incoming = QcFlag.parse(incoming)

        automatic = QcFlagTuple(QcFlag.parse(flag) for flag in automatic)

        manual = QcFlag.parse(manual)

        return cls(incoming, automatic, manual)


if __name__ == "__main__":
    qcflags = QcFlags().from_string("0_0400400000_0_0")
    print(f"qcflags {qcflags:}")
    print(f"qcflags.automatic {qcflags.automatic:}")
    total_flag_index = min(
        enumerate([flag for flag in (qcflags.automatic)]),
        key=lambda x: QcFlag.key_function(x[1]),
        default=(None, QcFlag.NO_QUALITY_CONTROL),
    )[0]
    print(f"QcField(total_flag_index).name: {QcField(total_flag_index).name:}")
    print(f"qcflags.total_automatic: {qcflags.total_automatic:}")
