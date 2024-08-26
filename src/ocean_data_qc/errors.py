class FysKemQcError(Exception):
    pass


class InputDataError(FysKemQcError):
    pass


class QcFlagTupleError(FysKemQcError):
    pass


class MetadataQcError(Exception):
    pass


class VisitError(MetadataQcError):
    pass
