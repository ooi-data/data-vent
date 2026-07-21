class DataNotReadyError(Exception):
    pass


class DuplicateTimeStampError(Exception):
    pass


class NullMetadataError(Exception):
    pass


class StreamNotFoundError(Exception):
    pass


class MissingDataError(Exception):
    pass


class DimensionChangedError(Exception):
    pass


class RefreshRequestInAppendModeError(Exception):
    pass
