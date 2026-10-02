"""Exception types for the dataset version repository."""


class DVError(Exception):
    """Base class for all repository errors."""


class InvalidSchema(DVError):
    """A schema failed validation at publish time."""


class PublishRejected(DVError):
    """publish_version rejected the new schema version."""


class InvalidRecord(DVError):
    """A record does not conform to the latest schema version."""


class PushRejected(DVError):
    """push_records rejected the batch (unreadable at a pinned version)."""


class UnknownDataset(DVError):
    """The dataset does not exist or has no published version."""


class NotPinned(DVError):
    """The consumer has not pinned the dataset yet."""
