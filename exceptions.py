#custom exceptions
from cinder.i18n import _
from cinder import exception

class PureDriverException(exception.VolumeDriverException):
    message = _("Pure Storage Cinder driver failure: %(reason)s")


class PureRetryableException(exception.VolumeBackendAPIException):
    message = _("Retryable Pure Storage Exception encountered")

