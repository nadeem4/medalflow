from .base import CTEBaseSettings


class ProcessingSettings(CTEBaseSettings):
    """Processing configuration settings.

    Carries only the fields inherited from :class:`CTEBaseSettings`; it exposes
    them under the shared unprefixed environment variables.
    """
