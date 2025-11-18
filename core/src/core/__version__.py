from importlib import metadata

try:
    __version__ = metadata.version("medalflow")
except metadata.PackageNotFoundError:  # local dev without install
    __version__ = "0.0.dev0"