"""The installed version, read from distribution metadata.

Not hardcoded: ``medalflow.__version__`` and the version the installer
resolved cannot disagree if only one of them exists. The fallback covers the
package being importable without being installed -- ``src`` on the path,
which is how the tests run -- where there is no metadata to read.
"""

from importlib import metadata

try:
    __version__ = metadata.version("medalflow")
except metadata.PackageNotFoundError:  # local dev without install
    __version__ = "0.1.0.dev0"
