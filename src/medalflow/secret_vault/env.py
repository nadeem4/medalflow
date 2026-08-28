"""Environment-variable secret provider.

The zero-dependency default. With no Key Vault configured and no cloud SDK
installed, secrets are read from ``MEDALFLOW_SECRET_<NAME>`` environment
variables, so the package is usable out of the box.

Secret names are normalised to a legal environment variable name: uppercased,
with every run of non-alphanumeric characters folded to a single underscore.
``PROCESSED-ADLS-ACCOUNT-KEY`` is therefore read from
``MEDALFLOW_SECRET_PROCESSED_ADLS_ACCOUNT_KEY``.
"""

import os
import re

from pydantic import SecretStr

ENV_PREFIX = "MEDALFLOW_SECRET_"

_UNSAFE_CHARACTERS = re.compile(r"[^A-Z0-9]+")


class EnvSecretProvider:
    """Read secrets from prefixed environment variables.

    Attributes:
        prefix: Environment variable prefix, ``MEDALFLOW_SECRET_`` by default
    """

    def __init__(self, prefix: str = ENV_PREFIX):
        """Initialize the provider.

        Args:
            prefix: Environment variable prefix to read secrets from
        """
        self.prefix = prefix

    def env_var_name(self, secret_name: str) -> str:
        """Environment variable a secret name maps to.

        Args:
            secret_name: Logical secret name (e.g. ``PROCESSED-ADLS-ACCOUNT-KEY``)

        Returns:
            The environment variable name to read
        """
        return f"{self.prefix}{_UNSAFE_CHARACTERS.sub('_', secret_name.upper())}"

    def get_secret(self, secret_name: str, default: str | None = None) -> SecretStr | None:
        """Retrieve a secret from the environment.

        Args:
            secret_name: Name of the secret to retrieve
            default: Value to use when the variable is not set

        Returns:
            SecretStr with the value, or None when unset and no default given
        """
        value = os.environ.get(self.env_var_name(secret_name), default)

        return SecretStr(value) if value is not None else None
