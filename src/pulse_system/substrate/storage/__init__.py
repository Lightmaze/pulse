from .store import Storage
from .migrator import SchemaMigrationError, SchemaMigrator


def __getattr__(name: str):
    if name == "CausalLedger":
        from pulse_system.core.causality import CausalLedger

        return CausalLedger
    raise AttributeError(name)


__all__ = ["Storage", "CausalLedger", "SchemaMigrationError", "SchemaMigrator"]
