"""Portable bounded JavaScript orchestration through a host dispatch lease."""

from .runner import ProgrammaticTool, mount

__amplifier_module_type__ = "tool"
__all__ = ["ProgrammaticTool", "mount"]
