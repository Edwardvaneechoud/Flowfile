"""Squad — a tiny, dependency-free harness for orchestrating future agents.

A *squad* is a named group of *members* (think: future AI agents, pipeline
builders, or helper handlers) that a task can be dispatched to. It ships no
Polars/FlowFrame wiring so the frame package can grow a concrete implementation
on top of the same shared shape used across the other Flowfile packages.

This module is a template/scaffold: it has no side effects on import, depends
only on the stdlib, and is intentionally *not* exported from
``flowfile_frame.__init__`` (so it does not affect the public API stubs).
Override :meth:`SquadMember.handle` when you are ready to give a member real
behaviour.

Example
-------
>>> squad = Squad("frame-builders")
>>> squad.add(SquadMember("echo", role="repeat the task back"))
>>> squad.dispatch("build the orders pipeline")
[SquadResult(member='echo', output='build the orders pipeline')]
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["Squad", "SquadMember", "SquadResult"]


@dataclass
class SquadResult:
    """Outcome of a single member handling a task."""

    member: str
    output: str


@dataclass
class SquadMember:
    """A single unit of a squad.

    Subclass or override :meth:`handle` to give the member real behaviour. The
    default implementation simply echoes the task so the harness is runnable
    out of the box.
    """

    name: str
    role: str = ""

    def handle(self, task: str) -> str:
        """Process ``task`` and return a result string. Override me."""
        return task


@dataclass
class Squad:
    """An ordered, named collection of :class:`SquadMember` units."""

    name: str
    members: list[SquadMember] = field(default_factory=list)

    def add(self, member: SquadMember) -> Squad:
        """Append ``member`` and return ``self`` for chaining."""
        self.members.append(member)
        return self

    def dispatch(self, task: str) -> list[SquadResult]:
        """Send ``task`` to every member, preserving order."""
        return [SquadResult(member=m.name, output=m.handle(task)) for m in self.members]
