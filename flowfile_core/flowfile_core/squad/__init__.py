"""Squad — a tiny, dependency-free harness for orchestrating future agents.

A *squad* is a named group of *members* (think: future AI agents, workers, or
plug-in handlers) that a task can be dispatched to. It deliberately ships no
provider/transport wiring so each major Flowfile package can grow its own
concrete implementation on top of the same shared shape.

This module is a template/scaffold: it has no side effects on import, pulls in
nothing beyond the stdlib, and is intentionally *not* wired into the rest of
``flowfile_core``. Extend ``SquadMember.handle`` (or register richer members)
when you are ready to give the squad real behaviour.

Example
-------
>>> squad = Squad("etl-helpers")
>>> squad.add(SquadMember("echo", role="repeat the task back"))
>>> squad.dispatch("profile the orders table")
[SquadResult(member='echo', output='profile the orders table')]
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
