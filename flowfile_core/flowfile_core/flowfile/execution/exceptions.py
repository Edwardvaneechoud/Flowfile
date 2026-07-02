"""Typed errors for core -> worker communication."""

import requests


class WorkerError(Exception):
    """Base class for worker-communication failures."""


class WorkerConnectionError(WorkerError, requests.exceptions.ConnectionError):
    """The worker service is unreachable.

    Also a ``requests.ConnectionError`` so pre-existing
    ``except requests.RequestException`` call sites keep catching it.
    """


class WorkerTaskError(WorkerError):
    """The worker was reachable but the task request failed."""


class TaskCancelledError(WorkerError):
    """The task was cancelled before it completed."""
