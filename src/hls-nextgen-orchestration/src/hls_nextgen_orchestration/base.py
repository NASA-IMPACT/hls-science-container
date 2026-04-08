from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar, TypeVar

logger = logging.getLogger(__name__)


class TaskFailure(Exception):
    """Exception raised by a task indicating a specific exit code."""

    def __init__(self, message: str, exit_code: int = 1):
        super().__init__(message)
        self.exit_code = exit_code


# Define a generic type variable for Assets
T = TypeVar("T")


@dataclass(frozen=True)
class Asset[T]:
    """
    Represents a unique identifier for a piece of data with a specific type.

    Attributes
    ----------
    key : str
        The unique string identifier for this asset.
    type_class : Type[T]
        The class type used for runtime validation and static typing.
    """

    key: str
    type_class: type[T]

    def __repr__(self) -> str:
        return f"<{self.key} [{self.type_class.__name__}]>"


type AssetBundle = dict[Asset[Any], Any]


@dataclass
class TaskContext:
    """
    Mutable container for data during execution.
    """

    exit_code: int = 0
    _store: AssetBundle = field(default_factory=dict)

    def put(self, asset: Asset[T], value: T) -> None:
        """
        Store a value for an asset, validating its type at runtime.
        """
        logger.info(f"[Context] Storing {asset.key}")

        # Runtime Type Check
        if not isinstance(value, asset.type_class):
            # Special handling: generic aliases like dict[str, str] or list[int]
            # don't work well with isinstance. We strictly check the origin class.
            # If explicit None is allowed, handle Optional logic here (omitted for strictness).
            raise TypeError(
                f"Asset '{asset.key}' expected type {asset.type_class.__name__}, "
                f"but got {type(value).__name__}: {value}"
            )

        logger.debug(f"          Value: {value}")
        self._store[asset] = value

    def get(self, asset: Asset[T]) -> T:
        """
        Retrieve a value for an asset with type hinting.
        """
        if asset not in self._store:
            raise ValueError(f"Missing dependency data for: {asset.key}")

        val = self._store[asset]
        assert isinstance(val, asset.type_class)
        return val


Assets = tuple[Asset[Any], ...]


@dataclass(frozen=True)
class NodeBase(ABC):
    """
    Base class defines the identity (name) and the interface.
    """

    name: str
    requires: ClassVar[Assets] = ()
    provides: ClassVar[Assets] = ()
    instrument: ClassVar[bool] = False

    @abstractmethod
    def execute(self, context: TaskContext) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"


@dataclass(frozen=True)
class DataSource(NodeBase):
    """
    DataSource generally only uses 'provides'.
    """

    def fetch(self) -> dict[Asset[Any], Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext) -> None:
        logger.info(f"Running DataSource: {self.name}")

        cwd = os.getcwd()
        try:
            results = self.fetch()
        finally:
            os.chdir(cwd)

        for asset in self.provides:
            if asset not in results:
                raise RuntimeError(
                    f"{self.name} failed to provide promised asset: {asset.key}"
                )
            context.put(asset, results[asset])


@dataclass(frozen=True)
class Task(NodeBase):
    """
    Task uses both 'requires' and 'provides'.
    """

    def run(self, inputs: dict[Asset[Any], Any]) -> dict[Asset[Any], Any]:
        raise NotImplementedError

    def execute(self, context: TaskContext) -> None:
        logger.info(f"Running Task: {self.name}")

        # 1. Gather Inputs
        inputs = {asset: context.get(asset) for asset in self.requires}

        # 2. Run Logic
        cwd = os.getcwd()
        try:
            outputs = self.run(inputs)
        finally:
            os.chdir(cwd)

        # 3. Validate & Store Outputs
        for asset in self.provides:
            if asset not in outputs:
                raise RuntimeError(
                    f"{self.name} failed to provide promised output: {asset.key}"
                )
            context.put(asset, outputs[asset])


TMapped = TypeVar("TMapped", bound="MappedTask")
TMerge = TypeVar("TMerge", bound="MergeTask")


@dataclass(frozen=True)
class MappedTask(Task):
    """A Task mapped across granule(s)"""

    granule_id: ClassVar[str]
    requires_factory: ClassVar[Callable[[str], Assets] | None] = None
    provides_factory: ClassVar[Callable[[str], Assets] | None] = None

    @classmethod
    def map(cls: type[TMapped], granule_id: str) -> type[TMapped]:
        """Build a unique Task to process this granule ID"""
        requires = (
            cls.requires_factory(granule_id) if cls.requires_factory else cls.requires
        )
        provides = (
            cls.provides_factory(granule_id) if cls.provides_factory else cls.provides
        )
        return type(
            f"{cls.__name__}-{granule_id}",
            (cls,),
            {"granule_id": granule_id, "requires": requires, "provides": provides},
        )


@dataclass(frozen=True)
class MergeTask(Task):
    """A Task merges outputs from granule(s)"""

    granule_ids: ClassVar[list[str]]
    # Called per _granule_id_
    requires_factory: ClassVar[Callable[[str], Assets] | None] = None

    @classmethod
    def merge(cls: type[TMerge], granule_ids: list[str]) -> type[TMerge]:
        """Build a unique Task to process this granule ID"""
        if cls.requires_factory:
            requires = tuple(
                require
                for granule_id in granule_ids
                for require in cls.requires_factory(granule_id)
            )
        else:
            requires = cls.requires

        return type(
            f"{cls.__name__}-Merged",
            (cls,),
            {"granule_ids": granule_ids, "requires": requires},
        )
