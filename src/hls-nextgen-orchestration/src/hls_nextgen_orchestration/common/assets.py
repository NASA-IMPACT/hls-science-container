from pathlib import Path


# ----- Asset type definitions
# We can't use `isinstance` on generics like `list[str]`, but we can
# define a subclass to allow type hints AND runtime checks.
class Paths(list[Path]): ...
