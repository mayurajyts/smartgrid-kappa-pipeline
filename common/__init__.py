"""Shared library imported by every service in the platform.

Lives at the repo root (not inside any one service) because the Kappa claim
depends on the simulators, the Spark jobs, the loader and the API all agreeing
on exactly one definition of the config, the log shape, the data contracts and
the simulated clock. A second copy of any of those is a second source of truth,
which is the bug class this architecture exists to avoid.
"""
