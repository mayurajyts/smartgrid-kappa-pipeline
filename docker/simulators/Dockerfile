# =============================================================================
# Image for the two data sources: the meter simulator and the tariff/weather
# simulator.
#
# Both live in one image because they share `simulators/` — the reference data,
# the solar and load models, and the fault injector. Two images would mean two
# copies of that code and the possibility of them drifting, which is the failure
# mode the shared-library design exists to prevent. Which simulator runs is chosen
# by the compose `command`, not by the image.
# =============================================================================
FROM smartgrid-base:latest

# Application code. `common/` is already installed in the base image.
COPY simulators/ /app/simulators/

# Overridden per service in docker-compose.yml.
CMD ["python", "-m", "simulators.meter_simulator"]
