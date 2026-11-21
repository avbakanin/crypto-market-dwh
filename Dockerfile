# syntax=docker/dockerfile:1
#
# The official Airflow image plus this project. Airflow keeps its own tested
# environment (its constraints pin snowflake-connector-python 4.8.1); the
# project runs from a separate venv built from the same pinned requirements
# as the local .venv, so ingestion and dbt behave exactly as on the host.
#
#   docker build --target runtime -t crypto-market-dwh:dev .
#   docker build --target test -t crypto-market-dwh:test . && docker run --rm crypto-market-dwh:test

ARG AIRFLOW_IMAGE=apache/airflow:3.4.0-python3.12

FROM ${AIRFLOW_IMAGE} AS runtime

ENV PROJECT_HOME=/opt/project \
    PROJECT_VENV=/opt/project-venv

# /opt belongs to root; everything below it belongs to the airflow user, and
# pip runs as that user (the image refuses pip installs as root).
USER root
RUN mkdir -p "${PROJECT_HOME}" "${PROJECT_VENV}" \
    && chown airflow:0 "${PROJECT_HOME}" "${PROJECT_VENV}"
USER airflow

# Dependencies before code: this layer is rebuilt only when the pins change.
# The venv comes from the base interpreter, not from Airflow's own venv.
# dbt-core-experimental-parser is an sdist whose build step downloads its
# wheel from GitHub releases, so the build needs github.com as well as PyPI.
COPY --chown=airflow:0 requirements.txt requirements-dbt.txt /tmp/requirements/
RUN /usr/python/bin/python -m venv "${PROJECT_VENV}" \
    && "${PROJECT_VENV}/bin/pip" install --no-cache-dir --disable-pip-version-check \
        -r /tmp/requirements/requirements-dbt.txt \
    && "${PROJECT_VENV}/bin/pip" check \
    && python -m pip check

COPY --chown=airflow:0 . ${PROJECT_HOME}
RUN cd "${PROJECT_HOME}" && "${PROJECT_VENV}/bin/dbt" deps --project-dir dbt


# Linux check of the same code: pytest, ruff and pyright on top of runtime.
FROM runtime AS test

RUN "${PROJECT_VENV}/bin/pip" install --no-cache-dir --disable-pip-version-check \
        -r "${PROJECT_HOME}/requirements-dev.txt" \
    && "${PROJECT_VENV}/bin/python" -m pyright --version

# pyproject.toml points pyright at ./.venv; the link makes that path the project venv.
RUN ln -s "${PROJECT_VENV}" "${PROJECT_HOME}/.venv"

WORKDIR ${PROJECT_HOME}
ENTRYPOINT ["/opt/project-venv/bin/python", "-m"]
CMD ["pytest", "-q"]
