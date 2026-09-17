FROM ghcr.io/ministryofjustice/hmpps-python:python3.13-alpine AS base

WORKDIR /app

# dependencies
COPY includes includes
COPY processes processes
COPY utilities utilities

# initialise uv
COPY pyproject.toml .
RUN uv sync

# Copy the Python goodness
COPY ./*.py .

CMD [ "uv", "run", "python", "-u", "github_discovery.py" ]
