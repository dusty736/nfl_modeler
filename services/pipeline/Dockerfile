FROM rocker/r-ver:4.4.1

# System deps
RUN apt-get update && apt-get install -y \
    python3 python3-pip python3-venv \
    libpq-dev libssl-dev libxml2-dev libcurl4-opens-dev \
    libfontconfig1-dev libfreetype6-dev \
    git make g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

# R deps via renv (if you use renv)
RUN R -q -e 'install.packages("renv", repos="https://cloud.r-project.org"); renv::consent(provided=TRUE); if (file.exists("renv.lock")) renv::restore(lockfile="renv.lock", prompt=FALSE) else message("No renv.lock; skipping renv::restore")'

# Python deps
RUN python3 -m pip install --upgrade pip && \
    if [ -f modeling/Python/requirements.txt ]; then pip install -r modeling/Python/requirements.txt; fi

# The entrypoint script is now at /app/services/pipeline/entrypoint.sh
COPY services/pipeline/entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]