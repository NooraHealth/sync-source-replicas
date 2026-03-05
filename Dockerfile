FROM rocker/r-ver:4.5

# System dependencies (add more if needed)
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libpq-dev \
    libxml2-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy renv files and restore packages
COPY renv.lock renv.lock
COPY renv/ renv/

# Install renv
RUN R -e "install.packages('renv', repos='https://cloud.r-project.org')"

# Restore packages from renv.lock
RUN R -e "renv::restore(prompt = FALSE)"

# Copy R dependency file first (for caching)
COPY params /app/params
COPY R /app/R
COPY secrets /app/secrets


# Default command
CMD ["Rscript", "R/sync_hrpw_database.R"]
