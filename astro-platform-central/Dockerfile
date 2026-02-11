FROM astrocrpublic.azurecr.io/runtime:3.1-11
 
COPY packages.txt .
USER root
RUN apt-get update && xargs -r -a packages.txt apt-get install -y && rm -rf /var/lib/apt/lists/*
USER astro
# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt