# Image Spark du projet : apache/spark 3.5.3 + bibliotheques Python
# necessaires a Spark ML (numpy) et aux pandas UDF (pandas, pyarrow)
FROM apache/spark:3.5.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Retour a l'utilisateur non privilegie de l'image officielle
USER spark