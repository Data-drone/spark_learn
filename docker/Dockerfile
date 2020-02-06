FROM datadrone/deeplearn_pytorch

USER root

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pyspark toree

USER $NB_USER