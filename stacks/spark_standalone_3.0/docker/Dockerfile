FROM datadrone/deeplearn_pytorch:latest

USER root

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

# setup scala
ENV SPARK_VERSION=3.1.1
ENV HADOOP_VERSION=3.2
ENV SCALA_VERSION=2.12.13
ENV SBT_VERSION=1.5.1
ENV SCALA_HOME=/usr/share/scala
ENV SPARK_HOME=/spark

RUN cd "/tmp" && \
    wget --no-verbose "https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    mkdir "${SCALA_HOME}" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*

#Scala instalation
RUN export PATH="/usr/local/sbt/bin:$PATH" &&  apt -y update && \
    apt -y install ca-certificates wget tar && mkdir -p "/usr/local/sbt" && \
    wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && sbt sbtVersion

RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME='/home/jovyan/spark/'

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED 1

## Need to add a conda env flag
RUN conda create --name spark python=3.8 pyspark==3.1.1 conda-pack

SHELL ["conda", "run", "-n", "spark", "/bin/bash", "-c"]

# this isn't installing in the right folder
#RUN pip install pyspark==3.1.1 conda-pack

# lock in high versions
# main for Deep Learning packages and issues from that
RUN conda config --set channel_priority false


## from jupyter stacks
# Install pyarrow
RUN conda install --quiet -y 'pyarrow' && \
    conda clean --all -f -y

# R packages
RUN conda install --quiet --yes \
    'r-base=4.0.3' \
    'r-ggplot2=3.3*' \
    'r-irkernel=1.1*' \
    'r-rcurl=1.98*' \
    'r-sparklyr=1.6*' \
    && \
    conda clean --all -f -y

# Apache Toree kernel
# hadolint ignore=DL3013
# Toree doesn't support Spark 3 currently
#RUN pip install --no-cache-dir \
#    https://dist.apache.org/repos/dist/release/incubator/toree/0.3.0-incubating/toree-pip/toree-0.3.0.tar.gz \
#    && \
#    jupyter toree install --sys-prefix && \
#    rm -rf "/home/${NB_USER}/.local"

# Spylon-kernel
RUN conda install --quiet --yes 'spylon-kernel=0.4*' && \
    conda clean --all -f -y && \
    python -m spylon_kernel install --sys-prefix && \
    rm -rf "/home/${NB_USER}/.local"

# Add repids 0.5.0
RUN mkdir /opt/sparkRapidsPlugin
RUN wget -O /opt/sparkRapidsPlugin/rapids-4-spark_2.12-0.5.0.jar https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.5.0/rapids-4-spark_2.12-0.5.0.jar
RUN wget -O /opt/sparkRapidsPlugin/cudf-0.19.2-cuda11.jar https://repo1.maven.org/maven2/ai/rapids/cudf/0.19.2/cudf-0.19.2-cuda11.jar
RUN wget -O /opt/sparkRapidsPlugin/getGpusResources.sh https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh

RUN chmod u+x /opt/sparkRapidsPlugin/getGpusResources.sh \
      && chmod u+x /opt/sparkRapidsPlugin/rapids-4-spark_2.12-0.5.0.jar \
      && chmod u+x /opt/sparkRapidsPlugin/cudf-0.19.2-cuda11.jar

RUN chown jovyan:1000 /opt/sparkRapidsPlugin/getGpusResources.sh \
      && chown jovyan:1000 /opt/sparkRapidsPlugin/rapids-4-spark_2.12-0.5.0.jar \
      && chown jovyan:1000 /opt/sparkRapidsPlugin/cudf-0.19.2-cuda11.jar

USER $NB_USER

WORKDIR $HOME