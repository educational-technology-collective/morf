# Pull base image.
FROM ubuntu:14.04

# Install Java
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  apt-get update && \
  apt-get install -y software-properties-common && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer && \
  apt-get update && \
  rm -rf /var/lib/apt/lists/* 

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# add scripts
ADD MORF-evaluate MORF-evaluate
ADD morf-prule-evaluate.py morf-prule-evaluate.py

RUN \
  javac -cp /MORF-evaluate/jars/jess.jar -d /MORF-evaluate/bin/ /MORF-evaluate/src/Execute.java && \
  javac -d /MORF-evaluate/bin/ /MORF-evaluate/src/Combine.java

# define entrypoint
ENTRYPOINT ["python3", "morf-prule-evaluate.py"]





