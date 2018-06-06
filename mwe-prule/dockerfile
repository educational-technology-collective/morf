# Pull base image.
FROM ubuntu:14.04

# Install Java, Python, and base R
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  apt-get update && \
  apt-get install -y software-properties-common python-software-properties && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer && \
  apt-get update && \
  apt-get -y install python3-dev python3-pip python-virtualenv && \ 
  rm -rf /var/lib/apt/lists/* 


# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# install MySQL
RUN apt-get update && \
  echo "mysql-server-5.6 mysql-server/root_password password root" | sudo debconf-set-selections && \
  echo "mysql-server-5.6 mysql-server/root_password_again password root" | sudo debconf-set-selections && \
  apt-get -y install mysql-server-5.6

# add configuration to mysql
RUN echo "secure-file-priv = \"\"" >> /etc/mysql/conf.d/my5.6.cnf

# add scripts
ADD MORF1.4 MORF1.4
ADD morf-prule.py morf-prule.py
ADD feature_extraction feature_extraction
# start mysql
RUN service mysql start

# define entrypoint
ENTRYPOINT ["python3", "morf-prule.py"]





