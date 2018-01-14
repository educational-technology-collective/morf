# Pull base image.
FROM ubuntu:14.04

# install Python 
RUN \
  apt-get update && \
  apt-get install -y software-properties-common python-software-properties && \
  apt-get -y install python3-dev python3-pip python-virtualenv && \ 
  rm -rf /var/lib/apt/lists/* 

# install Python libraries
RUN pip3 install numpy pandas

# install r and dependencies
# see https://www.digitalocean.com/community/tutorials/how-to-set-up-r-on-ubuntu-14-04
RUN \
  sh -c 'echo "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/" >> /etc/apt/sources.list' && \
  gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-key E084DAB9 && \
  gpg -a --export E084DAB9 | apt-key add - && \
  apt-get update && \
  apt-get -y install r-base && \
  R -e "install.packages('getopt', repos = c('http://cran.rstudio.com/','http://cran.us.r-project.org'), dependencies = c('Depends'))" && \
  R -e "install.packages('optparse', repos = c('http://cran.rstudio.com/','http://cran.us.r-project.org'), dependencies = c('Depends'))"


# install MySQL and add configurations
RUN echo "mysql-server-5.6 mysql-server/root_password password root" | sudo debconf-set-selections && \
  echo "mysql-server-5.6 mysql-server/root_password_again password root" | sudo debconf-set-selections && \
  apt-get -y install mysql-server-5.6 && \
  echo "secure-file-priv = \"\"" >> /etc/mysql/conf.d/my5.6.cnf

# add scripts
ADD morf_test_course.py morf_test_course.py
ADD feature_extraction feature_extraction
ADD modeling modeling
# start mysql
RUN service mysql start

# define entrypoint
ENTRYPOINT ["python3", "morf_test_course.py"]





