FROM ubuntu

#update repos
RUN apt-get update
RUN apt-get install -y curl
RUN apt-get install -y software-properties-common python-software-properties debconf-utils
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update

#install java
RUN apt-get install -y default-jdk
RUN curl -s http://mirror.nohup.it/apache/hadoop/common/hadoop-2.7.1/hadoop-2.7.1.tar.gz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s hadoop-2.7.1 hadoop
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-2.2.0-bin-hadoop2.7 spark
RUN apt-get install -y vim
RUN /usr/local/spark/conf/spark-env.sh.template  /usr/local/spark-2.2.0-bin-hadoop2.7/conf/spark-env.sh

#set env variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark/
ENV HADOOP_HOME /usr/local/hadoop/
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
ENV YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop/

#copy config files, jar and dataset
COPY yarn-site.xml /usr/local/hadoop/etc/hadoop/yarn-site.xml
COPY core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml
RUN mkdir /yelp-dataset
RUN mkdir /client
COPY challenge-1.0.jar /client/challenge-1.0.jar
COPY yelp_dataset_challenge_round9.tar /yelp-dataset
COPY json_to_parquet.sh /client/json_to_parquet.sh
COPY put_to_hdfs.py /client/put_to_hdfs.py
RUN mkdir /client/queries
COPY queries/top_rated.scala /client/queries/top_rated.scala
COPY queries/top_nightlife_businesses_in_LasVegas.scala /client/queries/top_nightlife_businesses_in_LasVegas.scala
COPY queries/top_users_for_starbucks_by_reviews.scala /client/queries/top_users_for_starbucks_by_reviews.scala
COPY queries/top_statescites_by_reviews.scala /client/queries/top_statescites_by_reviews.scala