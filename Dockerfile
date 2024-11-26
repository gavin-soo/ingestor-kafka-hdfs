FROM rust:1.72 as build

RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    software-properties-common \
    cmake \
    wget \
    libclang-dev \
    libudev-dev \
    libssl-dev \
    ca-certificates \
    && add-apt-repository ppa:openjdk-r/ppa \
    && apt-get update && apt-get install -y openjdk-8-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV HADOOP_VERSION=3.4.0
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Set HADOOP_HOME and update PATH
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$HADOOP_HOME/bin:$PATH"

# Set JAVA_HOME and update LD_LIBRARY_PATH for Java libraries
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
# ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/aarch64/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV CLASSPATH $HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*

RUN cp $HADOOP_HOME/lib/native/libhdfs.so /usr/local/lib


RUN USER=root cargo new --bin solana
WORKDIR /solana

COPY . /solana

RUN cargo build --release



FROM rust:1.72

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates \
    wget \
    && add-apt-repository ppa:openjdk-r/ppa \
    && apt-get update && apt-get install -y openjdk-8-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop
ENV HADOOP_VERSION=3.4.0
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
    && rm hadoop-${HADOOP_VERSION}.tar.gz \
    && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Set HADOOP_HOME and update PATH
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$HADOOP_HOME/bin:$PATH"

# Set JAVA_HOME and update LD_LIBRARY_PATH for Java libraries
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
# ENV LD_LIBRARY_PATH="$JAVA_HOME/lib/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/aarch64/server:$HADOOP_HOME/lib/native:/usr/local/lib:$LD_LIBRARY_PATH"
ENV CLASSPATH $HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*

RUN cp $HADOOP_HOME/lib/native/libhdfs.so /usr/local/lib

COPY --from=build /solana/docker/config/log4j.properties /opt/hadoop/etc/hadoop/log4j.properties

# Set working directory
WORKDIR /usr/local/bin

# Copy the built binary from host to container
#COPY target/release/ingestor-kafka-hbase .
COPY --from=build /solana/target/release/block-encoder-service .
#COPY docker/config/.env.test ./.env

# Make the binary executable
RUN chmod +x ingestor-kafka-hbase

ENV RUST_LOG=info

# Set entrypoint
ENTRYPOINT ["./ingestor-kafka-hbase"]
