FROM erlang:19

ENV HANDOFF_PORT "8099"
ENV PB_PORT "8087"
ENV PB_IP "0.0.0.0"
ENV PBSUB_PORT "8086"
ENV LOGREADER_PORT "8085"
ENV RING_STATE_DIR "data/ring"
ENV PLATFORM_DATA_DIR "data"
ENV NODE_NAME "antidote@127.0.0.1"
ENV SHORT_NAME "false"
# ENV ANTIDOTE_REPO "https://github.com/AntidoteDB/antidote.git"
# ENV ANTIDOTE_BRANCH "master"

RUN apt-get update

RUN set -ex && \
  echo 'deb http://deb.debian.org/debian jessie-backports main' \
  > /etc/apt/sources.list.d/jessie-backports.list && \
  apt update -y && \
  apt install -t \
  jessie-backports \
  openjdk-8-jdk \
  ca-certificates-java -y

WORKDIR /usr/src/antidote
COPY . /usr/src/antidote

RUN set -xe \
  && apt-get update \
  && apt-get install -y --no-install-recommends git openssl ca-certificates

RUN apt-get install erlang-jinterface && \
  cp -R /usr/lib/erlang/lib/jinterface-1.5.10 /usr/local/lib/erlang/lib/

RUN git clone "https://github.com/fredlund/JavaErlang.git" && \
  cd JavaErlang && \
  rebar3 compile && \
  echo y | erl -noshell -pa _build/default/lib/java_erlang/ebin -eval "java_erlang_install:install()" -s init stop && \
  cd ..

RUN  make clean \
  && make rel \
  && cp -R _build/default/rel/antidote /opt/ \
  && sed -e '$i,{kernel, [{inet_dist_listen_min, 9100}, {inet_dist_listen_max, 9100}]}' /usr/src/antidote/_build/default/rel/antidote/releases/0.0.1/sys.config > /opt/antidote/releases/0.0.1/sys.config \
  && rm -rf /var/lib/apt/lists/*

ADD Dockerfiles/start_and_attach.sh /opt/antidote/
ADD Dockerfiles/entrypoint.sh /

RUN chmod a+x /opt/antidote/start_and_attach.sh \
  && chmod a+x /entrypoint.sh

# Distributed Erlang Port Mapper
EXPOSE 4369
# Ports for Antidote
EXPOSE 8085 8086 8087 8099

# Antidote RPC
EXPOSE 9100

VOLUME /opt/antidote/data

ENTRYPOINT ["/entrypoint.sh"]

CMD ["/opt/antidote/start_and_attach.sh"]