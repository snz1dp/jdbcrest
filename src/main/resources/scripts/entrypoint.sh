#!/bin/bash
# 赵岳峰 13317312768@qq.com
# 适用于普通的springboot工程

path=$(cd `dirname $0`; pwd)

if [ ! -z "${CONFIG_FILE}" ];then
  JAVA_OPTS="${JAVA_OPTS} -Dspring.config.location=${CONFIG_FILE}"
fi

JAVA_CMD="java"
JDBC_DMDB="${JDBC_DMDB:-}"
JDBC_DRIVER="${JDBC_DRIVER:-}"

if [ ! "$JDBC_DMDB" == "" ];then
  echo "write /etc/dm_svc.conf..."
  echo "DMDB=($JDBC_DMDB)">>/etc/dm_svc.conf
  echo "LOGIN_MODE=(1)">>/etc/dm_svc.conf
  if [ "$JDBC_DRIVER" == "" ];then
    export JDBC_DRIVER=dm.jdbc.driver.DmDriver
  fi
  export JDBC_URL=jdbc:dm://DMDB
  cat /etc/dm_svc.conf
fi

function run() {
  ${JAVA_CMD} ${JAVA_OPTS} -jar ${path}/app.jar $*
}

function console() {
  shift
  bash $*
}

case $1 in
  sh)
    console
  ;;
  bash)
    console
  ;;
  *)
    run $*
  ;;
esac
