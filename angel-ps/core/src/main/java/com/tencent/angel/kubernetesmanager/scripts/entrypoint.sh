#!/bin/bash
# echo commands to the terminal output
set -ex

# Check whether there is a passwd entry for the container UID
myuid=$(id -u)
mygid=$(id -g)
# turn off -e for getent because it will return error code in anonymous uid case
set +e
uidentry=$(getent passwd $myuid)
set -e

# If there is no passwd entry for the container UID, attempt to create one
if [ -z "$uidentry" ] ; then
    if [ -w /etc/passwd ] ; then
        echo "$myuid:x:$myuid:$mygid:anonymous uid:$ANGEL_HOME:/bin/false" >> /etc/passwd
    else
        echo "Container ENTRYPOINT failed to add passwd entry for anonymous UID"
    fi
fi

ANGEL_ON_K8S_CMD="$1"
case "$ANGEL_ON_K8S_CMD" in
    master | ps | worker)
      shift 1
      ;;
    "")
      ;;
    *)
      echo "Non-angel-on-k8s command provided, proceeding in pass-through mode..."
      exec "$@"
      ;;
esac
ANGEL_LIB_CLASSPATH=""
for jar in "${ANGEL_HOME}"/lib/*.jar; do
  if compgen -G "${ANGEL_HOME}/lib/hadoop-common-2.*.jar" > /dev/null; then
    case "${jar##*/}" in
      hadoop-client-api-*.jar | hadoop-client-runtime-*.jar) continue ;;
    esac
  fi
  ANGEL_LIB_CLASSPATH="${ANGEL_LIB_CLASSPATH:+$ANGEL_LIB_CLASSPATH:}$jar"
done
ANGEL_CLASSPATH="${ANGEL_CLASSPATH:+$ANGEL_CLASSPATH:}$ANGEL_LIB_CLASSPATH${CLASSPATH:+:$CLASSPATH}"
env | grep ANGEL_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt
readarray -t ANGEL_EXECUTOR_JAVA_OPTS < /tmp/java_opts.txt
ANGEL_MASTER_JAVA_OPTS=("${ANGEL_EXECUTOR_JAVA_OPTS[@]}")

if [ -n "$ANGEL_EXTRA_CLASSPATH" ]; then
  ANGEL_CLASSPATH="$ANGEL_CLASSPATH:$ANGEL_EXTRA_CLASSPATH"
fi

case "$ANGEL_ON_K8S_CMD" in
  ps)
    CMD=(
      ${JAVA_HOME}/bin/java
      "${ANGEL_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$ANGEL_EXECUTOR_MEMORY
      -Xmx$ANGEL_EXECUTOR_MEMORY
      -cp "$ANGEL_CLASSPATH"
      com.tencent.angel.ps.KubernetesParameterServerApp
    )
    ;;
  worker)
    CMD=(
      ${JAVA_HOME}/bin/java
      "${ANGEL_EXECUTOR_JAVA_OPTS[@]}"
      -Xms$ANGEL_EXECUTOR_MEMORY
      -Xmx$ANGEL_EXECUTOR_MEMORY
      -cp "$ANGEL_CLASSPATH"
      com.tencent.angel.worker.KubernetesWorkerApp
    )
    ;;
  master)
    CMD=(
      ${JAVA_HOME}/bin/java
      "${ANGEL_MASTER_JAVA_OPTS[@]}"
      -Xms$ANGEL_MASTER_MEMORY
      -Xmx$ANGEL_MASTER_MEMORY
      -cp "$ANGEL_CLASSPATH"
      com.tencent.angel.master.AngelKubernetesApplicationMaster
    )
    ;;

  *)
    echo "Unknown command: $ANGEL_ON_K8S_CMD" 1>&2
    exit 1
esac

# Execute the container CMD under tini for better hygiene
exec /sbin/tini -s -- "${CMD[@]}"