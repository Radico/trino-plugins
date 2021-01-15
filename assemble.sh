#!/bin/bash

git diff --quiet
DIRTY_TREE=$?

if [[ $DIRTY_TREE -ne 0 ]]; then
  echo "================================================================================="
  echo " Artifacts built from a dirty working tree should not be promoted to production."
  echo "================================================================================="
fi

SCALA_VERSION='2.13'
ARTIFACT=`sbt assembly | grep TRINO_PLUGINS_ARTIFACT | grep '.jar' | head -n 1 | cut -d ':' -f 2 | tr -d [:space:]`

SRC_PATH="target/scala-${SCALA_VERSION}/${ARTIFACT}"
TRINO_VERSION=`echo $ARTIFACT | cut -d '-' -f 4`
GIT_HASH=`echo $ARTIFACT | cut -d '-' -f 5 | cut -d '.' -f 1`

AUTH_DST_PATH="trino-config-docker/plugin/trino-auth/trino-${TRINO_VERSION}-plugins.jar"
EVENTS_DST_PATH="trino-config-docker/plugin/trino-events/trino-${TRINO_VERSION}-plugins.jar"

DIRTY_MESSAGE=""
if [[ $DIRTY_TREE -ne 0 ]]; then
  DIRTY_MESSAGE=" (dirty)"
fi

echo "Artifact name: ${ARTIFACT}"
echo "Trino version: ${TRINO_VERSION}"
echo "Git hash: ${GIT_HASH} ${DIRTY_MESSAGE}"

rm $AUTH_DST_PATH
echo "Copying artifact to docker config: ${SRC_PATH} -> ${AUTH_DST_PATH}"
cp $SRC_PATH $AUTH_DST_PATH

rm $EVENTS_DST_PATH
echo "Copying artifact to docker config: ${SRC_PATH} -> ${EVENTS_DST_PATH}"
cp $SRC_PATH $EVENTS_DST_PATH

if [[ $DIRTY_TREE -ne 0 ]]; then
  echo "Artifact built from a dirty working tree."
  exit 1
fi
