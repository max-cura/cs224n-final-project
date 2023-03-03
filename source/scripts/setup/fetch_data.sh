#!/bin/zsh

COMPRESSED_DIR=data/compressed
INPUTS_DIR=data/inputs

URL_BASE="https://archive.org/download/stackexchange/"
URL_FNAMES="Stackoverflow.com-Posts.7z stackoverflow.com-PostHistory.7z stackoverflow.com-Votes.7z"

for FNAME in $URL_FNAMES ; do
  if ! [ -f "${COMPRESSED_DIR}/${FNAME}" ] ; then
    wget $URL_BASE$FNAME --output-file $COMPRESSED_DIR/$FNAME
  else
    echo "${COMPRESSED_DIR}/${FNAME} is already present"
  fi
done

if ! (which 7za > /dev/null) ; then
  if [[ $OSTYPE == "darwin"* ]] ; then
    brew install p7zip
  else
    echo "Unknown OS"
  fi
else
  echo "found p7zip; ($(which 7za))"
fi

for FNAME in $URL_FNAMES ; do
  7za x -o$INPUTS_DIR $COMPRESSED_DIR/$FNAME
done
