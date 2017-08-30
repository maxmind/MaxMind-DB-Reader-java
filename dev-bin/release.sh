#!/bin/bash

set -eu -o pipefail

changelog=$(cat CHANGELOG.md)

regex='
([0-9]+\.[0-9]+\.[0-9]+) \(([0-9]{4}-[0-9]{2}-[0-9]{2})\)
-*

((.|
)*)
'

if [[ ! $changelog =~ $regex ]]; then
      echo "Could not find date line in change log!"
      exit 1
fi

version="${BASH_REMATCH[1]}"
date="${BASH_REMATCH[2]}"
notes="$(echo "${BASH_REMATCH[3]}" | sed -n -e '/^[0-9]\+\.[0-9]\+\.[0-9]\+/,$!p')"

if [[ "$date" -ne  $(date +"%Y-%m-%d") ]]; then
    echo "$date is not today!"
    exit 1
fi

tag="v$version"

if [ -n "$(git status --porcelain)" ]; then
    echo ". is not clean." >&2
    exit 1
fi

mvn versions:display-dependency-updates

read -r -n 1 -p "Continue given above dependencies? (y/n) " should_continue

if [ "$should_continue" != "y" ]; then
    echo "Aborting"
    exit 1
fi

perl -pi -e "s/(?<=<version>)[^<]*/$version/" README.md
perl -pi -e "s/(?<=com\.maxmind\.db\:maxmind-db\:)\d+\.\d+\.\d+([\w\-]+)?/$version/" README.md

if [ -n "$(git status --porcelain)" ]; then
    git diff

    read -r -n 1 -p "Commit README.md changes? " should_commit
    if [ "$should_commit" != "y" ]; then
        echo "Aborting"
        exit 1
    fi
    git add README.md
    git commit -m 'update version number in README.md'
fi


# could be combined with the primary build
mvn release:clean
mvn release:prepare -DreleaseVersion="$version" -Dtag="$tag"
mvn release:perform

read -r -n 1 -p "Push to origin? " should_push

if [ "$should_push" != "y" ]; then
    echo "Aborting"
    exit 1
fi

git push
git push --tags

message="$version

$notes"

hub release create -m "$message" "$tag"

echo "Remember to do the release on https://oss.sonatype.org/!"
