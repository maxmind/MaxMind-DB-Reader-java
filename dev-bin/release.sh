#!/bin/bash

set -e

VERSION=$(perl -MFile::Slurp::Tiny=read_file -MDateTime <<EOF
use v5.16;
my \$log = read_file(q{CHANGELOG.md});
\$log =~ /\n(\d+\.\d+\.\d+) \((\d{4}-\d{2}-\d{2})\)\n/;
die 'Release time is not today!' unless DateTime->now->ymd eq \$2;
say \$1;
EOF
)

TAG="v$VERSION"

if [ -n "$(git status --porcelain)" ]; then
    echo ". is not clean." >&2
    exit 1
fi

mvn versions:display-dependency-updates

read -e -p "Continue given above dependencies? (y/n) " SHOULD_CONTINUE

if [ "$SHOULD_CONTINUE" != "y" ]; then
    echo "Aborting"
    exit 1
fi

export VERSION
perl -pi -e 's/(?<=<version>)[^<]*/$ENV{VERSION}/' README.md
cat README.md >> "$PAGE"

if [ -n "$(git status --porcelain)" ]; then
    git diff

    read -e -p "Commit README.md changes? " SHOULD_COMMIT
    if [ "$SHOULD_COMMIT" != "y" ]; then
        echo "Aborting"
        exit 1
    fi
    git add README.md
    git commit -m 'update version number in README.md'
fi


# could be combined with the primary build
mvn release:clean
mvn release:prepare -DreleaseVersion="$VERSION" -Dtag="$TAG"
mvn release:perform

read -e -p "Push to origin? " SHOULD_PUSH

if [ "$SHOULD_PUSH" != "y" ]; then
    echo "Aborting"
    exit 1
fi

git push
git push --tags

echo "Remember to do the release on https://oss.sonatype.org/!"
