#!/bin/bash

# Packaging of bakufu
#
# Requires fpm: https://github.com/jordansissel/fpm
#
set -e

RELEASE_VERSION="1.0.0"
TOPDIR=/tmp/bakufu-release
export RELEASE_VERSION TOPDIR
export GO15VENDOREXPERIMENT=1

usage() {
  echo
  echo "Usage: $0 [-t target ] [-a arch ] [ -p prefix ] [-h] [-d]"
  echo "Options:"
  echo "-h Show this screen"
  echo "-t (linux|darwin) Target OS Default:(linux)"
  echo "-a (amd64|386) Arch Default:(amd64)"
  echo "-d debug output"
  echo "-b build only, do not generate packages"
  echo "-p build prefix Default:(/usr/local)"
  echo "-s release subversion"
  echo
}

function precheck() {
  local target
  local ok=0 # return err. so shell exit code

  if [[ "$target" == "linux" ]]; then
    if [[ ! -x "$( which fpm )" ]]; then
      echo "Please install fpm and ensure it is in PATH (typically: 'gem install fpm')"
      ok=1
    fi

    if [[ ! -x "$( which rpmbuild )" ]]; then
      echo "rpmbuild not in PATH, rpm will not be built (OS/X: 'brew install rpm')"
    fi
  fi

  if [[ -z "$GOPATH" ]]; then
    echo "GOPATH not set"
    ok=1
  fi

  if [[ ! -x "$( which go )" ]]; then
    echo "go binary not found in PATH"
    ok=1
  fi

  if [[ $(go version | egrep "go1[.][01234]") ]]; then
    echo "go version is too low. Must use 1.5 or above"
    ok=1
  fi

  return $ok
}

function setuptree() {
  local b prefix
  prefix="$1"

  mkdir -p $TOPDIR
  rm -rf ${TOPDIR:?}/*
  b=$( mktemp -d $TOPDIR/bakufuXXXXXX ) || return 1
  mkdir -p $b/bakufu
  mkdir -p $b/bakufu${prefix}/bakufu/
  mkdir -p $b/bakufu/etc/init.d
  mkdir -p $b/bakufu-cli/usr/bin
  echo $b
}

function oinstall() {
  local builddir prefix
  builddir="$1"
  prefix="$2"

  cd  $(dirname $0)
  gofmt -s -w  go/
  rsync -qa ./resources $builddir/bakufu${prefix}/bakufu/
  rsync -qa ./conf/bakufu-sample.* $builddir/bakufu${prefix}/bakufu/
  cp etc/init.d/bakufu.bash $builddir/bakufu/etc/init.d/bakufu
  chmod +x $builddir/bakufu/etc/init.d/bakufu
}

function package() {
  local target builddir prefix packages
  target="$1"
  builddir="$2"
  prefix="$3"

  cd $TOPDIR

  echo "Release version is ${RELEASE_VERSION}"

  case $target in
    'linux')
      echo "Creating Linux Tar package"
      tar -C $builddir/bakufu -czf $TOPDIR/bakufu-"${RELEASE_VERSION}"-$target-$arch.tar.gz ./

      echo "Creating Distro full packages"
      fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -t rpm -n bakufu -C $builddir/bakufu --prefix=/ .
      fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -t deb -n bakufu -C $builddir/bakufu --prefix=/ --deb-no-default-config-files .

      cd $TOPDIR
      # rpm packaging -- executable only
      echo "Creating Distro cli packages"
      fpm -v "${RELEASE_VERSION}" --epoch 1  -f -s dir -t rpm -n bakufu-cli -C $builddir/bakufu-cli --prefix=/ .
      fpm -v "${RELEASE_VERSION}" --epoch 1  -f -s dir -t deb -n bakufu-cli -C $builddir/bakufu-cli --prefix=/ --deb-no-default-config-files .
      ;;
    'darwin')
      echo "Creating Darwin full Package"
      tar -C $builddir/bakufu -czf $TOPDIR/bakufu-"${RELEASE_VERSION}"-$target-$arch.tar.gz ./
      echo "Creating Darwin cli Package"
      tar -C $builddir/bakufu-cli -czf $TOPDIR/bakufu-cli-"${RELEASE_VERSION}"-$target-$arch.tar.gz ./
      ;;
  esac

  echo "---"
  echo "Done. Find releases in $TOPDIR"
}

function build() {
  local target arch builddir gobuild prefix
  os="$1"
  arch="$2"
  builddir="$3"
  prefix="$4"
  ldflags="-X main.AppVersion=${RELEASE_VERSION}"
  echo "Building via $(go version)"
  gobuild="go build -ldflags \"$ldflags\" -o $builddir/bakufu${prefix}/bakufu/bakufu go/cmd/bakufu/main.go"

  case $os in
    'linux')
      echo "GOOS=$os GOARCH=$arch $gobuild" | bash
    ;;
    'darwin')
      echo "GOOS=darwin GOARCH=amd64 $gobuild" | bash
    ;;
  esac
  [[ $(find $builddir/bakufu${prefix}/bakufu/ -type f -name bakufu) ]] &&  echo "bakufu binary created" || (echo "Failed to generate bakufu binary" ; exit 1)
  cp $builddir/bakufu${prefix}/bakufu/bakufu $builddir/bakufu-cli/usr/bin && echo "binary copied to bakufu-cli" || (echo "Failed to copy bakufu binary to bakufu-cli" ; exit 1)
}

function main() {
  local target arch builddir prefix build_only
  target="$1"
  arch="$2"
  prefix="$3"
  build_only=$4

  precheck "$target"
  builddir=$( setuptree "$prefix" )
  oinstall "$builddir" "$prefix"
  build "$target" "$arch" "$builddir" "$prefix"
  [[ $? == 0 ]] || return 1
  if [[ $build_only -eq 0 ]]; then
    package "$target" "$builddir" "$prefix"
  fi
}

build_only=0
while getopts a:t:p:s:dbh flag; do
  case $flag in
  a)
    arch="$OPTARG"
    ;;
  t)
    target="$OPTARG"
    ;;
  h)
    usage
    exit 0
    ;;
  d)
    debug=1
    ;;
  b)
    echo "Build only; no packaging"
    build_only=1
    ;;
  p)
    prefix="$OPTARG"
    ;;
  s)
    RELEASE_VERSION="${RELEASE_VERSION}_${OPTARG}"
    ;;
  ?)
    usage
    exit 2
    ;;
  esac
done

shift $(( OPTIND - 1 ));
target=${target:-"linux"} # default for target is linux
arch=${arch:-"amd64"} # default for arch is amd64
prefix=${prefix:-"/usr/local"}

[[ $debug -eq 1 ]] && set -x
main "$target" "$arch" "$prefix" "$build_only"

echo "bakufu build done; exit status is $?"
