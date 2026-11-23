#!/usr/bin/env bash

# Copyright 2026 The Agent Substrate Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds a static mkfs.erofs from erofs-utils at a pinned commit. atelet builds
# the root filesystem image of a seed with it. The binary goes into
# cmd/atelet/kodata, which ko copies into the atelet image as
# /var/run/ko/mkfs.erofs-<arch>.
#
# Usage: hack/build-mkfs-erofs.sh <amd64|arm64> [output file]
#
# Needs git, gcc, make, autoconf, automake, libtool, pkg-config and, to build for another
# architecture than the host's, that architecture's gcc cross compiler
# (gcc-aarch64-linux-gnu or gcc-x86-64-linux-gnu).

set -o errexit -o nounset -o pipefail

readonly REPO="https://git.kernel.org/pub/scm/linux/kernel/git/xiang/erofs-utils.git"
readonly TAG="v1.8.10"
readonly COMMIT="51b5939b5f783221310d25146e6a2019ba8129b6"

ROOT="$(git rev-parse --show-toplevel)"
arch="${1:?usage: $0 <amd64|arm64> [output file]}"
out="${2:-${ROOT}/cmd/atelet/kodata/mkfs.erofs-${arch}}"

case "${arch}" in
  amd64) triple=x86_64-linux-gnu ;;
  arm64) triple=aarch64-linux-gnu ;;
  *) echo "unsupported architecture ${arch}" >&2; exit 1 ;;
esac

host_args=()
if [[ "$(uname -m)" != "${triple%%-*}" ]]; then
  host_args=("--host=${triple}")
  export CC="${triple}-gcc"
  STRIP="${triple}-strip"
else
  STRIP=strip
fi

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

git clone --quiet --depth 1 --branch "${TAG}" "${REPO}" "${work}/src"
got="$(git -C "${work}/src" rev-parse 'HEAD^{commit}')"
if [[ "${got}" != "${COMMIT}" ]]; then
  echo "erofs-utils ${TAG} is commit ${got}, want ${COMMIT}" >&2
  exit 1
fi

# The files of the source tree have no fixed timestamps: pin them, so that
# nothing the build embeds depends on when it ran.
export SOURCE_DATE_EPOCH="$(git -C "${work}/src" log -1 --format=%ct)"

cd "${work}/src"
./autogen.sh
# Without compressors the image is plain, which is all atelet asks for. UUID
# and timestamps are set on the command line (see cmd/atelet/erofs.go).
./configure "${host_args[@]}" \
  --disable-shared --enable-static --disable-fuse --disable-lz4 --disable-lzma --disable-multithreading \
  --without-zlib --without-libdeflate --without-libzstd --without-qpl \
  --without-xxhash --without-uuid --without-selinux \
  CFLAGS="-O2 -ffile-prefix-map=${work}=. -fdebug-prefix-map=${work}=." \
  LDFLAGS="-static"
make -j"$(nproc)" -C lib
make -j"$(nproc)" -C mkfs LDFLAGS="-all-static" mkfs.erofs
"${STRIP}" --strip-all mkfs/mkfs.erofs

mkdir -p "$(dirname "${out}")"
install -m 0755 mkfs/mkfs.erofs "${out}"
echo "built ${out} from erofs-utils ${TAG} ($(sha256sum "${out}" | cut -d' ' -f1))"
