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

# Builds the static mkfs.erofs of both architectures into cmd/atelet/kodata, for
# ko to copy into the atelet image (see hack/build-mkfs-erofs.sh).

set -o errexit -o nounset -o pipefail

sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  autoconf automake libtool pkg-config gcc gcc-aarch64-linux-gnu gcc-x86-64-linux-gnu

ROOT="$(git rev-parse --show-toplevel)"
for arch in amd64 arm64; do
  "${ROOT}/hack/build-mkfs-erofs.sh" "${arch}"
done
