#!/bin/zsh
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

################################################################################

# Source the user's profile to load environment variables
if [ -f ~/.zshrc ]; then
    . ~/.zshrc
fi

set -e  # 遇到错误立即退出
# set -x  # 会在执行每个命令之前，先打印出这个命令以及其所有参数。方便排查具体执行到哪里了

export FLINK_VERSION="os-troubleshooting-$(git rev-parse --short HEAD)"

# jdk11

mvn versions:set -DnewVersion="$FLINK_VERSION" -DgenerateBackupPoms=false

git add .

git commit -m "tmp version change"

# ./mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -P java11-target -P java11

./mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true
