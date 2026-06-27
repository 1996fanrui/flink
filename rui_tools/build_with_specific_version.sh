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

# This build requires JDK 17; force it unless the shell already runs it.
# Portable across macOS (/usr/libexec/java_home) and Linux (JAVA_HOME_<v>_X64 / /usr/lib/jvm scan).
ensure_java() {
  local want="$1" home="" d envvar
  if java -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
    echo "Java $want already active; keeping current JAVA_HOME."
    return 0
  fi
  if [ -x /usr/libexec/java_home ]; then                       # macOS
    home="$(/usr/libexec/java_home -v "$want" 2>/dev/null)"
    # java_home silently returns the nearest JDK when the exact version is absent.
    if [ -n "$home" ] && ! "$home/bin/java" -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
      home=""
    fi
  fi
  if [ -z "$home" ]; then                                      # CI (GitHub Actions etc.)
    envvar="JAVA_HOME_${want}_X64"
    home="${(P)envvar}"
  fi
  if [ -z "$home" ]; then                                      # Linux: scan common JVM dirs
    for d in /usr/lib/jvm/*"$want"*(N) /usr/java/*"$want"*(N); do
      if [ -x "$d/bin/java" ] && "$d/bin/java" -version 2>&1 | grep -qE "version \"${want}[.\"]"; then
        home="$d"; break
      fi
    done
  fi
  [ -n "$home" ] || { echo "JDK $want not found"; exit 1; }
  export JAVA_HOME="$home"
  export PATH="$JAVA_HOME/bin:$PATH"
  echo "Using JDK $want at $JAVA_HOME"
}
ensure_java 17

mvn versions:set -DnewVersion="$FLINK_VERSION" -DgenerateBackupPoms=false

git add .

git commit -m "tmp version change"

# ./mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -P java11-target -P java11

./mvnw -T 20 clean install -U -Pfast -DskipTests -Dmaven.javadoc.skip=true -Drat.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true
