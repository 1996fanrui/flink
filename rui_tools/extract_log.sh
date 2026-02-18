#!/usr/bin/env bash
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

# 日志提取脚本
# 用法: ./extract_log.sh "搜索内容" "源日志文件路径"

# 检查参数数量
if [ $# -ne 2 ]; then
    echo "用法: $0 \"搜索内容\" \"源日志文件路径\""
    echo "示例: $0 \"Starting org.apache.flink.test\" \"/path/to/source.log\""
    exit 1
fi

SEARCH_CONTENT="$1"
SOURCE_LOG="$2"
OUTPUT_DIR=$(dirname "$SOURCE_LOG")

# 检查源文件是否存在
if [ ! -f "$SOURCE_LOG" ]; then
    echo "错误: 源日志文件不存在: $SOURCE_LOG"
    exit 1
fi

# 创建输出目录（如果不存在）
mkdir -p "$OUTPUT_DIR"

# 转义搜索内容中的特殊字符
ESCAPED_PATTERN=$(echo "$SEARCH_CONTENT" | sed 's/\./\\./g; s/\[/\\[/g; s/\]/\\]/g; s/(/\\(/g; s/)/\\)/g; s/\*/\\*/g; s/+/\\+/g; s/?/\\?/g; s/{/\\{/g; s/}/\\}/g')

echo "正在搜索模式: $ESCAPED_PATTERN"

# 查找起始行号
LINE_NUMBER=$(grep -n "$ESCAPED_PATTERN" "$SOURCE_LOG" | head -1 | cut -d: -f1)

# LINE_NUMBER=487560

if [ -z "$LINE_NUMBER" ]; then
    echo "错误: 在日志文件中未找到指定的搜索内容"
    echo "搜索内容: $SEARCH_CONTENT"
    exit 1
fi

echo "找到起始行: $LINE_NUMBER"

# 生成输出文件名
SOURCE_BASENAME=$(basename "$SOURCE_LOG" .log)
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_FILE="$OUTPUT_DIR/extracted_${SOURCE_BASENAME}_from_line_${LINE_NUMBER}_${TIMESTAMP}.log"

echo "正在提取日志到: $OUTPUT_FILE"

# 创建文件头注释
cat > "$OUTPUT_FILE" << EOF
# ========================================
# 日志提取文件
# ========================================
# 生成时间: $(date '+%Y-%m-%d %H:%M:%S')
# 源文件: $SOURCE_LOG
# 搜索内容: $SEARCH_CONTENT
# 起始行号: $LINE_NUMBER
# 提取方式: 从匹配行开始到文件末尾的所有内容
# ========================================

EOF

# 提取从指定行开始的所有内容并追加到输出文件
tail -n +$LINE_NUMBER "$SOURCE_LOG" >> "$OUTPUT_FILE"

# 统计行数
TOTAL_LINES=$(wc -l < "$OUTPUT_FILE")
EXTRACTED_LINES=$((TOTAL_LINES - 6))  # 减去文件头的行数

echo "提取完成!"
echo "输出文件: $OUTPUT_FILE"
echo "提取的日志行数: $EXTRACTED_LINES"
echo "总行数（包含文件头）: $TOTAL_LINES"