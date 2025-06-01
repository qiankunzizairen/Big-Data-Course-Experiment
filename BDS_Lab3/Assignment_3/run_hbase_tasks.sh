#!/bin/bash
# ./run_hbase_tasks.sh
# 注意：
#   - 所有 HBase shell 命令中的警告通过 "2>/dev/null" 重定向到 /dev/null
#   - 请确保 HBase 服务已启动，且 hbase 命令在 PATH 中可用

echo "Initializing environment: 创建表并插入示例数据..."
hbase shell -n <<EOF 2>/dev/null

create 'book', 'bookName'
put 'book', '60', 'bookName:', 'Thinking in Java'
put 'book', '20', 'bookName:', 'Database System Concept'
put 'book', '30', 'bookName:', 'Data Mining'


EOF

echo "=== 展示对“price”的排序结果 ==="
hbase shell -n <<EOF 2>/dev/null
scan 'book'
EOF

echo "=== 删除所有表，恢复最初始的测试环境 ==="
hbase shell -n <<EOF 2>/dev/null
disable 'book'
drop 'book'
EOF

echo "=== 打印删除后的表信息 ==="
hbase shell -n <<EOF 2>/dev/null
list
EOF

echo "测试完毕！"