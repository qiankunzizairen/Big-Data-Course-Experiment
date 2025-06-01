#!/bin/bash
# ./run_hbase_tasks.sh
# 此脚本利用 HBase shell 实现以下 5 个主要任务：
# (1) 列出所有表的相关信息
# (2) 打印指定表 'TestTable1' 的所有记录数据
# (3) 统计表 'TestTable1' 的行数
# (4) 向表 'TestTable1' 添加/删除指定的列数据，并打印变更后的数据
# (5) 清空表 'TestTable1' 的所有记录数据，并打印清空后的结果
# 注意：所有 HBase shell 命令中的警告通过 "2>/dev/null" 重定向到 /dev/null
# 请确保 HBase 服务已启动，且 hbase 命令在 PATH 中可用

echo "Initializing environment: 创建初始表，插入初始测试数据..."
hbase shell -n <<EOF 2>/dev/null

# 创建 TestTable1（包含 info 和 data 列族）
create 'TestTable1', 'info', 'data'
# 创建 TestTable2（包含 cf 列族）
create 'TestTable2', 'cf'

# 插入初始数据到 TestTable1
put 'TestTable1', 'row1', 'info:name', 'Alice'
put 'TestTable1', 'row2', 'info:name', 'Bob'
put 'TestTable1', 'row3', 'info:name', 'Charlie'
put 'TestTable1', 'row1', 'data:score', '100'
put 'TestTable1', 'row2', 'data:score', '85'
put 'TestTable1', 'row3', 'data:score', '90'
EOF

echo "=== (1) 列出所有表的相关信息 ==="
hbase shell -n <<EOF 2>/dev/null
list
EOF

echo "=== (2) 打印指定表 'TestTable1' 的所有记录数据 ==="
hbase shell -n <<EOF 2>/dev/null
scan 'TestTable1'
EOF

echo "=== (3) 统计表 'TestTable1' 的行数 ==="
hbase shell -n <<EOF 2>/dev/null
count 'TestTable1'
EOF

echo "=== (4) 向表 'TestTable1' 添加/删除指定的列数据 ==="
echo "=== 向 row1 添加 info:age 列（值为 30） ==="
echo "=== 删除 row2 中的 data:score 列 ==="
hbase shell -n <<EOF 2>/dev/null
put 'TestTable1', 'row1', 'info:age', '30'
delete 'TestTable1', 'row2', 'data:score'
EOF

echo "=== 打印变更后的数据 ==="
hbase shell -n <<EOF 2>/dev/null
scan 'TestTable1'
EOF

echo "=== (5) 删除所有表，恢复最初始的测试环境 ==="
hbase shell -n <<EOF 2>/dev/null
disable 'TestTable1'
drop 'TestTable1'
disable 'TestTable2'
drop 'TestTable2'
EOF

echo "=== 打印删除后的表信息 ==="
hbase shell -n <<EOF 2>/dev/null
list
EOF

echo "测试完毕！"