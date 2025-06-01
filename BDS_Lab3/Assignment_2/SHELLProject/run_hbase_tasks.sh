#!/bin/bash
# ./run_hbase_tasks.sh
# 说明：
#   1) 创建三张表: Student, Course, SC，并插入题目要求的示例数据
#   2) 列出所有表信息
#   3) 打印三张表中的所有记录
#   4) 删除三张表，恢复最初始的测试环境
# 注意：
#   - 所有 HBase shell 命令中的警告通过 "2>/dev/null" 重定向到 /dev/null
#   - 请确保 HBase 服务已启动，且 hbase 命令在 PATH 中可用

echo "Initializing environment: 创建 [Student, Course, SC] 三张表并插入示例数据..."
hbase shell -n <<EOF 2>/dev/null

# 1. 创建三张表(都以 info 为列族)
create 'Student', 'info'
create 'Course', 'info'
create 'SC', 'info'

# 2. 在 Student 表中插入数据 (学号做行键)
put 'Student', '2015001', 'info:S_Name', 'Zhangsan'
put 'Student', '2015001', 'info:S_Sex', 'male'
put 'Student', '2015001', 'info:S_Age', '23'

put 'Student', '2015003', 'info:S_Name', 'Mary'
put 'Student', '2015003', 'info:S_Sex', 'female'
put 'Student', '2015003', 'info:S_Age', '22'

put 'Student', '2015002', 'info:S_Name', 'Lisi'
put 'Student', '2015002', 'info:S_Sex', 'male'
put 'Student', '2015002', 'info:S_Age', '24'

# 3. 在 Course 表中插入数据 (课程号做行键)
put 'Course', '123001', 'info:C_Name', 'Math'
put 'Course', '123001', 'info:C_Credit', '2.0'

put 'Course', '123002', 'info:C_Name', 'Computer'
put 'Course', '123002', 'info:C_Credit', '2.5'

put 'Course', '123003', 'info:C_Name', 'English'
put 'Course', '123003', 'info:C_Credit', '3.0'

# 4. 在 SC 表中插入数据 (将“学号_课程号”作为行键)
put 'SC', '2015001_123001', 'info:SC_Score', '86'
put 'SC', '2015001_123003', 'info:SC_Score', '69'
put 'SC', '2015002_123002', 'info:SC_Score', '77'
put 'SC', '2015002_123003', 'info:SC_Score', '88'
put 'SC', '2015003_123002', 'info:SC_Score', '90'
put 'SC', '2015003_123003', 'info:SC_Score', '95'

EOF

echo "=== (1) 列出所有表的相关信息 ==="
hbase shell -n <<EOF 2>/dev/null
list
EOF

echo "=== (2) 打印表 'Student'、'Course'、'SC' 的所有记录数据 ==="
hbase shell -n <<EOF 2>/dev/null
scan 'Student'
scan 'Course'
scan 'SC'
EOF

echo "=== (3) 删除所有表，恢复最初始的测试环境 ==="
hbase shell -n <<EOF 2>/dev/null
disable 'Student'
drop 'Student'
disable 'Course'
drop 'Course'
disable 'SC'
drop 'SC'
EOF

echo "=== 打印删除后的表信息 ==="
hbase shell -n <<EOF 2>/dev/null
list
EOF

echo "测试完毕！"