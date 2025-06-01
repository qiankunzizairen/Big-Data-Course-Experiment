#!/bin/bash
# ./hdfs_lab2.sh
# 此脚本实现以下主要任务：

# startup：在HDFS上创建Lab2文件夹，Lab2文件夹内创建testdir文件夹，在testdir中创建test.txt空文件，并写入“lab2 test”（6、7），
# 向HDFS再次上传同名文件test.txt，追加、覆盖支持（1），追加支持开头或结尾（8）；
# 输出追加后的文件到终端（3）
# 输出Lab2文件夹及内部所有文件的信息（递归支持）（4、5）
# 将test.txt移动到Lab2文件夹中（11）
# 从HDFS中下载test.txt（重命名副本支持）（2）
# teardown：清理环境，删除Lab2文件夹及其内部测试文件和目录（6、7、9、10）

# 注意：所有 HBase shell 命令中的警告通过 "2>/dev/null" 重定向到 /dev/null
# 请确保 HBase 服务已启动，且 hbase 命令在 PATH 中可用

echo "===== 实验2任务1 HDFS Shell 实现开始 ====="

# 1. 创建 HDFS 目录结构
echo ">> 创建 /Lab2/testdir 目录..."
hdfs dfs -mkdir -p /Lab2/testdir 2>/dev/null

# 2. 创建 test.txt 并写入初始内容
echo ">> 写入 lab2 test 到 /Lab2/testdir/test.txt..."
echo "lab2 test" | hdfs dfs -put - /Lab2/testdir/test.txt 2>/dev/null

echo "===== 输出 /Lab2/testdir/test.txt 内容 ====="
hdfs dfs -cat /Lab2/testdir/test.txt 2>/dev/null

# 3. 上传同名文件，支持覆盖或模拟追加
read -p ">> 现在上传 test.txt，选择 1(追加) 或 2(覆盖): " choice
if [ "$choice" = "1" ]; then
  read -p ">>> 追加到开头(a) 还是末尾(b): " ab
  tmpfile=$(mktemp)
  hdfs dfs -cat /Lab2/testdir/test.txt 2>/dev/null > "$tmpfile"

  if [ "$ab" = "a" ]; then
    cat test.txt "$tmpfile" > "$tmpfile"_new
  else
    cat "$tmpfile" test.txt > "$tmpfile"_new
  fi

  echo ">>> 模拟追加完成，重新写入 test.txt..."
  hdfs dfs -rm /Lab2/testdir/test.txt 2>/dev/null
  hdfs dfs -put "$tmpfile"_new /Lab2/testdir/test.txt 2>/dev/null
  rm "$tmpfile" "$tmpfile"_new
else
  echo ">>> 执行覆盖上传..."
  hdfs dfs -put -f test.txt /Lab2/testdir/test.txt 2>/dev/null
fi

# 4. 输出文件内容
echo "===== 输出 /Lab2/testdir/test.txt 内容 ====="
hdfs dfs -cat /Lab2/testdir/test.txt 2>/dev/null

# 5. 递归列出目录内容
echo "===== 递归列出 /Lab2 所有内容 ====="
hdfs dfs -ls -R /Lab2 2>/dev/null

# 6. 移动文件到 /Lab2 根目录
echo ">> 移动 test.txt 到 /Lab2..."
hdfs dfs -mv /Lab2/testdir/test.txt /Lab2/test.txt 2>/dev/null

# 7. 再次列出
echo "===== 移动后再次列出 /Lab2 ====="
hdfs dfs -ls -R /Lab2 2>/dev/null

# 8. 下载文件（如重名则自动改名）
localfile="./test.txt"
downloaded="download_$(date +%s)_test.txt"
if [ -f "$localfile" ]; then
  echo ">> 本地已有 test.txt，重命名下载为 $downloaded"
  hdfs dfs -get /Lab2/test.txt "$downloaded" 2>/dev/null
else
  hdfs dfs -get /Lab2/test.txt 2>/dev/null
fi

# 9. teardown 清理 HDFS
read -p ">> 是否删除 /Lab2 目录？(y/n): " del
if [ "$del" = "y" ]; then
  hdfs dfs -rm -r /Lab2 2>/dev/null
  echo "已删除 /Lab2"
else
  echo "保留 /Lab2"
fi

echo "===== 实验结束 ====="