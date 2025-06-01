#!/bin/bash

# 编译 Java 文件并将 .class 文件输出到 bin 目录
javac -classpath $(hadoop classpath) -d bin src/MyHDFS.java

# 创建 JAR 文件，并将 bin 目录中的所有 .class 文件打包
jar -cvf MyHDFS.jar -C bin .

# 使用 Hadoop 执行 JAR 文件
hadoop jar MyHDFS.jar MyHDFS