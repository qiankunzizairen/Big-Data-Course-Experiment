# Gradle-Project

这是一个使用 Gradle 构建和运行的 Java 项目。无须额外配置，只需将整个项目文件夹解压到本地，Gradle 会根据 `build.gradle` 自动在线下载所需依赖。第一次编译时间可能较长。

## 项目目录结构

```
my-gradle-project/
├── build.gradle              // Gradle 构建脚本
├── src/                      // 源代码和资源文件
│   ├── main/                 // 主代码
│   │   └── java/             // Java 源代码
│   └── resources/            // 资源文件        
│       └── log4j.properties  // 日志配置
├── build/                    // 生成的构建文件夹
│   └── libs/                 // 存放打包好的 Fat JAR 包
└── start.sh                  // 项目启动脚本，包含编译、打包、运行命令
```

如果项目中存在 Shell 任务，则会有一个独立的 `SHELLProject` 文件夹，里面包含一个封装好的脚本文件，可直接在命令行中运行。

```
SHELLProject/
└── run.sh                    // 封装好的 Shell 脚本
```

## 环境要求

- 已安装 Java JDK（建议 JDK 8 及以上版本）
- 已安装 Git（可选，仅用于版本控制）
- 已安装 Gradle（若未安装，Gradle Wrapper 会自动下载合适版本）
- Unix/Linux/macOS 环境（若在 Windows 上，请使用相应的 Shell 或者 Git Bash）

## 快速开始

1. 将项目压缩包解压到本地任意目录。
2. 进入项目根目录：
   ```bash
   cd my-gradle-project
   ```
3. 赋予 `start.sh` 可执行权限（若未设置）：
   ```bash
   chmod +x start.sh
   ```
4. 执行启动脚本：
   ```bash
   ./start.sh
   ```
   - 该脚本会自动执行以下操作：
     1. 使用 Gradle 进行编译
     2. 打包成 Fat JAR
     3. 运行生成的程序

5. 若项目中包含 Shell 任务，进入 `SHELLProject` 文件夹，赋予脚本可执行权限并运行：
   ```bash
   cd SHELLProject
   chmod +x run.sh
   ./run.sh
   ```

## 目录说明

- **build.gradle**  
  Gradle 构建脚本，定义了项目的依赖、插件、编译选项等。  
- **src/**  
  - **main/java/**：存放所有 Java 源代码。  
  - **resources/**：存放项目运行所需的资源文件（如 `log4j.properties`）。  
- **build/libs/**  
  Gradle 构建完成后，会将打包好的 Fat JAR 放在此目录下。  
- **start.sh**  
  项目一键编译、打包和运行的脚本。  
- **SHELLProject/**（可选）  
  若项目包含其他 Shell 脚本任务，会放在此文件夹中，运行方式与 `start.sh` 类似。

