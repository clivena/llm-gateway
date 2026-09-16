@echo off
rem ---------------------------------------------------------------------------
rem llm-gateway 一键编译 + 单测
rem
rem 前置：JDK 17+（JAVA_HOME 指向它）、Maven
rem 可覆盖的环境变量：JAVA_HOME / MVN_CMD / MAVEN_REPO / GATEWAY_DIR
rem
rem 用法：scripts\run-gateway-tests.cmd
rem ---------------------------------------------------------------------------
setlocal

if not defined GATEWAY_DIR set "GATEWAY_DIR=%~dp0.."

rem --- 找 Maven：环境变量 -> 本机隔离安装 -> PATH ---
if defined MVN_CMD goto mvn_ready
set "MVN_CMD=%USERPROFILE%\.workbuddy\binaries\maven\apache-maven-3.9.11\bin\mvn.cmd"
if exist "%MVN_CMD%" goto mvn_ready
set "MVN_CMD=mvn"
:mvn_ready

rem --- 找本地仓库：环境变量 -> 本机隔离仓库 -> 标准 ~/.m2 ---
if defined MAVEN_REPO goto repo_ready
set "MAVEN_REPO=%USERPROFILE%\.workbuddy\binaries\maven\repo"
if exist "%MAVEN_REPO%" goto repo_ready
set "MAVEN_REPO=%USERPROFILE%\.m2\repository"
:repo_ready

echo [run-gateway-tests] JAVA_HOME = %JAVA_HOME%
echo [run-gateway-tests] MVN       = %MVN_CMD%
echo [run-gateway-tests] REPO      = %MAVEN_REPO%
echo [run-gateway-tests] GATEWAY   = %GATEWAY_DIR%

"%MVN_CMD%" -B -Duser.language=en -Dmaven.repo.local="%MAVEN_REPO%" -f "%GATEWAY_DIR%\pom.xml" test %*
if errorlevel 1 (
  echo [run-gateway-tests] FAILED
  endlocal
  exit /b 1
)

echo [run-gateway-tests] OK
endlocal
