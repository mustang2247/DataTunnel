@echo off

if "%1"=="" goto Usage
if "%1" == "start" goto doStart
if "%1" == "restart" goto doRestart
if "%1" == "stop" goto doStop


:Usage
echo """""""""""""""""""""""""""""""""""""""""""""""""""""
echo "               DataTunnel    Console               "
echo """""""""""""""""""""""""""""""""""""""""""""""""""""
echo "Usage:    spider  <Commands>                       "
echo "Commands:       start       start the datatunnel   "
echo "                restart     restart the datatunnel "
echo "                stop        stop the datatunnel    "
echo """""""""""""""""""""""""""""""""""""""""""""""""""""
goto end

:doStart
shift
set ACTION=start
goto run

:doRestart
shift
set ACTION=start
goto run

:doStop
shift
set ACTION=stop
goto run

:end
exit /b 0

:exit
exit /b 1

:noJreHome
rem Needed at least a JRE
echo The JRE_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto exit


:ADDLiB
SET CLASSPATH=%1;%CLASSPATH%
goto :EOF

:run
set "JRE_HOME=%JAVA_HOME%"
if not exist "%JRE_HOME%\bin\java.exe" goto noJreHome
set RUNJAVA="%JRE_HOME%\bin\java.exe"
set "DATATUNNEL_BIN_DIR=%cd%"
cd ..
set "DATATUNNEL_HOME=%cd%"

SetLocal EnableDelayedExpansion
set "CLASSPATH=.;%DATATUNNEL_HOME%\conf"
for /R %DATATUNNEL_HOME%\lib\ %%i in (*.jar) do (
call :ADDLiB %%i
)
set MAINCLASS=com.bytegriffin.datatunnel.DataTunnel
set JAVA_OPTS=-server -Xms512m -Xmx512m -XX:+UseParallelGC -XX:+HeapDumpOnOutOfMemoryError -Dfile.encoding=UTF-8 -Djcore.parser=SAX -Djcore.logger=org.apache.log4j
%RUNJAVA% -classpath "%CLASSPATH%" %JAVA_OPTS% %MAINCLASS%
EndLocal
goto end