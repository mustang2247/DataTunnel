#!/bin/sh

# resolve links - "${BASH_SOURCE-$0}" may be a softlink
this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# Get standard environment variables
bin=`dirname "$this"`

# Only set datatunnel_home if not already set
[ -z "$datatunnel_home" ] && datatunnel_home=`cd "$bin/.." ; pwd`

BASEDIR="$datatunnel_home"

# Make sure prerequisite environment variables are set
if [ -z "$JAVA_HOME" -a -z "$JRE_HOME" ]; then
  JAVA_PATH=`which java 2>/dev/null`
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_PATH=`dirname $JAVA_PATH 2>/dev/null`
    JRE_HOME=`dirname $JAVA_PATH 2>/dev/null`
  fi
  if [ "x$JRE_HOME" = "x" ]; then
    if [ -x /usr/bin/java ]; then
      JRE_HOME=/usr
    fi
  fi
  if [ -z "$JAVA_HOME" -a -z "$JRE_HOME" ]; then
    echo "Neither the JAVA_HOME nor the JRE_HOME environment variable is defined"
    echo "At least one of these environment variable is needed to run this program"
    exit 1
  fi
fi

if [ -z "$JRE_HOME" ]; then
  JRE_HOME="$JAVA_HOME"
fi

if [ -z "$BASEDIR" ]; then
  echo "The BASEDIR environment variable is not defined"
  echo "This environment variable is needed to run this program"
  exit 1
fi

# Set standard CLASSPATH
if [ "$1" = "debug" -o "$1" = "javac" ] ; then
  if [ -f "$JAVA_HOME"/lib/tools.jar ]; then
    CLASSPATH="$JAVA_HOME"/lib/tools.jar
  fi
fi

for lib in "$BASEDIR"/lib/*.jar
do
  CLASSPATH=$CLASSPATH:$lib
done
CLASSPATH=$CLASSPATH:"$BASEDIR"/conf

# Set standard commands for invoking Java.
RUNJAVA="$JRE_HOME"/bin/java

# Now switch to home directory of echo
cd $datatunnel_home

if [ -z "$JAVA_OPTS" ]; then
  JAVA_OPTS="-server -Xms2560m -Xmx2560m -Xmn2g -XX:+UseParallelGC -XX:+HeapDumpOnOutOfMemoryError -Dfile.encoding=UTF-8"
else
  JAVA_OPTS="$JAVA_OPTS -server -Xms2560m -Xmx2560m -Xmn2g -XX:+UseParallelGC -XX:+HeapDumpOnOutOfMemoryError -Dfile.encoding=UTF-8"
fi

export datatunnel_pid=$datatunnel_home/datatunnel.pid
export datatunnel_prgname="DataTunnel"

psid=0

checkpid() {  
   javaps=`$JAVA_HOME/bin/jps | grep $datatunnel_prgname`
   if [ -n "$javaps" ]; then  
      psid=`echo $javaps | awk '{print $1}'`  
   else  
      psid=0  
   fi
}

# if no args specified, show usage
if [ $# -lt 1 ]; then
  more <<'EOF'
Usage:  datatunnel  <Commands>                        
Commands:           start       start the datatunnel      
                    restart     restart the datatunnel    
                    stop        stop the datatunnel       
EOF
  exit 1
fi

if [ "$1" = "start" ] ; then
  if [ ! -d "$datatunnel_home"/log ]; then
    mkdir $datatunnel_home/log
    chmod a+x $datatunnel_home/log
  fi
  checkpid
  if [ $psid -ne 0 ]; then
	echo "DataTunnel已经处于运行状态。"
  else
  	if [ -f "$datatunnel_pid" ]; then
  	   rm -rf $datatunnel_pid
	fi
	$RUNJAVA -classpath $CLASSPATH $JAVA_OPTS com.bytegriffin.datatunnel.DataTunnel & 
	checkpid
	if [ $psid -ne 0 ]; then
	   echo $psid >> $datatunnel_pid
	   echo "DataTunnel正在启动。。。"
	else
	   echo "DataTunnel启动失败。"
	fi
  fi
elif [ "$1" = "restart" ] ; then
  if [ -f "$datatunnel_pid" ]; then
    kill -9 `cat $datatunnel_pid` > /dev/null 2>&1 | xargs rm -rf $datatunnel_pid
  else
    ps -ef | grep $datatunnel_prgname | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1 &
  fi
  if [ ! -d "$datatunnel_home"/log ]; then
    mkdir $datatunnel_home/log
  fi
  $RUNJAVA -classpath $CLASSPATH $JAVA_OPTS com.bytegriffin.datatunnel.DataTunnel &
  checkpid
  if [ $psid -ne 0 ]; then
  	echo $psid >> $datatunnel_pid
    echo "DataTunnel正在重启。。。"
  else
	echo "DataTunnel重启失败。"
  fi
elif [ "$1" = "stop" ] ; then
  if [ -f "$datatunnel_pid" ]; then
    kill -9 `cat $datatunnel_pid` > /dev/null 2>&1 | xargs rm -rf $datatunnel_pid
    echo "DataTunnel成功关闭。"
  else
    ps -ef | grep $datatunnel_prgname | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1 &
  fi
else
  more <<'EOF'
Usage:  datatunnel  <Commands>                        
Commands:           start       start the datatunnel      
                    restart     restart the datatunnel    
                    stop        stop the datatunnel    
EOF
  exit 1
fi