#!/bin/bash
#
# a script for running an arbitrary class, adapted heavily from Terrier

BIN=`dirname $0`

#setup JAVA_HOME
if [ ! -n "$JAVA_HOME" ]
then
	#where is java?
	TEMPVAR=`which java`
	#j2sdk/bin folder is in the dir that java was in
	TEMPVAR=`dirname $TEMPVAR`
	#then java install prefix is folder above
	JAVA_HOME=`dirname $TEMPVAR`
	echo "Setting JAVA_HOME to $JAVA_HOME"
fi

LIB=$BIN/../lib

#setup CLASSPATH
for jar in $LIB/*.jar $LIB/../target/*.jar; do
	if [ ! -n "$CLASSPATH" ]
	then
		CLASSPATH=$jar
	else
		CLASSPATH=$CLASSPATH:$jar
	fi
done

CLASSPATH=$CLASSPATH:/etc/hadoop/conf:/etc/hbase/conf
# echo $CLASSPATH
if [ ! -n "$JAVA_OPTS" ];
then
    JAVA_OPTS=-Xmx1G
fi

$JAVA_HOME/bin/java $JAVA_OPTS -cp $CLASSPATH $@

