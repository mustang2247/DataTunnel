#!/bin/bash
echo $$ > $DATATUNNEL_PID
exec $@
