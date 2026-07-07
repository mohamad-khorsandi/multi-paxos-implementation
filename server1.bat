@echo off
cd C:\Users\mohamad\Projects\java-projects\paxos

call mvn exec:java -Dexec.mainClass="org.example.Main" -Dexec.args="2001 1 2001 2002 2003" -q
