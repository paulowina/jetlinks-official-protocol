@echo off & setlocal enabledelayedexpansion
cd %
cd ../../..
mvn package -Dmaven.test.skip=true