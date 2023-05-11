@echo off & setlocal enabledelayedexpansion
cd %
cd ../../..
mvn clean package -Dmaven.test.skip=true