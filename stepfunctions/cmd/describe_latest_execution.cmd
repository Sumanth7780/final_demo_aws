@echo off
setlocal

set AWS_REGION=us-east-1
set SFN_NAME=final-demo-governed-lakehouse

for /f "delims=" %%A in ('aws stepfunctions list-state-machines --region %AWS_REGION% --query "stateMachines[?name=='%SFN_NAME%'].stateMachineArn | [0]" --output text') do set SFN_ARN=%%A

for /f "delims=" %%E in ('aws stepfunctions list-executions --state-machine-arn "%SFN_ARN%" --max-results 1 --region %AWS_REGION% --query "executions[0].executionArn" --output text') do set EXEC_ARN=%%E

echo Latest execution: %EXEC_ARN%
aws stepfunctions describe-execution --execution-arn "%EXEC_ARN%" --region %AWS_REGION%

endlocal
