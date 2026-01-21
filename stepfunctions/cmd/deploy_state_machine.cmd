@echo off
setlocal

REM ====== SET THESE ======
set AWS_REGION=us-east-1
set SFN_NAME=final-demo-governed-lakehouse

REM Find ARN by name
for /f "delims=" %%A in ('aws stepfunctions list-state-machines --region %AWS_REGION% --query "stateMachines[?name=='%SFN_NAME%'].stateMachineArn | [0]" --output text') do set SFN_ARN=%%A

if "%SFN_ARN%"=="None" (
  echo ERROR: State machine not found: %SFN_NAME%
  exit /b 1
)

echo Updating definition for: %SFN_ARN%
aws stepfunctions update-state-machine ^
  --state-machine-arn "%SFN_ARN%" ^
  --definition file://stepfunctions/definitions/final_demo.asl.json ^
  --region %AWS_REGION%

echo DONE.
endlocal
