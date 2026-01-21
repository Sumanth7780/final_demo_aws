@echo off
setlocal

set AWS_REGION=us-east-1
set SFN_NAME=final-demo-governed-lakehouse
set INPUT_FILE=stepfunctions/payloads/run_input.dev.json

for /f "delims=" %%A in ('aws stepfunctions list-state-machines --region %AWS_REGION% --query "stateMachines[?name=='%SFN_NAME%'].stateMachineArn | [0]" --output text') do set SFN_ARN=%%A

if "%SFN_ARN%"=="None" (
  echo ERROR: State machine not found: %SFN_NAME%
  exit /b 1
)

echo Starting execution...
aws stepfunctions start-execution ^
  --state-machine-arn "%SFN_ARN%" ^
  --input file://%INPUT_FILE% ^
  --region %AWS_REGION%

endlocal
