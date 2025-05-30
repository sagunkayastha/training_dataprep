@echo off
setlocal enabledelayedexpansion

:: === Configurable ===
set "SUBFOLDER=%1"
set "LOCAL_DIR=F:\Forecasting_PS\Training_prep\GS_data\%SUBFOLDER%\site_data"
set "IMPUTED_DIR=F:\Forecasting_PS\Training_prep\GS_data\%SUBFOLDER%\site_data_imputed"
set "ZIP_NAME=site_data.zip"
set "RETURN_ZIP=site_data_return.zip"
set "SERVER=skayasth@Ampere"
set "REMOTE_DIR=/tng4/users/skayasth/Yearly/2025/May/Pollensense/Forecasting/laptop_impute"
set "REMOTE_EXTRACT_DIR=current_run"
set "REMOTE_OUTPUT_DIR=imputed_data"
set "PYTHON_SCRIPT=impute.py"
set "CONDA_ENV_PATH=/tng4/users/skayasth/envs/xesmf"

:: === Step 1: Zip locally ===
echo 📦 Zipping %LOCAL_DIR% into %ZIP_NAME%
powershell -command "Compress-Archive -Force -Path '%LOCAL_DIR%\*' -DestinationPath '%ZIP_NAME%'"

:: === Step 2: Send zip to server ===
echo 📤 Sending zip to server...
scp "%ZIP_NAME%" %SERVER%:%REMOTE_DIR%/

:: === Step 3: SSH unzip ===
echo 🚀 SSH to server and unzip input...
ssh %SERVER% "cd %REMOTE_DIR% && rm -rf %REMOTE_EXTRACT_DIR% %REMOTE_OUTPUT_DIR% && mkdir %REMOTE_EXTRACT_DIR% && unzip -o %ZIP_NAME% -d %REMOTE_EXTRACT_DIR%"

:: === Step 4: SSH to run Python script ===
echo 🐍 Running Python script...
ssh %SERVER% "cd %REMOTE_DIR% && source ~/.bashrc && source /tng4/users/skayasth/anaconda3/etc/profile.d/conda.sh && conda activate %CONDA_ENV_PATH% && python %PYTHON_SCRIPT%"


:: === Step 5: SSH to zip the output folder ===
echo 🗜️ Zipping output folder...
ssh %SERVER% "cd %REMOTE_DIR%/%REMOTE_OUTPUT_DIR% && zip -r ../%RETURN_ZIP% ./*"


:: === Step 6: Download re-zipped archive ===
echo 📥 Downloading result zip...
scp %SERVER%:%REMOTE_DIR%/%RETURN_ZIP% .

:: === Step 6.5: Clean up zip files on server ===
echo 🧹 Removing zip files from server...
ssh %SERVER% "cd %REMOTE_DIR% && rm -f %ZIP_NAME% %RETURN_ZIP%"

:: === Step 7: Extract locally to site_data_imputed ===
echo 🗃️ Extracting to: %IMPUTED_DIR%
rmdir /s /q "%IMPUTED_DIR%" 2>nul
powershell -command "Expand-Archive -Force -Path '%RETURN_ZIP%' -DestinationPath '%IMPUTED_DIR%'"

echo ✅ Done. Result extracted to:
echo     %IMPUTED_DIR%

endlocal
pause
