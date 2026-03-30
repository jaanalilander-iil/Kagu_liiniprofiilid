@echo off
title Andmetootlus

cd /d "%~dp0"

echo ================================================
echo  prepare_data.py - liiniprofiilide andmetootlus
echo  Kaust: %~dp0
echo ================================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo VIGA: Python ei ole paigaldatud voi pole PATH-is.
    echo Laadi alla: https://www.python.org/downloads/
    echo Paigaldamisel vali Add Python to PATH.
    echo.
    pause
    exit /b 1
)

for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo OK: %%v

if not exist "prepare_data.py" (
    echo VIGA: prepare_data.py ei leitud kaustast %~dp0
    echo Veendu, et kaivita.bat ja prepare_data.py on samas kaustas.
    echo.
    pause
    exit /b 1
)
echo OK: prepare_data.py leitud.

python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo INFO: pandas pole paigaldatud. Paigaldan...
    python -m pip install pandas --quiet
    if errorlevel 1 (
        echo VIGA: pandas paigaldamine ebaonnestus.
        pause
        exit /b 1
    )
    echo OK: pandas paigaldatud.
) else (
    echo OK: pandas on olemas.
)
echo.

set CSV_FILE=%~1

if not "%CSV_FILE%"=="" (
    echo Tootlen: %CSV_FILE%
    echo.
    python prepare_data.py "%CSV_FILE%"
    if errorlevel 1 (
        echo.
        echo VIGA: Skript lopetas veaga.
        pause
        exit /b 1
    )
    goto done
)

set COUNT=0
for %%f in ("*.csv") do set /a COUNT+=1

if %COUNT%==0 (
    echo VIGA: CSV-faile ei leitud kaustast %~dp0
    echo Kopeeri CSV-failid samasse kausta kui kaivita.bat
    echo voi lohista CSV-fail selle .bat faili peale.
    echo.
    pause
    exit /b 1
)

echo Leitud %COUNT% CSV-faili. Tootlen koik...
echo.

for %%f in ("*.csv") do (
    echo ------------------------------------------------
    echo Tootlen: %%~nxf
    echo ------------------------------------------------
    python prepare_data.py "%%~nxf"
    if errorlevel 1 (
        echo VIGA: %%~nxf tootlemine ebaonnestus.
        pause
        exit /b 1
    )
    echo.
)

:done
echo.
echo ================================================
echo  Valmis!
echo  JSON-failid on kaustas: %~dp0data\
echo  manifest.json on uuendatud.
echo ================================================
echo.
pause
