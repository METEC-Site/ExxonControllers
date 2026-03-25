@echo off
setlocal

REM ── ExxonController Nuitka Windows Onefile Build ──────────────────────────
REM Run from the repo root (the directory containing app.py).
REM See BUILDING.md for full prerequisites.
REM
REM Options:
REM   --skip-deps   Skip pip install/upgrade step (faster on repeat builds)

set OUTPUT_DIR=dist
set /p VERSION=<VERSION
set OUTPUT_NAME=ExxonController_%VERSION%

REM Intermediate build files go here — outside OneDrive, deleted after the build.
set BUILD_CACHE=%TEMP%\ExxonController_build

REM Parse flags
set SKIP_DEPS=0
for %%A in (%*) do (
    if /I "%%A"=="--skip-deps" set SKIP_DEPS=1
)

if "%SKIP_DEPS%"=="0" (
    echo [build] Installing/upgrading build dependencies...
    python -m pip install --upgrade "nuitka[onefile]" ordered-set zstandard
    if %ERRORLEVEL% NEQ 0 (
        echo [build] FAILED to install dependencies.
        exit /b %ERRORLEVEL%
    )
) else (
    echo [build] Skipping dependency install ^(--skip-deps^).
)

echo [build] Cleaning previous output...
if exist "%OUTPUT_DIR%\%OUTPUT_NAME%.exe" del /q "%OUTPUT_DIR%\%OUTPUT_NAME%.exe"
if exist "%BUILD_CACHE%" rmdir /s /q "%BUILD_CACHE%"

echo [build] Running Nuitka ^(jobs=%NUMBER_OF_PROCESSORS%^)...
echo [build] Intermediate files: %BUILD_CACHE%  ^(deleted on success^)
python -m nuitka ^
    --onefile ^
    --onefile-tempdir-spec={CACHE_DIR}/{PRODUCT}/{VERSION} ^
    --jobs=%NUMBER_OF_PROCESSORS% ^
    --lto=no ^
    --output-filename=%OUTPUT_NAME%.exe ^
    --output-dir=%BUILD_CACHE% ^
    --windows-console-mode=force ^
    --windows-product-name="METEC Exxon Controller" ^
    --windows-file-description="METEC Exxon Controller — Alicat Flow Controller Web Interface" ^
    --windows-product-version=%VERSION%.0 ^
    --windows-file-version=%VERSION%.0 ^
    --include-package=flask ^
    --include-package=flask_socketio ^
    --include-package=engineio ^
    --include-package=socketio ^
    --include-package=jinja2 ^
    --include-package=werkzeug ^
    --include-package=markupsafe ^
    --include-package=itsdangerous ^
    --include-package=click ^
    --include-package=gevent ^
    --include-package=geventwebsocket ^
    --include-package=pymodbus ^
    --include-package=paho ^
    --include-package=dateutil ^
    --include-package=core ^
    --include-package=Phidget22 ^
    --include-package-data=flask ^
    --include-data-dir=templates=templates ^
    --include-data-dir=static=static ^
    --include-data-dir=config=_default_config ^
    --include-data-files=VERSION=VERSION ^
    --nofollow-import-to=gevent.tests ^
    --nofollow-import-to=gevent.testing ^
    --nofollow-import-to=gevent.testing.monkey_test ^
    --nofollow-import-to=gevent.testing.support ^
    --nofollow-import-to=gevent.tests.test__util ^
    --nofollow-import-to=gevent.tests.test__doctests ^
    --assume-yes-for-downloads ^
    app.py

if %ERRORLEVEL% NEQ 0 (
    echo [build] FAILED — see errors above.
    echo [build] Intermediate files left in: %BUILD_CACHE%
    exit /b %ERRORLEVEL%
)

REM Move the finished exe into dist\ and clean up all intermediate files.
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"
move /y "%BUILD_CACHE%\%OUTPUT_NAME%.exe" "%OUTPUT_DIR%\%OUTPUT_NAME%.exe" > nul
echo [build] Cleaning up intermediate files...
rmdir /s /q "%BUILD_CACHE%"

echo.
echo [build] SUCCESS: %OUTPUT_DIR%\%OUTPUT_NAME%.exe
echo.
echo Deploy by copying only %OUTPUT_NAME%.exe to the target machine.
echo On first run it will create config\ beside the exe from the bundled defaults.
echo config\ and Data\ persist across updates — only replace the .exe to update.
