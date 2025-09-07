@echo off
echo Exterminating all Stockfish engines...
taskkill /f /im "stockfish-windows-x86-64.exe" /t >nul 2>&1
echo Done.
