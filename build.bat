@echo off
chcp 65001

echo 正在打包流程...
pyinstaller --onefile --console ^
  --hidden-import=requests ^
  --hidden-import=json ^
  --hidden-import=pandas  ^
  --clean ^
  main.py

if exist "dist\main.exe" (
    echo.
    echo  打包成功！
    echo  EXE文件位置: dist\main.exe
) else (
    echo  打包失败！
)

pause