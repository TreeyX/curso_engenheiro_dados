@echo off
python -m pipenv install
echo.
python -m pipenv run python main.py
echo.
exit