git pull

pip install -r requirements.txt

pip install undetected_chromedriver

pip install requests

.\venv\Scripts\Activate.ps1

python -m PyInstaller --onefile --noconsole `
  --add-data "config.json;." `
  --add-data "data;data" `
  --add-data "helpers;helpers" `
  --hidden-import customtkinter `
  --hidden-import undetected_chromedriver `
  salesforce_scraper_gui.py
  
python -m Pyinstaller --onefile --noconsole  --add-data "config.json;." --add-data "data;data" --add-data "helpers;helpers" --hidden-import customtkinter --hidden-import undetected_chromedriver   salesforce_scraper_gui.py

pyinstaller --onefile --noconsole --icon icon.ico --add-data "config.json;." --add-data "data;data" --add-data "helpers;helpers" --add-data "chrome;chrome" --hidden-import customtkinter --hidden-import undetected_chromedriver   salesforce_scraper_gui.py 