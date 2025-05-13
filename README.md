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
  
python -m Pyinstaller --onefile --noconsole  --add-data "config.json;." --add-data "data;data" --add-data "helpers;helpers" --hidden-import customtkinter --hidden-import undetected_chromedriver   salesforce_scraper_gui.pypyinstaller --onefile --noconsole  --add-data "config.json;." --add-data "data;data" --add-data "helpers;helpers" --hidden-import customtkinter --hidden-import undetected_chromedriver   salesforce_scraper_gui.py