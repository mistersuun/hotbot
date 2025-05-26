# Salesforce Scraper GUI

A powerful GUI application for scraping Salesforce data with advanced features including city and street data integration, multi-threaded scraping, and a modern dark-mode interface.

## Features

- Modern dark-mode GUI built with CustomTkinter
- Multi-threaded Salesforce data scraping
- City and street data integration using Overpass API
- Configurable scraping parameters
- Robust error handling and logging
- Support for both headless and visible browser modes
- Automatic data caching for improved performance

## Prerequisites

- Python 3.8 or higher
- Chrome browser (version 136 or compatible)
- Git (for version control)

## Installation

1. Clone the repository:
```bash
git clone <your-repository-url>
cd salesforce-scraper
```

2. Create and activate a virtual environment:
```bash
# Windows
python -m venv venv
.\venv\Scripts\Activate.ps1

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The application uses a `config.json` file for configuration. Default settings include:

```json
{
    "max_parallel_tabs": 5,
    "mfa_timeout_sec": 60,
    "overpass_timeout": 120,
    "selenium_headless": false
}
```

You can modify these settings according to your needs.

## Usage

1. Run the application:
```bash
python salesforce_scraper_gui.py
```

2. Enter your Salesforce credentials in the GUI
3. Select the city and street (optional)
4. Click "Start" to begin scraping
5. Monitor progress in the log window
6. Results will be saved in the `data` directory

## Building Executable

To create a standalone executable:

```bash
pyinstaller --onefile --noconsole --icon icon.ico \
    --add-data "config.json;." \
    --add-data "data;data" \
    --add-data "helpers;helpers" \
    --add-data "chrome;chrome" \
    --hidden-import customtkinter \
    --hidden-import undetected_chromedriver \
    salesforce_scraper_gui.py
```

## Project Structure

```
├── salesforce_scraper_gui.py    # Main application file
├── config.json                  # Configuration file
├── requirements.txt            # Python dependencies
├── data/                       # Data storage directory
├── helpers/                    # Utility functions
├── logs/                       # Log files
├── chrome/                     # Chrome-related resources
└── venv/                       # Virtual environment
```

## Development

### Setting up Pre-commit Hooks

1. Install pre-commit:
```bash
pip install pre-commit
```

2. Install the pre-commit hooks:
```bash
pre-commit install
```

3. (Optional) Run pre-commit on all files:
```bash
pre-commit run --all-files
```

The pre-commit hooks will automatically:
- Remove trailing whitespace
- Fix file endings
- Check YAML syntax
- Check for large files
- Format code with Black
- Check code quality with Flake8
- Sort imports with isort

## Troubleshooting

1. **Chrome Driver Issues**
   - Ensure Chrome is installed and up to date
   - Check the `chrome` directory for the correct driver version

2. **Authentication Problems**
   - Verify your Salesforce credentials
   - Check network connectivity
   - Ensure MFA timeout settings are appropriate

3. **Performance Issues**
   - Adjust `max_parallel_tabs` in config.json
   - Consider enabling headless mode for better performance

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Your License Here]

## Support

For support, please [create an issue](your-repository-issues-url) in the repository.
