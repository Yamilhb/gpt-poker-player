from pathlib import Path
import api.configuration as configuration

# Project Directories
PACKAGE_ROOT = Path(configuration.__file__).resolve().parent.parent
ROOT = PACKAGE_ROOT.parent
CONFIG_DIR = PACKAGE_ROOT / "configuration"
HIST_DIR = PACKAGE_ROOT / "historical"
RAW_DIR = HIST_DIR / "rawdata"
TAB_DIR = HIST_DIR / "tabdata"
PROCESS_DIR = PACKAGE_ROOT / "process"
IMG_DIR = PACKAGE_ROOT / "images"
print(IMG_DIR)