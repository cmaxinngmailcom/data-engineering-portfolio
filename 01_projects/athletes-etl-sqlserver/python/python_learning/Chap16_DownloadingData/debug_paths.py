import os
from pathlib import Path

print("CWD:", os.getcwd())

base = Path(os.getcwd())
chap16_dir = base / "Chap16_DownloadingData"

print("\nDoes Chap16_DownloadingData exist?", chap16_dir.exists())
if chap16_dir.exists():
    print("Contents of Chap16_DownloadingData:")
    for item in chap16_dir.iterdir():
        print("  -", item.name)

    data_dir = chap16_dir / "Data"
    print("\nDoes Data folder exist?", data_dir.exists())
    if data_dir.exists():
        print("Contents of Data:")
        for item in data_dir.iterdir():
            print("  -", item.name)
    else:
        print("Data folder NOT found under Chap16_DownloadingData")
else:
    print("Chap16_DownloadingData folder NOT found under", base)
