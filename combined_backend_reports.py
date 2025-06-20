from export_stock_transfer import export_stock_transfer
from export_inventory import export_inventory
from export_purchase_order import export_purchase_order
from export_grn import export_grn
from export_stock_adjustment import export_stock_adjustment
from min_qty_inv import export_min_qty_inventory  # <--- ADD THIS LINE

import zipfile
from datetime import datetime
import subprocess
import os

def run_and_zip_all_exports():
    csv_or_zip_files = [
        export_stock_transfer(),
        export_inventory(),
        export_purchase_order(),
        export_grn(),
        export_stock_adjustment(),
        export_min_qty_inventory(),   # <--- ADD THIS CALL
    ]
    csv_or_zip_files = [f for f in csv_or_zip_files if f and os.path.exists(f)]
    today = datetime.now().strftime("%Y%m%d")
    zip_filename = f"all_backend_reports_{today}.zip"
    with zipfile.ZipFile(zip_filename, "w") as myzip:
        for report_file in csv_or_zip_files:
            myzip.write(report_file)
    print(f"Created ZIP: {zip_filename}")
    return zip_filename

if __name__ == "__main__":
    final_zip = run_and_zip_all_exports()
    subprocess.run(["python", "send_to_slack.py", final_zip])
