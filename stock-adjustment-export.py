import psycopg2
import json
import csv
from datetime import datetime, date

brand_names = {
    "1": "HQ", "2": "BL", "3": "VEN", "4": "CUR", "5": "B+"
}

outlet_ids = {
    "1": "EX-HQ", "2": "BL-Mid Valley", "3": "BL-Desa ParkCity",
    "4": "BL-Pavilion Bukit Jalil", "5": "BPLUS-SJ",
    "6": "BPLUS-Kota Damansara", "8": "BPLUS-SV2", "9": "BL-HQ", "11": "BL-1 Utama",
    "12": "BL-EkoCheras", "13": "BL-Setia City Mall", "18": "CUR-IOI Mall Damansara",
    "19": "CUR-Publika", "20": "CUR-Mid Valley", "24": "CUR-Pavilion Bukit Jalil",
    "25": "Ven-HQ", "26": "VEN-IOI Mall Damansara", "27": "VEN-Mid Valley",
    "31": "VEN-Pavilion Bukit Jalil", "32": "BPLUS-HQ", "33": "BPLUS-Damansara Utama",
    "36": "BPLUS-Pavilion Bukit Jalil", "37": "BPLUS-SJ2"
}

status_map = {
    1: "In Progress",
    2: "Completed",
    3: "Cancelled"
}

def split_date_time(dt):
    if not dt or dt == "" or dt is None:
        return "", ""
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
    elif isinstance(dt, date):
        return dt.strftime("%Y-%m-%d"), ""
    elif isinstance(dt, str):
        if "T" in dt:
            date_part, time_part = dt.split("T", 1)
            time_part = time_part.rstrip("Z")
            if "." in time_part:
                time_part = time_part.split(".", 1)[0]
            return date_part, time_part
        elif " " in dt:
            date_part, time_part = dt.split(" ", 1)
            if "." in time_part:
                time_part = time_part.split(".", 1)[0]
            return date_part, time_part
        else:
            return dt, ""
    else:
        return str(dt), ""

def export_stock_adjustment():
    conn = psycopg2.connect(
        host="aws-0-ap-southeast-1.pooler.supabase.com",
        port=5432,
        database="postgres",
        user="postgres.jyuynhxfjqswclroeurq",
        password="xaTxM7aJSH34q4vM"
    )
    cur = conn.cursor()

    product_name_map = {}
    cur.execute('SELECT "id", "name" FROM "TblProductInventory"')
    for pid, pname in cur.fetchall():
        product_name_map[str(pid)] = pname

    user_lastname_map = {}
    cur.execute('SELECT "userId", "lastName" FROM "User"')
    for uid, lname in cur.fetchall():
        user_lastname_map[str(uid)] = lname

    cur.execute('''
        SELECT
            "stockadjustmentid",
            "stockAdjustmentQtyData",
            "remarks",
            "brandId",
            "locationId",
            "approved",
            "approvedByUserId",
            "approvedDate",
            "createdByUserId",
            "updatedByUserId",
            "createdAt",
            "updatedAt",
            "status"
        FROM "TblStockAdjustment"
        ORDER BY "stockadjustmentid" ASC
    ''')
    parent_rows = cur.fetchall()
    cur.close()
    conn.close()

    header = [
        "Stock Adjustment ID", "Brand ID", "Outlet ID", "Product ID", "Product Name",
        "From Qty", "Adjusted Qty", "Batch ID", "Remark", "Status", "Approved Status",
        "Created User ID", "Created At (Date)", "Created At (Time)",
        "Updated At (Date)", "Updated At (Time)"
    ]

    today = datetime.now().strftime("%Y%m%d")
    csv_filename = f"stock_adjustment_export_{today}.csv"

    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for (
            stockadjustmentid, json_data, remarks, brandId, locationId, approved,
            approvedByUserId, approvedDate, createdByUserId, updatedByUserId,
            createdAt, updatedAt, status
        ) in parent_rows:
            try:
                qty_items = json.loads(json_data) if json_data else []
                if not isinstance(qty_items, list):
                    qty_items = [qty_items]
            except Exception as e:
                print(f"Could not parse JSON for ID {stockadjustmentid}: {e}")
                qty_items = []
            createdAt_date, createdAt_time = split_date_time(createdAt)
            updatedAt_date, updatedAt_time = split_date_time(updatedAt)
            for element in qty_items:
                pid_str = str(element.get("productInvId") or "")
                outletId_str = str(locationId) if locationId is not None else ""
                brandId_str = str(brandId) if brandId is not None else ""
                row_out = {
                    "Stock Adjustment ID": stockadjustmentid,
                    "Brand ID": brand_names.get(brandId_str, brandId_str),
                    "Outlet ID": outlet_ids.get(outletId_str, outletId_str),
                    "Product ID": pid_str,
                    "Product Name": product_name_map.get(pid_str, ""),
                    "From Qty": element.get("stockCountFromOutlet"),
                    "Adjusted Qty": element.get("qty"),
                    "Batch ID": element.get("batchId"),
                    "Remark": element.get("remark", remarks or ""),
                    "Status": status_map.get(status, status),
                    "Approved Status": "Approved" if str(approved).upper() == "TRUE" else "No Approved",
                    "Created User ID": user_lastname_map.get(str(createdByUserId), createdByUserId),
                    "Created At (Date)": createdAt_date,
                    "Created At (Time)": createdAt_time,
                    "Updated At (Date)": updatedAt_date,
                    "Updated At (Time)": updatedAt_time,
                }
                writer.writerow(row_out)
    print(f"Wrote {csv_filename}")
    return csv_filename

if __name__ == "__main__":
    export_stock_adjustment()
