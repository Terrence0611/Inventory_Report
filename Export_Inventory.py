import psycopg2
import csv
from datetime import datetime
import zipfile

brand_names = {
    "1": "HQ", "2": "BL", "3": "VEN", "4": "CUR", "5": "B+"
}
outlet_ids = {
    "1": "EX-HQ", "2": "BL-Mid Valley", "3": "BL-Desa ParkCity",
    "4": "BL-Pavilion Bukit Jalil", "5": "BPLUS-SJ", "6": "BPLUS-Kota Damansara",
    "8": "BPLUS-SV2", "9": "BL-HQ", "11": "BL-1 Utama", "12": "BL-EkoCheras",
    "13": "BL-Setia City Mall", "18": "CUR-IOI Mall Damansara", "19": "CUR-Publika",
    "20": "CUR-Mid Valley", "24": "CUR-Pavilion Bukit Jalil", "25": "Ven-HQ",
    "26": "VEN-IOI Mall Damansara", "27": "VEN-Mid Valley", "31": "VEN-Pavilion Bukit Jalil",
    "32": "BPLUS-HQ", "33": "BPLUS-Damansara Utama", "36": "BPLUS-Pavilion Bukit Jalil",
    "37": "BPLUS-SJ2"
}

def split_date_time(dt):
    if not dt or dt == "" or dt is None:
        return "", ""
    if isinstance(dt, str):
        if "T" in dt:
            date_part, time_part = dt.split("T", 1)
            time_part = time_part.rstrip("Z")
            if "." in time_part:
                time_part = time_part.split(".", 1)[0]
            return date_part, time_part
        elif " " in dt:
            date_part, time_part = dt.split(" ", 1)
            return date_part, time_part
        else:
            return dt, ""
    else:
        return str(dt.date()), str(dt.time())

def export_inventory():
    conn = psycopg2.connect(
        host="aws-0-ap-southeast-1.pooler.supabase.com",
        port=5432,
        database="postgres",
        user="postgres.jyuynhxfjqswclroeurq",
        password="xaTxM7aJSH34q4vM"
    )
    cur = conn.cursor()

    # Fetch product id, name and product_code
    product_map = {}
    cur.execute('SELECT "id", "name", "product_code" FROM "TblProductInventory"')
    for pid, pname, pcode in cur.fetchall():
        product_map[str(pid)] = {"name": pname, "code": pcode}

    # User ID -> Last Name mapping
    user_lastname_map = {}
    cur.execute('SELECT "userId", "lastName" FROM "User"')
    for uid, lname in cur.fetchall():
        user_lastname_map[str(uid)] = lname

    cur.execute('''
        SELECT
            "batchModalid", "brand_id", "outletId", "productInventoryId", "stock_count",
            "batch_number", "expiry_date", "batch_remark", "createdByUserId", "updatedByUserId",
            "createdAt", "updatedAt"
        FROM "TblBatchModal"
        WHERE "brand_id" IN (1,2,3,4,5) AND "stock_count" <> 0
        ORDER BY "productInventoryId" ASC, "batchModalid" ASC
    ''')

    rows = cur.fetchall()
    brand_reports = {brand: [] for brand in brand_names.values()}

    for (
        batchModalid, brand_id, outletId, productInventoryId, stock_count,
        batch_number, expiry_date, batch_remark, createdByUserId, updatedByUserId,
        createdAt, updatedAt
    ) in rows:
        brand_label = brand_names.get(str(brand_id), str(brand_id))
        outlet_name = outlet_ids.get(str(outletId), outletId)
        product_info = product_map.get(str(productInventoryId), {"name": "", "code": ""})
        product_name = product_info["name"]
        product_code = product_info["code"]
        created_by = user_lastname_map.get(str(createdByUserId), createdByUserId)
        updated_by = user_lastname_map.get(str(updatedByUserId), updatedByUserId)
        createdAt_date, createdAt_time = split_date_time(createdAt)
        updatedAt_date, updatedAt_time = split_date_time(updatedAt)

        if brand_label:
            brand_reports[brand_label].append({
                "productInventoryId": productInventoryId,
                "Product Name": product_name,
                "product_code": product_code,
                "batchModalid": batchModalid,
                "batch_remark": batch_remark,
                "batch_number": batch_number,
                "stock_count": stock_count,
                "expiry_date": expiry_date,
                "brand": brand_label,
                "Outlet Name": outlet_name,
                "Created By": created_by,
                "Updated By": updated_by,
                "createdAt Date": createdAt_date,
                "createdAt Time": createdAt_time,
                "updatedAt Date": updatedAt_date,
                "updatedAt Time": updatedAt_time
            })

    csv_filenames = []
    header = [
        "productInventoryId", "product_code", "Product Name", "batchModalid", "batch_remark", "batch_number",
        "stock_count", "expiry_date", "brand", "Outlet Name", "Created By", "Updated By",
        "createdAt Date", "createdAt Time", "updatedAt Date", "updatedAt Time"
    ]

    for brand, records in brand_reports.items():
        records_sorted = sorted(
            records,
            key=lambda r: (
                int(r["productInventoryId"]) if str(r["productInventoryId"]).isdigit() else str(r["productInventoryId"]),
                int(r["batchModalid"]) if r["batchModalid"] is not None else 0
            )
        )
        filename = f"inventory_report_{brand}.csv"
        with open(filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for row in records_sorted:
                writer.writerow(row)
        print(f"Wrote {filename}")
        csv_filenames.append(filename)

    today = datetime.now().strftime("%Y%m%d")
    zip_filename = f"inv_bal as at {today}.zip"
    with zipfile.ZipFile(zip_filename, "w") as myzip:
        for csv_file in csv_filenames:
            myzip.write(csv_file)
    print(f"Created ZIP: {zip_filename}")

    cur.close()
    conn.close()
    return zip_filename

if __name__ == "__main__":
    export_inventory()
