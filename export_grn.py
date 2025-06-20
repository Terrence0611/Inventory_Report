import psycopg2
import json
import csv
from datetime import datetime, date

def split_date_time(dt):
    if isinstance(dt, datetime):
        return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')
    elif isinstance(dt, date):
        return dt.strftime('%Y-%m-%d'), ""
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

uom_map = {
    1: "pc", 2: "ml", 3: "l", 4: "mg", 5: "g", 6: "kg", 7: "set", 8: "pack", 9: "box",
    10: "bottle", 11: "vial", 12: "pair", 13: "unit"
}

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

status_map = {
    1: "New", 2: "Stock Receive / Partial Receive", 3: "Completed", 4: "Cancelled"
}

def export_grn():
    conn = psycopg2.connect(
        host="aws-0-ap-southeast-1.pooler.supabase.com",
        port=5432,
        database="postgres",
        user="postgres.jyuynhxfjqswclroeurq",
        password="xaTxM7aJSH34q4vM"
    )
    cur = conn.cursor()

    cat_map = {}
    cur.execute('SELECT "id", "name" FROM "TblProductCategories"')
    for cid, cname in cur.fetchall():
        cat_map[str(cid)] = cname

    subcat_map = {}
    cur.execute('SELECT "id", "name" FROM "TblProductSubCategories"')
    for scid, scname in cur.fetchall():
        subcat_map[str(scid)] = scname

    user_map = {}
    cur.execute('SELECT "userId", "lastName" FROM "User"')
    for uid, lname in cur.fetchall():
        user_map[str(uid)] = lname

    supplier_map = {}
    cur.execute('SELECT "id", "name" FROM "TblSupplier"')
    for sid, sname in cur.fetchall():
        supplier_map[str(sid)] = sname

    cur.execute('''
    SELECT "goodreceivedid", "poId", "supplier_id", "goodReceivedQtyData",
           "goodReceivedDate", "status", "createdByUserId", "createdAt",
           "updatedAt", "brand_id", "goodReceivedData", "outletId", "remarks", "updatedByUserId"
    FROM "TblGoodReceived"
    ORDER BY "goodreceivedid" ASC
    ''')
    parent_rows = cur.fetchall()
    cur.close()
    conn.close()

    header = [
        "GRN ID", "PO ID", "Supplier ID", "Supplier Name",
        "Good Received Date (Date)", "Good Received Date (Time)", "Status",
        "Created By", "Created At (Date)", "Created At (Time)",
        "Updated By", "Updated At (Date)", "Updated At (Time)",
        "Brand", "Good Received Data", "Outlet", "Remarks",
        "Line Index", "Product ID", "Product Code", "Product Name",
        "Product Category", "Product Subcategory", "Qty", "UOM",
        "Batch Remark", "Expiry Date (Date)", "Expiry Date (Time)"
    ]

    today = datetime.now().strftime("%Y%m%d")
    csv_filename = f"grn_export_{today}.csv"
    extracted_rows = set()

    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for (
            grnid, poid, supplier_id, qty_json, received_date, status,
            created_user, created_at, updated_at, brand_id, good_received_data,
            outlet_id, remarks, updated_user
        ) in parent_rows:
            received_date_date, received_date_time = split_date_time(received_date)
            created_at_date, created_at_time = split_date_time(created_at)
            updated_at_date, updated_at_time = split_date_time(updated_at)
            supplier_name = supplier_map.get(str(supplier_id), "")
            created_by_name = user_map.get(str(created_user), created_user)
            updated_by_name = user_map.get(str(updated_user), updated_user)
            brand_word = brand_names.get(str(brand_id), brand_id)
            outlet_word = outlet_ids.get(str(outlet_id), outlet_id)

            try:
                qty_items = json.loads(qty_json) if qty_json else []
                if not isinstance(qty_items, list):
                    qty_items = [qty_items]
            except Exception as e:
                print(f"Could not parse goodReceivedQtyData for GRN {grnid}: {e}")
                qty_items = []

            for child in qty_items:
                prod_cat_id = str(child.get("product_category_id", ""))
                prod_subcat_id = str(child.get("product_sub_category_id", ""))
                uom_val = child.get("uom")
                product_inv_id = child.get("productInvId")
                batch_remark = child.get("batch_remark")
                expiry_date_val = child.get("product_expiry") or child.get("expiry_date")
                expiry_date_only, expiry_time_only = split_date_time(expiry_date_val)
                dedup_key = (grnid, product_inv_id, batch_remark, expiry_date_val)
                if dedup_key in extracted_rows:
                    continue
                extracted_rows.add(dedup_key)
                writer.writerow({
                    "GRN ID": grnid,
                    "PO ID": poid,
                    "Supplier ID": supplier_id,
                    "Supplier Name": supplier_name,
                    "Good Received Date (Date)": received_date_date,
                    "Good Received Date (Time)": received_date_time,
                    "Status": status_map.get(status, status),
                    "Created By": created_by_name,
                    "Created At (Date)": created_at_date,
                    "Created At (Time)": created_at_time,
                    "Updated By": updated_by_name,
                    "Updated At (Date)": updated_at_date,
                    "Updated At (Time)": updated_at_time,
                    "Brand": brand_word,
                    "Good Received Data": good_received_data,
                    "Outlet": outlet_word,
                    "Remarks": remarks,
                    "Line Index": child.get("index"),
                    "Product ID": child.get("productInvId"),
                    "Product Code": child.get("productCode"),
                    "Product Name": child.get("productName"),
                    "Product Category": cat_map.get(prod_cat_id, prod_cat_id),
                    "Product Subcategory": subcat_map.get(prod_subcat_id, prod_subcat_id),
                    "Qty": child.get("qty"),
                    "UOM": uom_map.get(uom_val, uom_val),
                    "Batch Remark": batch_remark,
                    "Expiry Date (Date)": expiry_date_only,
                    "Expiry Date (Time)": expiry_time_only,
                })
    print(f"Wrote {csv_filename}")
    return csv_filename

# For standalone usage:
if __name__ == "__main__":
    export_grn()
