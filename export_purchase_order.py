import psycopg2
import json
import csv
from datetime import datetime, date

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

uom_map = {
    1: "pc", 2: "ml", 3: "l", 4: "mg", 5: "g",
    6: "kg", 7: "set", 8: "pack", 9: "box", 10: "bottle",
    11: "vial", 12: "pair", 13: "unit"
}

def export_purchase_order():
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
        SELECT
            "purchaseorderid",
            "purchaseOrderQtyData",
            "supplier_id",
            "delivery_order_number",
            "purchaseorderDate",
            "createdByUserId",
            "createdAt",
            "updatedAt"
        FROM "TblPurchaseOrder"
        ORDER BY "purchaseorderid" ASC
    ''')
    parent_rows = cur.fetchall()
    cur.close()
    conn.close()

    header = [
        "PO ID", "Delivery Order No", "Supplier ID", "Supplier Name", "PO Date",
        "Product ID", "Product Code", "Product Name", "Product Category", "Product Subcategory",
        "Qty", "UOM", "Created By", "Created At (Date)", "Created At (Time)", "Updated At (Date)", "Updated At (Time)"
    ]

    today = datetime.now().strftime("%Y%m%d")
    csv_filename = f"purchase_order_export_{today}.csv"

    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for (
            poid, qty_json, supplier_id, do_no, po_date,
            created_user, created_at, updated_at
        ) in parent_rows:
            po_date_date, _ = split_date_time(po_date)
            created_at_date, created_at_time = split_date_time(created_at)
            updated_at_date, updated_at_time = split_date_time(updated_at)
            supplier_name = supplier_map.get(str(supplier_id), "")
            created_by = user_map.get(str(created_user), created_user)
            try:
                items = json.loads(qty_json) if qty_json else []
                if not isinstance(items, list):
                    items = [items]
            except Exception as e:
                print(f"Could not parse JSON for PO {poid}: {e}")
                items = []
            for child in items:
                prod_cat_id = str(child.get("product_category_id", ""))
                prod_subcat_id = str(child.get("product_sub_category_id", ""))
                uom_val = child.get("uom")
                writer.writerow({
                    "PO ID": poid,
                    "Delivery Order No": do_no,
                    "Supplier ID": supplier_id,
                    "Supplier Name": supplier_name,
                    "PO Date": po_date_date,
                    "Product ID": child.get("productInvId"),
                    "Product Code": child.get("productCode"),
                    "Product Name": child.get("productName"),
                    "Product Category": cat_map.get(prod_cat_id, prod_cat_id),
                    "Product Subcategory": subcat_map.get(prod_subcat_id, prod_subcat_id),
                    "Qty": child.get("qty"),
                    "UOM": uom_map.get(uom_val, uom_val),
                    "Created By": created_by,
                    "Created At (Date)": created_at_date,
                    "Created At (Time)": created_at_time,
                    "Updated At (Date)": updated_at_date,
                    "Updated At (Time)": updated_at_time,
                })
    print(f"Wrote {csv_filename}")
    return csv_filename

# Run script standalone
if __name__ == "__main__":
    export_purchase_order()
