import psycopg2
import json
import csv
from datetime import datetime

def split_date_time(dt):
    if not dt or dt == "" or dt is None:
        return "", ""
    if isinstance(dt, str):
        if "T" in dt:
            date_part, time_part = dt.split("T", 1)
            time_part = time_part.replace("Z", "")
            if "." in time_part:
                time_part = time_part.split(".", 1)[0]
            return date_part, time_part
        elif " " in dt:
            date_part, time_part = dt.split(" ", 1)
            return date_part, time_part
        else:
            return dt, ""
    else:
        try:
            return str(dt.date()), str(dt.time())
        except Exception:
            return str(dt), ""

def export_stock_transfer():
    # Mappings (exact as needed)
    outlet_ids = {
        "EX-HQ": 1, "BL-Mid Valley": 2, "BL-Desa ParkCity": 3, "BL-Pavilion Bukit Jalil": 4, "BPLUS-SJ": 5,
        "BPLUS-Kota Damansara": 6, "BL-HQ": 9, "BPLUS-SV2": 8, "BL-1 Utama": 11, "BL-EkoCheras": 12,
        "BL-Setia City Mall": 13, "CUR-IOI Mall Damansara": 18, "CUR-Publika": 19, "CUR-Mid Valley": 20,
        "CUR-Pavilion Bukit Jalil": 24, "Ven-HQ": 25, "VEN-IOI Mall Damansara": 26, "VEN-Mid Valley": 27,
        "VEN-Pavilion Bukit Jalil": 31, "BPLUS-HQ": 32, "BPLUS-Damansara Utama": 33, "BPLUS-Pavilion Bukit Jalil": 36,
    }
    outlet_lookup = {str(v): k for k, v in outlet_ids.items()}
    brand_ids = {"1": "HQ", "2": "BL", "3": "VEN", "4": "CUR", "5": "B+"}

    conn = psycopg2.connect(
        host="aws-0-ap-southeast-1.pooler.supabase.com",
        port=5432,
        database="postgres",
        user="postgres.jyuynhxfjqswclroeurq",
        password="xaTxM7aJSH34q4vM"
    )
    cur = conn.cursor()

    # Build product ID-to-name map
    product_name_map = {}
    cur.execute('SELECT "id", "name" FROM "TblProductInventory"')
    for pid, pname in cur.fetchall():
        product_name_map[str(pid)] = pname

    # Query all rows, sorted
    cur.execute('''
        SELECT
            "stocktransferid",
            "stockTransferQtyData",
            "brandIdFrom",
            "locationIdFrom",
            "brandIdTo",
            "locationIdTo",
            "createdAt",
            "remarks"
        FROM "TblStockTransfer"
        ORDER BY "stocktransferid" ASC
    ''')
    rows = cur.fetchall()

    date_str = datetime.now().strftime("%Y%m%d")
    csv_filename = f"stock_transfer_export_{date_str}.csv"
    csv_columns = [
        "Stock Transfer ID", "Brand ID From", "Location ID From", "Brand ID To", "Location ID To",
        "Created At Date", "Created At Time", "Remarks", "Product ID", "Product Name", "Product Total Qty",
        "Batch ID", "Batch Qty", "Batch Number", "Batch Expiry Date", "Batch Expiry Time", "Batch Remark"
    ]

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=csv_columns)
        writer.writeheader()
        for row in rows:
            (stocktransferid, json_data, brandIdFrom, locationIdFrom,
             brandIdTo, locationIdTo, createdAt, remarks) = row
            created_at_date, created_at_time = split_date_time(createdAt)

            brandIdFrom_str = str(brandIdFrom) if brandIdFrom is not None else ""
            brandIdTo_str = str(brandIdTo) if brandIdTo is not None else ""
            brand_from_name = brand_ids.get(brandIdFrom_str, brandIdFrom)
            brand_to_name = brand_ids.get(brandIdTo_str, brandIdTo)

            locationIdFrom_str = str(locationIdFrom) if locationIdFrom is not None else ""
            locationIdTo_str = str(locationIdTo) if locationIdTo is not None else ""
            location_from_name = outlet_lookup.get(locationIdFrom_str, locationIdFrom)
            location_to_name = outlet_lookup.get(locationIdTo_str, locationIdTo)

            if isinstance(json_data, str):
                try:
                    qty_data = json.loads(json_data)
                except Exception:
                    qty_data = None
            else:
                qty_data = json_data

            # CASE 1: dict with "products" key
            if isinstance(qty_data, dict) and "products" in qty_data and qty_data["products"]:
                for product in qty_data["products"]:
                    product_id = product.get("productId")
                    json_product_name = product.get("productName")
                    if not json_product_name or str(json_product_name).strip() == "":
                        product_name = product_name_map.get(str(product_id), "")
                    else:
                        product_name = json_product_name
                    product_total_qty = product.get("totalQty") or product.get("qty") or ""
                    batches = product.get("batches", [])
                    if not batches:
                        writer.writerow({
                            "Stock Transfer ID": stocktransferid,
                            "Brand ID From": brand_from_name,
                            "Location ID From": location_from_name,
                            "Brand ID To": brand_to_name,
                            "Location ID To": location_to_name,
                            "Created At Date": created_at_date,
                            "Created At Time": created_at_time,
                            "Remarks": remarks,
                            "Product ID": product_id,
                            "Product Name": product_name,
                            "Product Total Qty": product_total_qty,
                            "Batch ID": "",
                            "Batch Qty": "",
                            "Batch Number": "",
                            "Batch Expiry Date": "",
                            "Batch Expiry Time": "",
                            "Batch Remark": ""
                        })
                    else:
                        for batch in batches:
                            try:
                                qty_value = batch.get("qty")
                                if qty_value is not None and str(qty_value).strip() != "" and int(qty_value) == 0:
                                    continue
                            except Exception:
                                pass
                            expiry_date, expiry_time = split_date_time(batch.get("expiryDate"))
                            writer.writerow({
                                "Stock Transfer ID": stocktransferid,
                                "Brand ID From": brand_from_name,
                                "Location ID From": location_from_name,
                                "Brand ID To": brand_to_name,
                                "Location ID To": location_to_name,
                                "Created At Date": created_at_date,
                                "Created At Time": created_at_time,
                                "Remarks": remarks,
                                "Product ID": product_id,
                                "Product Name": product_name,
                                "Product Total Qty": product_total_qty,
                                "Batch ID": batch.get("batchId"),
                                "Batch Qty": batch.get("qty"),
                                "Batch Number": batch.get("batchNumber", ""),
                                "Batch Expiry Date": expiry_date,
                                "Batch Expiry Time": expiry_time,
                                "Batch Remark": batch.get("batchRemark", "")
                            })
            # CASE 2: list
            elif isinstance(qty_data, list) and qty_data:
                for item in qty_data:
                    if "batchId" in item and "productInvId" in item:
                        try:
                            qty_value = item.get("qty")
                            if qty_value is not None and str(qty_value).strip() != "" and int(qty_value) == 0:
                                continue
                        except Exception:
                            pass
                        product_id = item.get("productInvId")
                        product_name = product_name_map.get(str(product_id), "")
                        writer.writerow({
                            "Stock Transfer ID": stocktransferid,
                            "Brand ID From": brand_from_name,
                            "Location ID From": location_from_name,
                            "Brand ID To": brand_to_name,
                            "Location ID To": location_to_name,
                            "Created At Date": created_at_date,
                            "Created At Time": created_at_time,
                            "Remarks": remarks,
                            "Product ID": product_id,
                            "Product Name": product_name,
                            "Product Total Qty": "",
                            "Batch ID": item.get("batchId"),
                            "Batch Qty": item.get("qty"),
                            "Batch Number": "",
                            "Batch Expiry Date": "",
                            "Batch Expiry Time": "",
                            "Batch Remark": ""
                        })
                    elif "productInvId" in item:
                        try:
                            qty_value = item.get("qty")
                            if qty_value is not None and str(qty_value).strip() != "" and int(qty_value) == 0:
                                continue
                        except Exception:
                            pass
                        product_id = item.get("productInvId")
                        json_product_name = item.get("productName")
                        if not json_product_name or str(json_product_name).strip() == "":
                            product_name = product_name_map.get(str(product_id), "")
                        else:
                            product_name = json_product_name
                        writer.writerow({
                            "Stock Transfer ID": stocktransferid,
                            "Brand ID From": brand_from_name,
                            "Location ID From": location_from_name,
                            "Brand ID To": brand_to_name,
                            "Location ID To": location_to_name,
                            "Created At Date": created_at_date,
                            "Created At Time": created_at_time,
                            "Remarks": remarks,
                            "Product ID": product_id,
                            "Product Name": product_name,
                            "Product Total Qty": item.get("qty"),
                            "Batch ID": "",
                            "Batch Qty": "",
                            "Batch Number": "",
                            "Batch Expiry Date": "",
                            "Batch Expiry Time": "",
                            "Batch Remark": ""
                        })
            # CASE 3: Nothing usable
            else:
                writer.writerow({
                    "Stock Transfer ID": stocktransferid,
                    "Brand ID From": brand_from_name,
                    "Location ID From": location_from_name,
                    "Brand ID To": brand_to_name,
                    "Location ID To": location_to_name,
                    "Created At Date": created_at_date,
                    "Created At Time": created_at_time,
                    "Remarks": remarks,
                    "Product ID": "",
                    "Product Name": "",
                    "Product Total Qty": "",
                    "Batch ID": "",
                    "Batch Qty": "",
                    "Batch Number": "",
                    "Batch Expiry Date": "",
                    "Batch Expiry Time": "",
                    "Batch Remark": ""
                })
    print(f"Exported data to {csv_filename}")
    return csv_filename

# If run standalone (not just imported), run the function:
if __name__ == "__main__":
    export_stock_transfer()
