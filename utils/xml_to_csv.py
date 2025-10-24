import sys
import xml.etree.ElementTree as ET
import pandas as pd

# Parse XML from file
if len(sys.argv) < 2:
    print("Usage: python xml_to_csv.py <xml_file_path>")
    sys.exit(1)
xml_path = sys.argv[1]
tree = ET.parse(xml_path)
root = tree.getroot()

# Extract product data
rows = []
for sku in root.findall(".//Stock_Keeping_Unit"):
    data = {
        "Item_Number": sku.attrib.get("Item_Number", ""),
        "Price": sku.findtext("Price", ""),
        "English_Description": sku.findtext("English_Description", "").strip(),
        "French_Description": sku.findtext("French_Description", "").strip(),
        "Department": sku.findtext("Department", ""),
        "UPC": sku.findtext(".//UPC", ""),
        "Conexxus_Product_Code": sku.findtext("Conexxus_Product_Code", ""),
        "Owner": sku.findtext("Owner", ""),
        "Loyalty_Card_Eligible": sku.findtext("Loyalty_Card_Eligible", ""),
        "Age_Requirements": sku.findtext("Age_Requirements", ""),
        "TAX1": sku.findtext("TAX1", ""),
        "TAX2": sku.findtext("TAX2", ""),
        "TAX3": sku.findtext("TAX3", ""),
    }
    rows.append(data)

# Convert to CSV and output to stdout
df = pd.DataFrame(rows)
df.to_csv(sys.stdout, index=False)
