"""Generate template CSV data files for Flowfile flow templates.

Run this script once to generate all CSV files, then commit them to the repo.
Uses random.seed(42) for reproducible output.

Usage:
    python generate_template_data.py
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

OUTPUT_DIR = Path(__file__).parent


def write_csv(filename: str, headers: list[str], rows: list[list]) -> None:
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  Written {len(rows)} rows to {filename}")


def random_date(start: datetime, end: datetime) -> str:
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")


def generate_sales_data():
    """~1000 sales records for filtering, aggregation, sorting."""
    products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones", "Webcam"]
    categories = {"Laptop": "Computers", "Phone": "Mobile", "Tablet": "Mobile", "Monitor": "Computers",
                  "Keyboard": "Accessories", "Mouse": "Accessories", "Headphones": "Audio", "Webcam": "Accessories"}
    regions = ["North", "South", "East", "West"]
    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    rows = []
    for i in range(1, 1001):
        product = random.choice(products)
        rows.append([
            i,
            product,
            categories[product],
            random.choice(regions),
            round(random.uniform(25, 2500), 2),
            random_date(start, end),
        ])
    write_csv("sales_data.csv", ["order_id", "product", "category", "region", "amount", "date"], rows)


def generate_customers():
    """~500 customers with intentional duplicates for deduplication demo."""
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
                   "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
                   "Thomas", "Sarah", "Charles", "Karen", "Emma", "Oliver", "Sophia", "Liam"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
              "San Antonio", "San Diego", "Dallas", "Austin", "Seattle", "Denver", "Boston"]
    segments = ["Premium", "Standard", "Basic"]
    start = datetime(2020, 1, 1)
    end = datetime(2024, 6, 30)

    rows = []
    emails_used = []
    for i in range(1, 451):
        first = random.choice(first_names)
        last = random.choice(last_names)
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 99)}@example.com"
        emails_used.append((first, last, email))
        rows.append([
            f"C{i:04d}",
            f"{first} {last}",
            email,
            random.choice(cities),
            random.choice(segments),
            random_date(start, end),
        ])

    # Add ~50 intentional duplicates (same email, possibly slightly different data)
    for _ in range(50):
        orig = random.choice(emails_used)
        rows.append([
            f"C{len(rows) + 1:04d}",
            f"{orig[0]} {orig[1]}",
            orig[2],  # same email = duplicate
            random.choice(cities),
            random.choice(segments),
            random_date(start, end),
        ])
    random.shuffle(rows)
    write_csv("customers.csv", ["customer_id", "name", "email", "city", "segment", "signup_date"], rows)


def generate_employees():
    """~200 employees for formula, filter, sort demo."""
    first_names = ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Hank",
                   "Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Pete",
                   "Quinn", "Rosa", "Sam", "Tina", "Uma", "Vic", "Wendy", "Xander"]
    last_names = ["Adams", "Baker", "Clark", "Dixon", "Ellis", "Fisher", "Grant", "Hayes",
                  "Irwin", "Jensen", "Klein", "Lopez", "Morgan", "Nash", "Owen", "Park"]
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Legal", "Support"]
    statuses = ["Active", "Active", "Active", "Active", "Inactive"]  # 80% active

    rows = []
    for i in range(1, 201):
        rows.append([
            i,
            random.choice(first_names),
            random.choice(last_names),
            random.choice(departments),
            random.choice(statuses),
            round(random.uniform(45000, 150000), 2),
        ])
    write_csv("employees.csv", ["emp_id", "first_name", "last_name", "department", "status", "salary"], rows)


def generate_products():
    """~50 products for join enrichment."""
    product_data = [
        ("Laptop Pro 15", "Computers", 1299.99), ("Laptop Air 13", "Computers", 999.99),
        ("Desktop Tower", "Computers", 1599.99), ("Mini PC", "Computers", 499.99),
        ("Phone X", "Mobile", 899.99), ("Phone SE", "Mobile", 429.99),
        ("Phone Plus", "Mobile", 1099.99), ("Tablet 10", "Mobile", 599.99),
        ("Tablet Mini", "Mobile", 399.99), ("Tablet Pro", "Mobile", 799.99),
        ("Monitor 27", "Displays", 449.99), ("Monitor 32", "Displays", 599.99),
        ("Monitor Ultra", "Displays", 899.99), ("Portable Monitor", "Displays", 299.99),
        ("Mechanical Keyboard", "Accessories", 129.99), ("Wireless Keyboard", "Accessories", 79.99),
        ("Gaming Mouse", "Accessories", 69.99), ("Ergonomic Mouse", "Accessories", 89.99),
        ("Wireless Mouse", "Accessories", 49.99), ("USB Hub", "Accessories", 39.99),
        ("Webcam HD", "Accessories", 79.99), ("Webcam 4K", "Accessories", 149.99),
        ("Headphones Pro", "Audio", 299.99), ("Headphones Basic", "Audio", 79.99),
        ("Earbuds Pro", "Audio", 199.99), ("Earbuds Basic", "Audio", 49.99),
        ("Speaker Portable", "Audio", 129.99), ("Speaker Desktop", "Audio", 89.99),
        ("Microphone USB", "Audio", 119.99), ("Microphone Pro", "Audio", 249.99),
        ("Mouse Pad XL", "Accessories", 29.99), ("Laptop Stand", "Accessories", 59.99),
        ("Cable USB-C", "Accessories", 14.99), ("Charger Fast", "Accessories", 34.99),
        ("Power Bank", "Accessories", 49.99), ("Screen Protector", "Accessories", 19.99),
        ("Phone Case", "Accessories", 24.99), ("Tablet Case", "Accessories", 34.99),
        ("Docking Station", "Accessories", 179.99), ("External SSD 1TB", "Storage", 109.99),
        ("External SSD 2TB", "Storage", 179.99), ("Flash Drive 128GB", "Storage", 19.99),
        ("SD Card 256GB", "Storage", 39.99), ("NAS Drive", "Storage", 299.99),
        ("Router WiFi 6", "Networking", 149.99), ("Network Switch", "Networking", 49.99),
        ("Ethernet Cable", "Networking", 9.99), ("WiFi Adapter", "Networking", 29.99),
        ("Printer Laser", "Office", 249.99), ("Printer Inkjet", "Office", 129.99),
    ]
    rows = []
    for i, (name, category, price) in enumerate(product_data, 1):
        rows.append([i, name, category, price])
    write_csv("products.csv", ["product_id", "name", "category", "unit_price"], rows)


def generate_orders():
    """~1000 orders referencing products and customers for join enrichment."""
    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    rows = []
    for i in range(1, 1001):
        rows.append([
            i,
            f"C{random.randint(1, 450):04d}",
            random.randint(1, 50),
            random.randint(1, 10),
            random_date(start, end),
        ])
    write_csv("orders.csv", ["order_id", "customer_id", "product_id", "quantity", "order_date"], rows)


def generate_survey_responses():
    """~2000 survey responses for pivot/aggregation demo."""
    questions = [
        "Product Quality", "Customer Service", "Value for Money",
        "Ease of Use", "Delivery Speed", "Overall Satisfaction",
    ]
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)

    rows = []
    for i in range(1, 2001):
        rows.append([
            i,
            f"R{random.randint(1, 300):04d}",
            random.choice(questions),
            random.randint(1, 5),
            random_date(start, end),
        ])
    write_csv("survey_responses.csv", ["response_id", "respondent_id", "question", "rating", "date"], rows)


def generate_page_views():
    """~5000 page view events for web analytics funnel demo."""
    pages = ["/home", "/products", "/product-detail", "/cart", "/checkout", "/confirmation",
             "/about", "/contact", "/blog", "/faq"]
    devices = ["desktop", "mobile", "tablet"]
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31)

    rows = []
    for i in range(1, 5001):
        rows.append([
            i,
            f"U{random.randint(1, 500):04d}",
            random.choice(pages),
            random.choice(devices),
            random_date(start, end),
        ])
    write_csv("page_views.csv", ["event_id", "user_id", "page", "device", "timestamp"], rows)


def generate_support_tickets():
    """~500 support tickets for customer 360 demo."""
    categories = ["Billing", "Technical", "Shipping", "Returns", "General", "Account"]
    priorities = ["Low", "Medium", "High", "Critical"]
    start = datetime(2023, 1, 1)
    end = datetime(2024, 12, 31)

    rows = []
    for i in range(1, 501):
        rows.append([
            i,
            f"C{random.randint(1, 450):04d}",
            random.choice(categories),
            random.choice(priorities),
            random_date(start, end),
        ])
    write_csv("support_tickets.csv", ["ticket_id", "customer_id", "category", "priority", "created_date"], rows)


def generate_fuzzy_match_data():
    """Two product lists with intentional name variations for fuzzy matching."""
    internal_products = [
        ("Apple MacBook Pro 16-inch", "MBP16-2024"),
        ("Samsung Galaxy S24 Ultra", "SGS24U"),
        ("Sony WH-1000XM5 Headphones", "SONYWH1000XM5"),
        ("Dell UltraSharp 27 Monitor", "DELLUS27"),
        ("Logitech MX Master 3S", "LOGMXM3S"),
        ("Apple iPad Pro 12.9", "IPADP129"),
        ("Bose QuietComfort 45", "BOSEQC45"),
        ("Microsoft Surface Pro 9", "MSSP9"),
        ("Google Pixel 8 Pro", "GPX8P"),
        ("HP Spectre x360 16", "HPSX360-16"),
        ("Lenovo ThinkPad X1 Carbon", "LENTPX1C"),
        ("Razer Blade 16", "RZRB16"),
        ("Corsair K100 RGB Keyboard", "CRSK100"),
        ("LG C3 65-inch OLED TV", "LGC365"),
        ("Nintendo Switch OLED", "NSWOLED"),
        ("Canon EOS R6 Mark II", "CEOSR6M2"),
        ("DJI Mavic 3 Pro", "DJIM3P"),
        ("Dyson V15 Detect", "DYSONV15"),
        ("Sonos Era 300", "SONOSE300"),
        ("Anker PowerCore 26800", "ANKPC26800"),
        ("SteelSeries Arctis Nova Pro", "SSANP"),
        ("Western Digital Black SN850X", "WDSN850X"),
        ("ASUS ROG Strix G16", "ASUSROGG16"),
        ("Jabra Elite 85t", "JABRE85T"),
        ("Fitbit Charge 6", "FBTC6"),
        ("Garmin Fenix 7X", "GRMFX7X"),
        ("Kindle Paperwhite 2024", "KINDLEPW24"),
        ("Roku Streaming Stick 4K", "ROKUSS4K"),
        ("TP-Link Deco XE75", "TPLXE75"),
        ("Samsung T7 Shield SSD", "SAMT7SH"),
        ("Crucial MX500 1TB SSD", "CRCMX500"),
        ("Seagate Expansion 4TB", "SGTE4TB"),
        ("AMD Ryzen 9 7950X", "AMDR97950X"),
        ("Intel Core i9-14900K", "INTCI914900K"),
        ("NVIDIA GeForce RTX 4090", "NVRTX4090"),
        ("Elgato Stream Deck MK.2", "ELGSD2"),
        ("Blue Yeti X Microphone", "BLUEYETIX"),
        ("Wacom Intuos Pro Medium", "WCMIPM"),
        ("Brother HL-L2350DW Printer", "BRHLL2350"),
        ("Epson EcoTank ET-2850", "EPSONET2850"),
        ("CalDigit TS4 Dock", "CLDGTS4"),
        ("Belkin Thunderbolt 4 Hub", "BLKTB4H"),
        ("APC Back-UPS Pro 1500", "APCBU1500"),
        ("Synology DS923+", "SYNDS923"),
        ("Ubiquiti Dream Machine Pro", "UBDMP"),
        ("Keychron Q1 Pro", "KEYCQ1P"),
        ("Audio-Technica ATH-M50x", "ATATHM50X"),
        ("Shure SM7B Microphone", "SHURESM7B"),
        ("Rode NT-USB Mini", "RODENTUSB"),
        ("Focusrite Scarlett 2i2", "FOCSC2I2"),
    ]

    rows_internal = []
    for i, (name, sku) in enumerate(internal_products, 1):
        rows_internal.append([i, name, sku])
    write_csv("internal_products.csv", ["id", "product_name", "sku"], rows_internal)

    # Create supplier list with intentional variations/typos
    variations = {
        "Apple MacBook Pro 16-inch": "MacBook Pro 16in Apple",
        "Samsung Galaxy S24 Ultra": "Galaxy S24 Ultra Samsung",
        "Sony WH-1000XM5 Headphones": "Sony WH1000XM5 Headphone",
        "Dell UltraSharp 27 Monitor": "Dell Ultra Sharp 27\" Monitor",
        "Logitech MX Master 3S": "Logitech MX Master 3 S",
        "Apple iPad Pro 12.9": "iPad Pro 12.9 inch Apple",
        "Bose QuietComfort 45": "Bose Quiet Comfort 45",
        "Microsoft Surface Pro 9": "MS Surface Pro 9",
        "Google Pixel 8 Pro": "Pixel 8 Pro Google",
        "HP Spectre x360 16": "HP Spectre x 360 16-inch",
        "Lenovo ThinkPad X1 Carbon": "ThinkPad X1 Carbon Lenovo",
        "Razer Blade 16": "Razer Blade 16 Gaming Laptop",
        "Corsair K100 RGB Keyboard": "Corsair K-100 RGB Mechanical Keyboard",
        "LG C3 65-inch OLED TV": "LG OLED C3 65 inch TV",
        "Nintendo Switch OLED": "Nintendo Switch OLED Model",
        "Canon EOS R6 Mark II": "Canon EOS R6 MK II Camera",
        "DJI Mavic 3 Pro": "DJI Mavic3 Pro Drone",
        "Dyson V15 Detect": "Dyson V15 Detect Vacuum",
        "Sonos Era 300": "Sonos Era300 Speaker",
        "Anker PowerCore 26800": "Anker Power Core 26800mAh",
        "SteelSeries Arctis Nova Pro": "Steel Series Arctis Nova Pro Headset",
        "Western Digital Black SN850X": "WD Black SN850X NVMe SSD",
        "ASUS ROG Strix G16": "Asus ROG Strix G16 Laptop",
        "Jabra Elite 85t": "Jabra Elite85t Earbuds",
        "Fitbit Charge 6": "Fitbit Charge6 Fitness Tracker",
        "Garmin Fenix 7X": "Garmin Fenix 7 X Solar",
        "Kindle Paperwhite 2024": "Amazon Kindle Paperwhite (2024)",
        "Roku Streaming Stick 4K": "Roku 4K Streaming Stick+",
        "TP-Link Deco XE75": "TPLink Deco XE-75 Mesh WiFi",
        "Samsung T7 Shield SSD": "Samsung Portable SSD T7 Shield",
        "Crucial MX500 1TB SSD": "Crucial MX-500 1TB SATA SSD",
        "Seagate Expansion 4TB": "Seagate Expansion Portable 4 TB",
        "AMD Ryzen 9 7950X": "AMD Ryzen9 7950X Processor",
        "Intel Core i9-14900K": "Intel Core i9 14900K CPU",
        "NVIDIA GeForce RTX 4090": "Nvidia RTX 4090 Graphics Card",
        "Elgato Stream Deck MK.2": "Elgato StreamDeck MK2",
        "Blue Yeti X Microphone": "Blue YetiX USB Microphone",
        "Wacom Intuos Pro Medium": "Wacom Intuos Pro Med Tablet",
        "Brother HL-L2350DW Printer": "Brother HLL2350DW Laser Printer",
        "Epson EcoTank ET-2850": "Epson Eco Tank ET2850 Printer",
        "CalDigit TS4 Dock": "CalDigit TS4 Thunderbolt Dock",
        "Belkin Thunderbolt 4 Hub": "Belkin TB4 USB-C Hub",
        "APC Back-UPS Pro 1500": "APC BackUPS Pro 1500VA",
        "Synology DS923+": "Synology DiskStation DS923 Plus",
        "Ubiquiti Dream Machine Pro": "Ubiquiti UDM Pro Router",
        "Keychron Q1 Pro": "Keychron Q1Pro Mechanical Keyboard",
        "Audio-Technica ATH-M50x": "Audio Technica ATH M50x Headphones",
        "Shure SM7B Microphone": "Shure SM-7B Dynamic Mic",
        "Rode NT-USB Mini": "Rode NTUSB Mini Microphone",
        "Focusrite Scarlett 2i2": "Focusrite Scarlett 2i2 Audio Interface",
    }

    suppliers = ["TechDistro Inc", "Global Electronics", "DataParts Supply", "MegaWholesale", "ProTech Direct"]
    rows_supplier = []
    for i, (original, variant) in enumerate(variations.items(), 1):
        rows_supplier.append([i, variant, random.choice(suppliers)])
    write_csv("supplier_products.csv", ["supplier_id", "product_name", "supplier"], rows_supplier)


if __name__ == "__main__":
    print("Generating template data files...")
    generate_sales_data()
    generate_customers()
    generate_employees()
    generate_products()
    generate_orders()
    generate_survey_responses()
    generate_page_views()
    generate_support_tickets()
    generate_fuzzy_match_data()
    print("Done!")
