

import csv
import io
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Generator, Tuple

# One record = one CSV row, e.g. {"Invoice": "489434", "Price": "6.95", ...}
Record = Dict[str, str]
RecordStream = Generator[Record, None, None]

REAL_DATA_FILE = (
    Path(__file__).resolve().parent.parent / "data" / "online_retail_II.csv"
)

# Small sample in the Online Retail II format. It includes bad rows on purpose:
# a cancelled order, a missing Customer ID, a negative quantity, a bad price.
SAMPLE_CSV = """Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country
489434,85048,CHRISTMAS GLASS BALL,12,2009-12-01 07:45:00,6.95,13085,United Kingdom
489434,79323P,PINK CHERRY LIGHTS,12,2009-12-01 07:45:00,6.75,13085,United Kingdom
489435,22350,CAT BOWL,12,2009-12-01 07:46:00,2.55,13085,United Kingdom
489436,48173C,DOOR HANGER,10,2009-12-01 09:06:00,1.45,13078,United Kingdom
C489449,22087,PAPER BUNTING,-12,2009-12-01 10:33:00,2.95,16321,Australia
489450,21329,WRITING SET,6,2009-12-01 10:40:00,1.65,,United Kingdom
489451,84029G,HOT WATER BOTTLE,3,2009-12-01 10:45:00,3.75,17850,United Kingdom
489452,22197,POPCORN HOLDER,-5,2009-12-01 11:00:00,0.85,12583,France
489453,21212,CAKE CASES,24,2009-12-01 11:05:00,0.55,12583,France
489454,20725,LUNCH BAG,10,2009-12-01 11:10:00,N/A,12583,Germany
"""


# ============================================
# 1. Abstractions (DIP & ISP)
# ============================================
class IDataReader(ABC):
    """Contract for anything that can stream records from a data source."""

    @abstractmethod
    def read_data(self) -> RecordStream:
        """Yield dataset records one at a time."""


class IDataProcessor(ABC):
    """Contract for anything that can turn a record stream into metrics."""

    @abstractmethod
    def process_records(self, records: RecordStream) -> Dict[str, float]:
        """Consume the records and return the calculated metrics."""


# ============================================
# 2. Concrete Implementations (SRP, OCP & LSP)
# ============================================
class CSVDataReader(IDataReader):
    """Streams a CSV file row by row so memory usage stays constant (SRP)."""

    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path

    def read_data(self) -> RecordStream:
        # utf-8-sig removes the hidden BOM character that Excel adds to CSV files
        with open(
            self._file_path, mode="r", encoding="utf-8-sig", errors="replace", newline=""
        ) as file:
            for row in csv.DictReader(file):
                yield row  # generator: one row in memory at a time


class InMemoryCSVReader(IDataReader):
    """Streams CSV text held in memory. Added later without touching any
    existing class, which demonstrates the Open/Closed Principle (OCP)."""

    def __init__(self, csv_text: str) -> None:
        self._csv_text = csv_text

    def read_data(self) -> RecordStream:
        for row in csv.DictReader(io.StringIO(self._csv_text.strip())):
            yield row


class SalesDataProcessor(IDataProcessor):
    """Applies the business rules to Online Retail II records (SRP)."""

    CANCELLATION_PREFIX = "C"

    # Different copies of the dataset use slightly different column names
    INVOICE_COLUMNS = ("Invoice", "InvoiceNo")
    QUANTITY_COLUMNS = ("Quantity",)
    PRICE_COLUMNS = ("Price", "UnitPrice")
    CUSTOMER_COLUMNS = ("Customer ID", "CustomerID")

    @staticmethod
    def _get_value(record: Record, column_names: Tuple[str, ...]) -> str:
        """Return the value of the first matching column, or an empty string."""
        for name in column_names:
            value = record.get(name)
            if value is not None:
                return value.strip()
        return ""

    def _is_invalid(self, record: Record) -> bool:
        """Cancelled orders (Invoice starts with 'C') or missing Customer ID."""
        invoice = self._get_value(record, self.INVOICE_COLUMNS)
        customer_id = self._get_value(record, self.CUSTOMER_COLUMNS)
        return invoice.startswith(self.CANCELLATION_PREFIX) or not customer_id

    def process_records(self, records: RecordStream) -> Dict[str, float]:
        total_revenue = 0.0
        total_items_sold = 0
        valid_records = 0
        flagged_records = 0

        for record in records:
            if self._is_invalid(record):
                flagged_records += 1
                continue

            try:
                price = float(self._get_value(record, self.PRICE_COLUMNS))
                quantity = int(float(self._get_value(record, self.QUANTITY_COLUMNS)))
            except ValueError:
                # Defensive coding: empty or non-numeric values
                flagged_records += 1
                continue

            if price < 0 or quantity < 0:
                flagged_records += 1
                continue

            total_revenue += price * quantity
            total_items_sold += quantity
            valid_records += 1

        return {
            "Total Revenue (GBP)": round(total_revenue, 2),
            "Total Items Sold": total_items_sold,
            "Valid Transactions": valid_records,
            "Flagged/Corrupted Entries": flagged_records,
        }


# ============================================
# 3. High-Level Orchestrator (DIP)
# ============================================
class SalesAnalyticsService:
    """Coordinates reading and processing. Depends only on abstractions (DIP)."""

    def __init__(self, reader: IDataReader, processor: IDataProcessor) -> None:
        self._reader = reader
        self._processor = processor

    def execute_pipeline(self) -> Dict[str, float]:
        """Run the read -> process pipeline and return the metrics."""
        return self._processor.process_records(self._reader.read_data())


# ============================================
# 4. Program Entry Point
# ============================================
def choose_reader() -> Tuple[IDataReader, str]:
    """Use the full dataset if it is present, otherwise the embedded sample."""
    if REAL_DATA_FILE.exists():
        return CSVDataReader(REAL_DATA_FILE), f"full dataset ({REAL_DATA_FILE.name})"
    return InMemoryCSVReader(SAMPLE_CSV), "embedded sample data"


def main() -> None:
    reader, source_name = choose_reader()

    # Dependency injection: concrete classes are created here and passed in
    service = SalesAnalyticsService(reader=reader, processor=SalesDataProcessor())
    results = service.execute_pipeline()

    print("Large Dataset Processing Service Initialized Successfully.")
    print(f"Data source: {source_name}")
    print("=" * 50)
    for metric, value in results.items():
        print(f"{metric:<30}: {value:,}")


if __name__ == "__main__":
    main()