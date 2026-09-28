import pytest
from task3_main import (
    InMemoryCSVReader,
    SalesAnalyticsService,
    SalesDataProcessor,
)


@pytest.fixture
def sample_csv_data():
    """Fixture providing valid, cancelled, and malformed CSV data."""
    return """Invoice,StockCode,Description,Quantity,InvoiceDate,Price,Customer ID,Country
489434,85048,GLASS BALL,10,2009-12-01 07:45:00,5.00,13085,United Kingdom
C489449,22087,CANCELLED ITEM,-2,2009-12-01 10:33:00,2.50,16321,Australia
489450,21329,NO CUSTOMER ID,6,2009-12-01 10:40:00,1.65,,United Kingdom
489451,84029G,BAD PRICE,3,2009-12-01 10:45:00,INVALID,17850,United Kingdom
"""


def test_in_memory_csv_reader(sample_csv_data):
    """Test standard streaming record parsing from in-memory string."""
    reader = InMemoryCSVReader(sample_csv_data)
    records = list(reader.read_data())
    assert len(records) == 4
    assert records[0]["Invoice"] == "489434"


def test_sales_data_processor_valid_and_flagged(sample_csv_data):
    """Test business logic filtration: ignores cancelled, missing ID, and corrupted prices."""
    reader = InMemoryCSVReader(sample_csv_data)
    processor = SalesDataProcessor()

    metrics = processor.process_records(reader.read_data())

    # Only row 1 is valid (10 * 5.00 = 50.00)
    assert metrics["Total Revenue (GBP)"] == 50.00
    assert metrics["Total Items Sold"] == 10
    assert metrics["Valid Transactions"] == 1
    assert metrics["Flagged/Corrupted Entries"] == 3


def test_sales_analytics_service_orchestration(sample_csv_data):
    """Test end-to-end service execution adhering to Dependency Inversion."""
    reader = InMemoryCSVReader(sample_csv_data)
    processor = SalesDataProcessor()
    service = SalesAnalyticsService(reader=reader, processor=processor)

    results = service.execute_pipeline()

    assert "Total Revenue (GBP)" in results
    assert results["Valid Transactions"] == 1