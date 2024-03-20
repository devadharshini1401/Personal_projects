import sqlite3
from datetime import date, timedelta

def create_date_dimension_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS date_dimension (
        DATE_KEY INTEGER PRIMARY KEY,
        calendar_DATE DATE,
        DAY_NUMBER_OF_WEEK INTEGER,
        DAY_NUMBER_OF_MONTH INTEGER,
        DAY_NUMBER_OF_YEAR INTEGER,
        WEEK_NO_OF_MONTH INTEGER,
        WEEK_NO_YEAR INTEGER,
        MONTH_NO INTEGER,
        MONTH_SHORT_NAME VARCHAR(40),
        MONTH_FULL_NAME VARCHAR(40),
        QTR_NO INTEGER,
        CALENDER_YEAR INTEGER,
        FISCAL_MONTH INTEGER,
        FISCAL_WEEK INTEGER,
        FISCAL_QTR INTEGER,
        FISCAL_YEAR VARCHAR(20),
        WEEK_DAY_FLAG VARCHAR(20),
        HOLIDAY_FLAG CHAR(1)
    )
    """)

def populate_date_dimension(conn, year):
    cursor = conn.cursor()
    
    # Check if records for the given year are already populated
    cursor.execute("SELECT COUNT(*) FROM date_dimension WHERE CALENDER_YEAR = ?", (year,))
    count = cursor.fetchone()[0]
    if count > 0:
        print(f"Records for the year {year} are already populated.")
        return
    
    # Populate date dimension table
    start_date = date(year, 1, 1)
    end_date = date(year, 12, 31)
    delta = timedelta(days=1)
    while start_date <= end_date:
        cursor.execute("""
        INSERT INTO date_dimension (
            calendar_DATE, DAY_NUMBER_OF_WEEK, DAY_NUMBER_OF_MONTH, DAY_NUMBER_OF_YEAR, 
            WEEK_NO_OF_MONTH, WEEK_NO_YEAR, MONTH_NO, MONTH_SHORT_NAME, MONTH_FULL_NAME, 
            QTR_NO, CALENDER_YEAR, FISCAL_MONTH, FISCAL_WEEK, FISCAL_QTR, FISCAL_YEAR, 
            WEEK_DAY_FLAG, HOLIDAY_FLAG
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            start_date, start_date.weekday() + 1, start_date.day, start_date.timetuple().tm_yday,
            (start_date.day - 1) // 7 + 1, start_date.isocalendar()[1], start_date.month, 
            start_date.strftime("%b"), start_date.strftime("%B"), (start_date.month - 1) // 3 + 1, 
            start_date.year, (start_date.year + 1) if start_date.month >= 4 else start_date.year, 
            (start_date.isocalendar()[1] - 1) // 4 + 1, start_date.isocalendar()[1], 
            (start_date.month - 1) // 4 + 1, f"{start_date.year}-{(start_date.year + 1)}", 
            'WEEKDAY' if start_date.weekday() < 5 else 'WEEKEND', 'N'
        ))
        start_date += delta
    
    conn.commit()

def main():
    year = int(input("Enter the year (YYYY): "))
    
    conn = sqlite3.connect('date_dimension.db')
    
    create_date_dimension_table(conn)
    populate_date_dimension(conn, year)
    
    conn.close()

if __name__ == "__main__":
    main()
