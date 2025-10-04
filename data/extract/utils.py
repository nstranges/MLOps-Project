import calendar
from datetime import date, datetime

def get_valid_date_ranges(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """
    Return a list of (start, end) date ranges covering all complete years 
    and the last partial year up to the last complete month between two dates.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    ranges = []
    current_year = start.year

    while current_year <= end.year:
        # Determine range start
        range_start = start if current_year == start.year else date(current_year, 1, 1)
        
        # Determine range end
        if current_year < end.year:
            range_end = date(current_year, 12, 31)
        else:
            # Last complete month before end_date
            last_complete_month = end.month - 1
            if end.day == calendar.monthrange(end.year, end.month)[1]:
                # If end date is already at the end of its month,
                # then that month is complete
                last_complete_month = end.month
            
            # If last_complete_month is 0, no valid range in this year
            if last_complete_month == 0:
                break
            
            last_day = calendar.monthrange(end.year, last_complete_month)[1]
            range_end = date(end.year, last_complete_month, last_day)
        
        if range_start <= range_end:
            ranges.append((
                range_start.strftime("%Y-%m-%d"),
                range_end.strftime("%Y-%m-%d")
            ))
        
        current_year += 1

    return ranges