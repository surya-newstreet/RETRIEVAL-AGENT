"""
Generic time window parser with smart year inference.

Parses natural language time expressions and converts to SQL-compatible date ranges.
Year inference uses runtime MAX(date_col) queries to select most relevant year.
"""
from dataclasses import dataclass
from typing import Optional, Tuple
from datetime import datetime, date, timedelta
import re
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as dateutil_parse
from db.connection import db_manager
from observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TimeWindow:
    """Parsed time window with SQL-ready dates."""
    start_date: date
    end_date: date
    description: str
    inferred_year: Optional[int] = None


class TimeParser:
    """Parse time windows from natural language with smart year inference."""
    
    def __init__(self):
        self._max_date_cache = {}  # (table, column) -> date
        self._cache_expiry = {}    # (table, column) -> datetime
        self._cache_ttl_minutes = 15
        
        # Month name mapping
        self.month_names = {
            'january': 1, 'jan': 1,
            'february': 2, 'feb': 2,
            'march': 3, 'mar': 3,
            'april': 4, 'apr': 4,
            'may': 5,
            'june': 6, 'jun': 6,
            'july': 7, 'jul': 7,
            'august': 8, 'aug': 8,
            'september': 9, 'sep': 9, 'sept': 9,
            'october': 10, 'oct': 10,
            'november': 11, 'nov': 11,
            'december': 12, 'dec': 12
        }
    
    async def parse_time_window(
        self,
        text: str,
        table: str,
        date_column: str,
        default_timezone: str = "UTC"
    ) -> Optional[TimeWindow]:
        """
        Parse time window from text.
        
        Args:
            text: Natural language time expression
            table: Schema-qualified table name (for year inference)
            date_column: Date column name (for year inference)
            default_timezone: Timezone for date calculations
        
        Returns:
            TimeWindow or None if parsing fails
        """
        text_lower = text.lower().strip()
        
        # Pattern: "last N days/weeks/months"
        last_n_match = re.search(r'last\s+(\d+)\s+(day|week|month)s?', text_lower)
        if last_n_match:
            n = int(last_n_match.group(1))
            unit = last_n_match.group(2)
            return self._parse_last_n(n, unit)
        
        # Pattern: "in {month}" or just "{month}"
        single_month = self._extract_month_name(text_lower)
        if single_month:
            month_num = self.month_names[single_month]
            year = await self._infer_year_for_month(month_num, table, date_column)
            return self._month_to_window(month_num, year)
        
        # Pattern: "{month1} to {month2}"
        month_range_match = re.search(
            r'(' + '|'.join(self.month_names.keys()) + r')\s+to\s+(' + '|'.join(self.month_names.keys()) + r')',
            text_lower
        )
        if month_range_match:
            start_month_name = month_range_match.group(1)
            end_month_name = month_range_match.group(2)
            start_month = self.month_names[start_month_name]
            end_month = self.month_names[end_month_name]
            year = await self._infer_year_for_month(start_month, table, date_column)
            return self._month_range_to_window(start_month, end_month, year)
        
        # Pattern: explicit dates "2025-12-01 to 2025-12-31" or "Dec 1 2025 to Dec 31 2025"
        try:
            # Try to find two dates in the text
            date_pattern = r'\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4}'
            dates_found = re.findall(date_pattern, text)
            if len(dates_found) >= 2:
                start = dateutil_parse(dates_found[0]).date()
                end = dateutil_parse(dates_found[1]).date()
                return TimeWindow(
                    start_date=start,
                    end_date=end,
                    description=f"Explicit range: {start} to {end}"
                )
        except Exception as e:
            logger.debug("explicit_date_parsing_failed", error=str(e))
        
        # No match
        logger.warning("time_window_parse_failed", text=text)
        return None
    
    def _parse_last_n(self, n: int, unit: str) -> TimeWindow:
        """Parse 'last N days/weeks/months'."""
        today = date.today()
        
        if unit == 'day':
            start_date = today - timedelta(days=n)
            end_date = today
        elif unit == 'week':
            start_date = today - timedelta(weeks=n)
            end_date = today
        elif unit == 'month':
            start_date = today - relativedelta(months=n)
            end_date = today
        else:
            raise ValueError(f"Unknown unit: {unit}")
        
        return TimeWindow(
            start_date=start_date,
            end_date=end_date,
            description=f"Last {n} {unit}(s)"
        )
    
    def _extract_month_name(self, text: str) -> Optional[str]:
        """Extract a single month name from text."""
        for month_name in self.month_names.keys():
            # Match whole word
            pattern = r'\b' + month_name + r'\b'
            if re.search(pattern, text):
                return month_name
        return None
    
    async def _infer_year_for_month(self, month: int, table: str, date_column: str) -> int:
        """
        Infer most relevant year for a given month.
        
        Strategy:
        1. Get MAX(date_col) from table (cached)
        2. If MAX year matches current year or prior year, use that
        3. If month is in future relative to MAX, use previous year
        4. Otherwise use current year
        """
        max_date = await self._get_max_date_cached(table, date_column)
        current_year = date.today().year
        
        if max_date is None:
            # No data, default to current year
            logger.warning("no_max_date_found", table=table, column=date_column)
            return current_year
        
        max_year = max_date.year
        max_month = max_date.month
        
        # If data exists in current year or recent past, prefer max year
        if max_year == current_year:
            # If the month is after the max month in current year, use previous year
            if month > max_month:
                inferred_year = current_year - 1
            else:
                inferred_year = current_year
        elif max_year == current_year - 1:
            # Data is from last year
            inferred_year = max_year
        else:
            # Data is older, use max year
            inferred_year = max_year
        
        logger.info(
            "year_inferred",
            month=month,
            table=table,
            max_date=str(max_date),
            inferred_year=inferred_year
        )
        
        return inferred_year
    
    async def _get_max_date_cached(self, table: str, date_column: str) -> Optional[date]:
        """Get MAX(date_col) with TTL caching."""
        cache_key = (table, date_column)
        now = datetime.now()
        
        # Check cache
        if cache_key in self._max_date_cache:
            expiry = self._cache_expiry.get(cache_key)
            if expiry and now < expiry:
                return self._max_date_cache[cache_key]
        
        # Query database
        try:
            query = f"SELECT MAX({date_column}) AS max_date FROM {table}"
            async with db_manager.acquire_metadata_connection() as conn:
                row = await conn.fetchrow(query)
                max_date = row['max_date'] if row and row['max_date'] else None
            
            # Cache result
            self._max_date_cache[cache_key] = max_date
            self._cache_expiry[cache_key] = now + timedelta(minutes=self._cache_ttl_minutes)
            
            logger.info("max_date_cached", table=table, column=date_column, max_date=str(max_date))
            return max_date
        
        except Exception as e:
            logger.error("max_date_query_failed", table=table, column=date_column, error=str(e))
            return None
    
    def _month_to_window(self, month: int, year: int) -> TimeWindow:
        """Convert single month to date range."""
        start_date = date(year, month, 1)
        
        # End date is first day of next month
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)
        
        # Subtract one day to get last day of month (for inclusive range)
        # Actually, for SQL we want end_date to be exclusive, so keep as first of next month
        
        return TimeWindow(
            start_date=start_date,
            end_date=end_date,
            description=f"{year}-{month:02d}",
            inferred_year=year
        )
    
    def _month_range_to_window(self, start_month: int, end_month: int, year: int) -> TimeWindow:
        """Convert month range to date range."""
        start_date = date(year, start_month, 1)
        
        # End date is first day after end_month
        if end_month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, end_month + 1, 1)
        
        return TimeWindow(
            start_date=start_date,
            end_date=end_date,
            description=f"{year}-{start_month:02d} to {year}-{end_month:02d}",
            inferred_year=year
        )
    
    def invalidate_cache(self, table: Optional[str] = None, date_column: Optional[str] = None):
        """Invalidate cache for a specific table/column or all."""
        if table and date_column:
            cache_key = (table, date_column)
            self._max_date_cache.pop(cache_key, None)
            self._cache_expiry.pop(cache_key, None)
        else:
            # Clear all
            self._max_date_cache.clear()
            self._cache_expiry.clear()
        
        logger.info("cache_invalidated", table=table, column=date_column)


# Global instance
time_parser = TimeParser()
