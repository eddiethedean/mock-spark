"""
Time Series Analysis for Mock Spark 1.0.0

Provides time series analysis capabilities using DuckDB.
"""

import duckdb
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
from .analytics_engine import AnalyticsEngine


class TimeSeriesAnalysis:
    """Time series analysis using DuckDB's analytical capabilities."""
    
    def __init__(self, analytics_engine: AnalyticsEngine):
        """Initialize with analytics engine.
        
        Args:
            analytics_engine: AnalyticsEngine instance
        """
        self.engine = analytics_engine
    
    def time_series_decomposition(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        period: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Decompose time series into trend, seasonal, and residual components.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values to analyze
            period: Seasonal period (auto-detected if None)
            
        Returns:
            Decomposed time series components
        """
        # First, get basic statistics to determine period if not provided
        if period is None:
            period = self._detect_seasonality(table_name, time_column, value_column)
        
        query = f"""
            WITH time_series AS (
                SELECT 
                    {time_column},
                    {value_column},
                    ROW_NUMBER() OVER (ORDER BY {time_column}) as row_num
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
                ORDER BY {time_column}
            ),
            trend AS (
                SELECT 
                    {time_column},
                    {value_column},
                    row_num,
                    AVG({value_column}) OVER (
                        ORDER BY row_num 
                        ROWS BETWEEN {period} PRECEDING AND {period} FOLLOWING
                    ) as trend
                FROM time_series
            ),
            seasonal AS (
                SELECT 
                    {time_column},
                    {value_column},
                    row_num,
                    trend,
                    {value_column} - trend as detrended,
                    AVG({value_column} - trend) OVER (
                        PARTITION BY (row_num - 1) % {period}
                    ) as seasonal
                FROM trend
            )
            SELECT 
                {time_column},
                {value_column} as original,
                trend,
                seasonal,
                {value_column} - trend - seasonal as residual
            FROM seasonal
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def _detect_seasonality(
        self,
        table_name: str,
        time_column: str,
        value_column: str
    ) -> int:
        """Detect seasonal period using autocorrelation.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            
        Returns:
            Detected seasonal period
        """
        query = f"""
            WITH lagged AS (
                SELECT 
                    {value_column},
                    LAG({value_column}, 1) OVER (ORDER BY {time_column}) as lag1,
                    LAG({value_column}, 2) OVER (ORDER BY {time_column}) as lag2,
                    LAG({value_column}, 7) OVER (ORDER BY {time_column}) as lag7,
                    LAG({value_column}, 30) OVER (ORDER BY {time_column}) as lag30,
                    LAG({value_column}, 365) OVER (ORDER BY {time_column}) as lag365
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
                ORDER BY {time_column}
            )
            SELECT 
                CORR({value_column}, lag1) as corr1,
                CORR({value_column}, lag2) as corr2,
                CORR({value_column}, lag7) as corr7,
                CORR({value_column}, lag30) as corr30,
                CORR({value_column}, lag365) as corr365
            FROM lagged
        """
        
        result = self.engine.execute_analytical_query(query)[0]
        
        # Find the lag with highest correlation
        correlations = {
            1: result['corr1'],
            2: result['corr2'],
            7: result['corr7'],
            30: result['corr30'],
            365: result['corr365']
        }
        
        return max(correlations.items(), key=lambda x: abs(x[1]) if x[1] else 0)[0]
    
    def moving_averages(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        windows: List[int] = [5, 10, 20, 50]
    ) -> List[Dict[str, Any]]:
        """Calculate multiple moving averages.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            windows: List of window sizes for moving averages
            
        Returns:
            Time series with moving averages
        """
        ma_columns = []
        for window in windows:
            ma_columns.append(f"AVG({value_column}) OVER (ORDER BY {time_column} ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW) as ma{window}")
        
        query = f"""
            SELECT 
                {time_column},
                {value_column} as original,
                {', '.join(ma_columns)}
            FROM {table_name}
            WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def exponential_smoothing(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        alpha: float = 0.3
    ) -> List[Dict[str, Any]]:
        """Apply exponential smoothing.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            alpha: Smoothing parameter (0 < alpha < 1)
            
        Returns:
            Time series with exponential smoothing
        """
        query = f"""
            WITH smoothed AS (
                SELECT 
                    {time_column},
                    {value_column},
                    ROW_NUMBER() OVER (ORDER BY {time_column}) as row_num,
                    FIRST_VALUE({value_column}) OVER (ORDER BY {time_column}) as initial_value
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
                ORDER BY {time_column}
            )
            SELECT 
                {time_column},
                {value_column} as original,
                CASE 
                    WHEN row_num = 1 THEN {value_column}
                    ELSE LAG({value_column}) OVER (ORDER BY {time_column}) * (1 - {alpha}) + {value_column} * {alpha}
                END as exponential_smooth
            FROM smoothed
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def seasonal_adjustment(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        period: int = 12
    ) -> List[Dict[str, Any]]:
        """Apply seasonal adjustment to time series.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            period: Seasonal period
            
        Returns:
            Seasonally adjusted time series
        """
        query = f"""
            WITH seasonal_index AS (
                SELECT 
                    {time_column},
                    {value_column},
                    EXTRACT(MONTH FROM {time_column}) as month,
                    AVG({value_column}) OVER () as overall_avg,
                    AVG({value_column}) OVER (PARTITION BY EXTRACT(MONTH FROM {time_column})) as monthly_avg
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
            )
            SELECT 
                {time_column},
                {value_column} as original,
                month,
                monthly_avg,
                monthly_avg / overall_avg as seasonal_index,
                {value_column} / (monthly_avg / overall_avg) as seasonally_adjusted
            FROM seasonal_index
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def time_series_forecasting(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        forecast_periods: int = 12,
        method: str = "linear"
    ) -> List[Dict[str, Any]]:
        """Simple time series forecasting.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            forecast_periods: Number of periods to forecast
            method: Forecasting method ('linear', 'exponential')
            
        Returns:
            Historical data with forecasts
        """
        if method == "linear":
            return self._linear_forecast(table_name, time_column, value_column, forecast_periods)
        elif method == "exponential":
            return self._exponential_forecast(table_name, time_column, value_column, forecast_periods)
        else:
            raise ValueError(f"Unsupported forecasting method: {method}")
    
    def _linear_forecast(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        forecast_periods: int
    ) -> List[Dict[str, Any]]:
        """Linear trend forecasting."""
        query = f"""
            WITH trend_analysis AS (
                SELECT 
                    {time_column},
                    {value_column},
                    ROW_NUMBER() OVER (ORDER BY {time_column}) as row_num,
                    AVG({value_column}) OVER () as mean_value,
                    AVG(ROW_NUMBER() OVER (ORDER BY {time_column})) OVER () as mean_time
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
                ORDER BY {time_column}
            ),
            regression AS (
                SELECT 
                    SUM((row_num - mean_time) * ({value_column} - mean_value)) / 
                    SUM((row_num - mean_time) * (row_num - mean_time)) as slope,
                    mean_value - (SUM((row_num - mean_time) * ({value_column} - mean_value)) / 
                                SUM((row_num - mean_time) * (row_num - mean_time))) * mean_time as intercept
                FROM trend_analysis
            )
            SELECT 
                {time_column},
                {value_column} as actual,
                intercept + slope * row_num as forecast,
                'historical' as type
            FROM trend_analysis, regression
            UNION ALL
            SELECT 
                {time_column} + INTERVAL '{forecast_periods}' DAY as {time_column},
                NULL as actual,
                intercept + slope * (SELECT MAX(row_num) FROM trend_analysis) + slope * forecast_row as forecast,
                'forecast' as type
            FROM regression, (SELECT generate_series(1, {forecast_periods}) as forecast_row)
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def _exponential_forecast(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        forecast_periods: int
    ) -> List[Dict[str, Any]]:
        """Exponential trend forecasting."""
        query = f"""
            WITH log_values AS (
                SELECT 
                    {time_column},
                    {value_column},
                    LN({value_column}) as log_value,
                    ROW_NUMBER() OVER (ORDER BY {time_column}) as row_num
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL AND {value_column} > 0
                ORDER BY {time_column}
            ),
            regression AS (
                SELECT 
                    AVG(log_value) OVER () as mean_log,
                    AVG(row_num) OVER () as mean_time,
                    SUM((row_num - AVG(row_num) OVER ()) * (log_value - AVG(log_value) OVER ())) / 
                    SUM((row_num - AVG(row_num) OVER ()) * (row_num - AVG(row_num) OVER ())) as slope,
                    AVG(log_value) OVER () - 
                    (SUM((row_num - AVG(row_num) OVER ()) * (log_value - AVG(log_value) OVER ())) / 
                     SUM((row_num - AVG(row_num) OVER ()) * (row_num - AVG(row_num) OVER ()))) * AVG(row_num) OVER () as intercept
                FROM log_values
            )
            SELECT 
                {time_column},
                {value_column} as actual,
                EXP(intercept + slope * row_num) as forecast,
                'historical' as type
            FROM log_values, regression
            UNION ALL
            SELECT 
                {time_column} + INTERVAL '{forecast_periods}' DAY as {time_column},
                NULL as actual,
                EXP(intercept + slope * ((SELECT MAX(row_num) FROM log_values) + forecast_row)) as forecast,
                'forecast' as type
            FROM regression, (SELECT generate_series(1, {forecast_periods}) as forecast_row)
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
    
    def time_series_anomaly_detection(
        self,
        table_name: str,
        time_column: str,
        value_column: str,
        threshold: float = 2.0
    ) -> List[Dict[str, Any]]:
        """Detect anomalies in time series using statistical methods.
        
        Args:
            table_name: Name of the table
            time_column: Column containing time data
            value_column: Column containing values
            threshold: Z-score threshold for anomaly detection
            
        Returns:
            Time series with anomaly flags
        """
        query = f"""
            WITH stats AS (
                SELECT 
                    AVG({value_column}) as mean_val,
                    STDDEV({value_column}) as std_val
                FROM {table_name}
                WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
            )
            SELECT 
                {time_column},
                {value_column} as original,
                ({value_column} - mean_val) / std_val as z_score,
                CASE 
                    WHEN ABS(({value_column} - mean_val) / std_val) > {threshold} THEN 1 
                    ELSE 0 
                END as is_anomaly,
                CASE 
                    WHEN ABS(({value_column} - mean_val) / std_val) > {threshold} THEN 'ANOMALY' 
                    ELSE 'NORMAL' 
                END as status
            FROM {table_name}, stats
            WHERE {time_column} IS NOT NULL AND {value_column} IS NOT NULL
            ORDER BY {time_column}
        """
        
        return self.engine.execute_analytical_query(query)
