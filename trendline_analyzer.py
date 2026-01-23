"""
AI-Powered Trendline Analyzer
Automatically selects the best trendline type that shows the most favorable positive trend
"""

import numpy as np
from typing import List, Dict, Tuple
from datetime import datetime


class TrendlineAnalyzer:
    """Analyzes time series data and selects the most favorable trendline"""

    @staticmethod
    def linear_trendline(x_values: List[float], y_values: List[float]) -> Tuple[List[float], float, str]:
        """
        Calculate linear trendline: y = mx + b
        Returns: (trendline_values, slope, equation)
        """
        if len(y_values) < 2:
            return y_values, 0, "Insufficient data"

        # Use numpy for linear regression
        coefficients = np.polyfit(x_values, y_values, 1)
        slope = coefficients[0]
        intercept = coefficients[1]

        # Calculate trendline values
        trendline = [slope * x + intercept for x in x_values]

        equation = f"y = {slope:.2f}x + {intercept:.2f}"
        return trendline, slope, equation

    @staticmethod
    def polynomial_trendline(x_values: List[float], y_values: List[float], degree: int = 2) -> Tuple[List[float], float, str]:
        """
        Calculate polynomial trendline
        Returns: (trendline_values, end_slope, equation)
        """
        if len(y_values) < degree + 1:
            return y_values, 0, "Insufficient data"

        # Fit polynomial
        coefficients = np.polyfit(x_values, y_values, degree)
        polynomial = np.poly1d(coefficients)

        # Calculate trendline values
        trendline = [polynomial(x) for x in x_values]

        # Calculate slope at the end (derivative)
        derivative = np.polyder(polynomial)
        end_slope = derivative(x_values[-1])

        equation = f"Polynomial (degree {degree})"
        return trendline, end_slope, equation

    @staticmethod
    def moving_average_trendline(y_values: List[float], window: int = 3) -> Tuple[List[float], float, str]:
        """
        Calculate moving average trendline
        Returns: (trendline_values, trend_direction, equation)
        """
        if len(y_values) < window:
            return y_values, 0, "Insufficient data"

        trendline = []
        for i in range(len(y_values)):
            if i < window - 1:
                # For early values, use what we have
                avg = sum(y_values[:i+1]) / (i + 1)
            else:
                # Moving average
                avg = sum(y_values[i-window+1:i+1]) / window
            trendline.append(avg)

        # Calculate trend (slope of last few points)
        if len(trendline) >= 2:
            slope = trendline[-1] - trendline[-2]
        else:
            slope = 0

        equation = f"{window}-period moving average"
        return trendline, slope, equation

    @staticmethod
    def exponential_smoothing(y_values: List[float], alpha: float = 0.3) -> Tuple[List[float], float, str]:
        """
        Calculate exponential smoothing trendline
        Returns: (trendline_values, trend, equation)
        """
        if len(y_values) < 2:
            return y_values, 0, "Insufficient data"

        trendline = [y_values[0]]

        for i in range(1, len(y_values)):
            smoothed = alpha * y_values[i] + (1 - alpha) * trendline[-1]
            trendline.append(smoothed)

        # Calculate trend
        slope = trendline[-1] - trendline[-2] if len(trendline) >= 2 else 0

        equation = f"Exponential smoothing (α={alpha})"
        return trendline, slope, equation

    @staticmethod
    def calculate_growth_score(original_values: List[float], trendline_values: List[float]) -> float:
        """
        Calculate a growth score based on how positive the trend is
        Higher score = more favorable trend
        """
        if len(trendline_values) < 2:
            return 0

        # Calculate overall growth rate
        start_value = trendline_values[0]
        end_value = trendline_values[-1]

        if start_value == 0:
            return 0

        growth_rate = (end_value - start_value) / abs(start_value)

        # Bonus for consistency (less volatility is better)
        # Calculate R² (how well the trendline fits)
        mean_original = sum(original_values) / len(original_values)
        ss_tot = sum((y - mean_original) ** 2 for y in original_values)
        ss_res = sum((original_values[i] - trendline_values[i]) ** 2 for i in range(len(original_values)))

        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        # Combine growth rate with fit quality
        # Prioritize growth but also consider fit
        score = growth_rate * 100 + r_squared * 10

        return score

    @classmethod
    def find_best_trendline(cls, y_values: List[float], labels: List[str] = None) -> Dict:
        """
        Test multiple trendline types and return the one with the most favorable trend

        Args:
            y_values: List of metric values
            labels: List of x-axis labels (dates/months)

        Returns:
            Dictionary with trendline data and metadata
        """
        if not y_values or len(y_values) < 2:
            return {
                'type': 'none',
                'values': y_values,
                'equation': 'Insufficient data',
                'score': 0
            }

        # Create numeric x values
        x_values = list(range(len(y_values)))

        # Test different trendline types
        candidates = []

        # 1. Linear trendline
        try:
            linear_trend, linear_slope, linear_eq = cls.linear_trendline(x_values, y_values)
            linear_score = cls.calculate_growth_score(y_values, linear_trend)
            candidates.append({
                'type': 'linear',
                'values': linear_trend,
                'equation': linear_eq,
                'slope': linear_slope,
                'score': linear_score
            })
        except:
            pass

        # 2. Polynomial trendline (degree 2)
        try:
            poly_trend, poly_slope, poly_eq = cls.polynomial_trendline(x_values, y_values, degree=2)
            poly_score = cls.calculate_growth_score(y_values, poly_trend)
            candidates.append({
                'type': 'polynomial',
                'values': poly_trend,
                'equation': poly_eq,
                'slope': poly_slope,
                'score': poly_score
            })
        except:
            pass

        # 3. 3-month moving average
        try:
            ma3_trend, ma3_slope, ma3_eq = cls.moving_average_trendline(y_values, window=3)
            ma3_score = cls.calculate_growth_score(y_values, ma3_trend)
            candidates.append({
                'type': 'moving_average_3',
                'values': ma3_trend,
                'equation': ma3_eq,
                'slope': ma3_slope,
                'score': ma3_score
            })
        except:
            pass

        # 4. 6-month moving average (if enough data)
        if len(y_values) >= 6:
            try:
                ma6_trend, ma6_slope, ma6_eq = cls.moving_average_trendline(y_values, window=6)
                ma6_score = cls.calculate_growth_score(y_values, ma6_trend)
                candidates.append({
                    'type': 'moving_average_6',
                    'values': ma6_trend,
                    'equation': ma6_eq,
                    'slope': ma6_slope,
                    'score': ma6_score
                })
            except:
                pass

        # 5. Exponential smoothing
        try:
            exp_trend, exp_slope, exp_eq = cls.exponential_smoothing(y_values, alpha=0.3)
            exp_score = cls.calculate_growth_score(y_values, exp_trend)
            candidates.append({
                'type': 'exponential_smoothing',
                'values': exp_trend,
                'equation': exp_eq,
                'slope': exp_slope,
                'score': exp_score
            })
        except:
            pass

        # Select the best trendline (highest score)
        if not candidates:
            return {
                'type': 'none',
                'values': y_values,
                'equation': 'Unable to calculate',
                'score': 0
            }

        best_trendline = max(candidates, key=lambda x: x['score'])

        # Add labels if provided
        if labels:
            best_trendline['labels'] = labels

        return best_trendline

    @classmethod
    def analyze_metric_history(cls, history: List[Dict]) -> Dict:
        """
        Analyze historical metric data and return the best trendline

        Args:
            history: List of historical data points from HistoricalMetric.get_history()
                    Each item should have 'kpi_value' and 'month_label'

        Returns:
            Dictionary with trendline analysis
        """
        if not history:
            return {
                'type': 'none',
                'values': [],
                'equation': 'No data',
                'score': 0
            }

        # Extract values and labels
        y_values = [point.get('kpi_value', 0) for point in history]
        labels = [point.get('month_label', '') for point in history]

        # Find best trendline
        result = cls.find_best_trendline(y_values, labels)

        # Add original data for comparison
        result['original_values'] = y_values
        result['original_labels'] = labels

        return result


def test_trendline_analyzer():
    """Test the trendline analyzer with sample data"""

    # Test case 1: Steady growth
    print("Test 1: Steady Growth")
    values1 = [100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320]
    result1 = TrendlineAnalyzer.find_best_trendline(values1)
    print(f"  Best type: {result1['type']}")
    print(f"  Equation: {result1['equation']}")
    print(f"  Score: {result1['score']:.2f}")

    # Test case 2: Volatile but upward trend
    print("\nTest 2: Volatile but Upward")
    values2 = [100, 90, 130, 110, 150, 120, 170, 140, 190, 160, 210, 180]
    result2 = TrendlineAnalyzer.find_best_trendline(values2)
    print(f"  Best type: {result2['type']}")
    print(f"  Equation: {result2['equation']}")
    print(f"  Score: {result2['score']:.2f}")

    # Test case 3: Accelerating growth
    print("\nTest 3: Accelerating Growth")
    values3 = [100, 105, 115, 130, 150, 175, 205, 240, 280, 325, 375, 430]
    result3 = TrendlineAnalyzer.find_best_trendline(values3)
    print(f"  Best type: {result3['type']}")
    print(f"  Equation: {result3['equation']}")
    print(f"  Score: {result3['score']:.2f}")


if __name__ == '__main__':
    test_trendline_analyzer()
