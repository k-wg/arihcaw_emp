import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from binance.client import Client
from colorama import Back, Fore, Style, init
from tabulate import tabulate

# Initialize colorama for cross-platform colored output
init(autoreset=True)

# Configuration
UPDATE_INTERVAL = 1  # seconds - adjustable update interval
DISPLAY_ROWS = 15  # number of rows to display in summary table
SYMBOL = "SOLFDUSD"  # Change to your trading pair
timeout_que = 3  # Time to choose option 1 - 3 before the default 1 activates

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class PinescriptRSICalculator:
    """
    Exact Pinescript RSI implementation with Wilder's smoothing
    Matches TradingView's ta.rsi() calculation precisely
    """

    def __init__(self, length=14):
        self.length = length
        self.alpha = 1.0 / length  # Wilder's smoothing alpha

    def calculate_rsi(self, prices):
        """
        Calculate RSI using Wilder's smoothing method (matches TradingView exactly)

        Args:
            prices: Series of close prices

        Returns:
            Series of RSI values
        """
        if len(prices) < self.length + 1:
            return pd.Series([np.nan] * len(prices), index=prices.index)

        # Calculate price changes
        changes = prices.diff()

        # Separate gains and losses
        gains = changes.where(changes >= 0, 0.0)
        losses = -changes.where(changes < 0, 0.0)  # Make losses positive

        # Initialize RSI series
        rsi = pd.Series(index=prices.index, dtype=float)

        # Calculate first RSI value using SMA
        first_avg_gain = gains.iloc[1 : self.length + 1].mean()
        first_avg_loss = losses.iloc[1 : self.length + 1].mean()

        if first_avg_loss == 0:
            rsi.iloc[self.length] = 100.0
        elif first_avg_gain == 0:
            rsi.iloc[self.length] = 0.0
        else:
            rs = first_avg_gain / first_avg_loss
            rsi.iloc[self.length] = 100 - (100 / (1 + rs))

        # Calculate subsequent RSI values using Wilder's smoothing
        avg_gain = first_avg_gain
        avg_loss = first_avg_loss

        for i in range(self.length + 1, len(prices)):
            # Wilder's smoothing formula
            avg_gain = (avg_gain * (self.length - 1) + gains.iloc[i]) / self.length
            avg_loss = (avg_loss * (self.length - 1) + losses.iloc[i]) / self.length

            if avg_loss == 0:
                rsi.iloc[i] = 100.0
            elif avg_gain == 0:
                rsi.iloc[i] = 0.0
            else:
                rs = avg_gain / avg_loss
                rsi.iloc[i] = 100 - (100 / (1 + rs))

        return rsi


class MovingAverageCalculator:
    """Simple Moving Average calculator matching TradingView's ta.sma()"""

    @staticmethod
    def sma(data, length):
        """Calculate Simple Moving Average"""
        return data.rolling(window=length, min_periods=length).mean()


class FibonacciLevels:
    """Calculate Fibonacci retracement levels"""

    def __init__(self, length=1000):
        self.length = length

    def calculate_levels(self, prices):
        """
        Calculate Fibonacci levels over specified period
        Returns NaN for all levels if insufficient data available

        Args:
            prices: Series of prices to calculate Fibonacci levels from

        Returns:
            Dictionary of Fibonacci levels (returns NaN if insufficient data)
        """
        # Return NaN for all levels if insufficient data
        if len(prices) < self.length:
            return {
                "level_100": np.nan,
                "level_764": np.nan,
                "level_618": np.nan,
                "level_500": np.nan,
                "level_382": np.nan,
                "level_236": np.nan,
                "level_000": np.nan,
            }

        # Calculate with proper rolling window when sufficient data exists
        high = (
            prices.rolling(window=self.length, min_periods=self.length).max().iloc[-1]
        )
        low = prices.rolling(window=self.length, min_periods=self.length).min().iloc[-1]

        range_val = high - low

        levels = {
            "level_100": high,
            "level_764": high - 0.236 * range_val,  # 76.4% retracement
            "level_618": high - 0.382 * range_val,  # 61.8% retracement
            "level_500": high - 0.50 * range_val,  # 50% retracement
            "level_382": low + 0.382 * range_val,  # 38.2% retracement
            "level_236": low + 0.236 * range_val,  # 23.6% retracement
            "level_000": low,
        }

        return levels


def calculate_all_indicators(df, rsi_length=14, rsi_source="Close"):
    """
    Calculate all indicators from the Pinescript code

    Args:
        df: DataFrame with OHLCV data
        rsi_length: RSI calculation period
        rsi_source: Column to use for RSI calculation

    Returns:
        DataFrame with all calculated indicators
    """

    # Ensure we have required columns
    required_cols = ["Open", "High", "Low", "Close", "Volume"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Create a copy to avoid modifying original data
    result_df = df.copy()

    # Initialize calculators
    rsi_calc = PinescriptRSICalculator(length=rsi_length)

    # Calculate RSI
    logger.info(f"Calculating RSI with length {rsi_length}")
    result_df["rsi"] = rsi_calc.calculate_rsi(result_df[rsi_source])

    # Calculate RSI MA50
    logger.info("Calculating RSI MA50")
    result_df["rsi_ma50"] = MovingAverageCalculator.sma(result_df["rsi"], 50)

    # Calculate Moving Averages (from Pinescript)
    logger.info("Calculating moving averages")
    result_df["short002"] = MovingAverageCalculator.sma(result_df["Close"], 2)
    result_df["short007"] = MovingAverageCalculator.sma(result_df["Close"], 7)
    result_df["short21"] = MovingAverageCalculator.sma(
        result_df["Close"], 14
    )  # Note: Pinescript shows 14, not 21
    result_df["short50"] = MovingAverageCalculator.sma(result_df["Close"], 50)
    result_df["long100"] = MovingAverageCalculator.sma(result_df["Close"], 100)
    result_df["long200"] = MovingAverageCalculator.sma(result_df["Close"], 200)
    result_df["long350"] = MovingAverageCalculator.sma(result_df["Close"], 350)
    result_df["long500"] = MovingAverageCalculator.sma(result_df["Close"], 500)

    # Calculate Fibonacci Levels (dynamic for each row)
    logger.info("Calculating Fibonacci levels")
    fib_calc = FibonacciLevels(length=1000)

    # For simplicity, calculate Fibonacci levels for the entire dataset
    # In practice, you might want to calculate these dynamically for each bar
    fib_levels = fib_calc.calculate_levels(result_df["Close"])

    for level_name, level_value in fib_levels.items():
        result_df[level_name] = level_value

    logger.info("All indicators calculated successfully")
    return result_df


def save_indicators_to_csv(df, filename="indicators_output.csv"):
    """
    Save calculated indicators to CSV file

    Args:
        df: DataFrame with calculated indicators
        filename: Output filename
    """
    try:
        # Select only the calculated indicators and essential OHLC data
        columns_to_save = [
            "Open Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "daily_diff",
            "rsi",
            "rsi_ma50",
            "short002",
            "short007",
            "short21",
            "short50",
            "long100",
            "long200",
            "long350",
            "long500",
            "level_100",
            "level_764",
            "level_618",
            "level_500",
            "level_382",
            "level_236",
            "level_000",
        ]

        # Filter to only include columns that exist in the DataFrame
        available_columns = [col for col in columns_to_save if col in df.columns]

        # Save to CSV
        df[available_columns].to_csv(filename, index=False)
        logger.info(f"Indicators saved to {filename}")
        logger.info(f"Saved {len(df)} rows with {len(available_columns)} columns")

        # Log summary statistics for key indicators
        if "rsi" in df.columns:
            rsi_stats = df["rsi"].describe()
            logger.info(
                f"RSI Statistics: Min={rsi_stats['min']:.2f}, Max={rsi_stats['max']:.2f}, Mean={rsi_stats['mean']:.2f}"
            )

        return filename

    except Exception as e:
        logger.error(f"Error saving indicators to CSV: {e}")
        raise


def load_and_process_range_bars(input_filename="historic_df.csv"):
    """
    Load range bar data and calculate all Pinescript indicators

    Args:
        input_filename: Path to the range bar CSV file

    Returns:
        DataFrame with all calculated indicators
    """

    try:
        logger.info(f"Loading data from {input_filename}")

        # Load the range bar data
        df = pd.read_csv(input_filename)
        logger.info(f"Loaded {len(df)} bars from {input_filename}")

        # Fetch daily difference using price_monitor approach
        try:
            client = Client()
            utc_now = datetime.now(timezone.utc)
            target_time = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
            timestamp = int(target_time.timestamp() * 1000)

            klines = client.get_historical_klines(
                symbol=SYMBOL,
                interval=Client.KLINE_INTERVAL_1MINUTE,
                start_str=timestamp,
                end_str=timestamp + 60000,
                limit=1,
            )

            if klines:
                price_0300 = float(klines[0][4])
            else:
                price_0300 = None

            if price_0300 is None:
                df["daily_diff"] = "N/A"
                logger.warning("Could not retrieve 03:00 price.")
            else:
                ticker = client.get_symbol_ticker(symbol=SYMBOL)
                current_price = float(ticker["price"])
                diff = current_price - price_0300
                percent = (diff / price_0300) * 100
                if percent >= 0:
                    formatted_change = f"+{percent:.2f}%"
                else:
                    formatted_change = f"{percent:.2f}%"
                df["daily_diff"] = formatted_change
                logger.info(f"Daily diff fetched for {SYMBOL}: {formatted_change}")
        except Exception as e:
            logger.error(f"Error fetching daily diff: {e}")
            df["daily_diff"] = "N/A"

        # Enhanced datetime parsing to handle mixed formats
        if "Open Time" in df.columns:
            try:
                # First try with mixed format parsing
                df["Open Time"] = pd.to_datetime(df["Open Time"], format="mixed")
            except:
                try:
                    # Fallback to infer format
                    df["Open Time"] = pd.to_datetime(
                        df["Open Time"], infer_datetime_format=True
                    )
                except:
                    # Final fallback - let pandas auto-detect
                    df["Open Time"] = pd.to_datetime(df["Open Time"])

        if "Close Time" in df.columns:
            try:
                # First try with mixed format parsing
                df["Close Time"] = pd.to_datetime(df["Close Time"], format="mixed")
            except:
                try:
                    # Fallback to infer format
                    df["Close Time"] = pd.to_datetime(
                        df["Close Time"], infer_datetime_format=True
                    )
                except:
                    # Final fallback - let pandas auto-detect
                    df["Close Time"] = pd.to_datetime(df["Close Time"])

        # Verify we have enough data for calculations
        min_required_bars = 500  # Need at least 500 bars for long-term MAs
        if len(df) < min_required_bars:
            logger.warning(
                f"Only {len(df)} bars available. Some long-term indicators may not be calculated."
            )

        # Calculate all indicators
        logger.info("Starting indicator calculations...")
        df_with_indicators = calculate_all_indicators(df)

        logger.info("Indicator calculations completed")
        return df_with_indicators

    except Exception as e:
        logger.error(f"Error processing range bars: {e}")
        raise


def get_rsi_color_and_emoji(rsi_value):
    """
    Get color and emoji for RSI value based on common trading levels

    Args:
        rsi_value: RSI value (0-100)

    Returns:
        tuple: (color, emoji, description)
    """
    if pd.isna(rsi_value):
        return Fore.WHITE, "⚪", "N/A"
    elif rsi_value >= 75:
        return Fore.RED, "🔥", "Overbought+"
    elif rsi_value >= 65:
        return Fore.MAGENTA, "🔴", "Overbought"
    elif rsi_value >= 55:
        return Fore.YELLOW, "🟡", "Bullish"
    elif rsi_value >= 45:
        return Fore.GREEN, "🟢", "Neutral"
    elif rsi_value >= 35:
        return Fore.CYAN, "🔵", "Bearish"
    elif rsi_value >= 25:
        return Fore.BLUE, "🟦", "Oversold"
    else:
        return Fore.LIGHTBLUE_EX, "❄️", "Oversold+"


def get_price_trend_emoji(current, previous):
    """Get emoji for price movement"""
    if pd.isna(current) or pd.isna(previous):
        return "⚪"
    elif current > previous:
        return "📈"
    elif current < previous:
        return "📉"
    else:
        return "➡️"


def get_ma_position_color(price, ma_value):
    """Get color based on price position relative to MA"""
    if pd.isna(ma_value) or pd.isna(price):
        return Fore.WHITE
    elif price > ma_value:
        return Fore.GREEN  # Price above MA (bullish)
    else:
        return Fore.RED  # Price below MA (bearish)


def format_number(value, decimals=2):
    """Format number with appropriate decimal places"""
    if pd.isna(value):
        return "N/A".center(8)
    return f"{value:.{decimals}f}".center(8)


def display_simple_indicators_table(summary_df, rows=15, update_count=None):
    """
    Display a clean tabulated view of indicators for the last N rows

    Args:
        summary_df: DataFrame with calculated indicators
        rows: Number of rows to display
    """
    if summary_df is None or summary_df.empty:
        print(f"{Fore.RED}❌ No data available for indicators table{Style.RESET_ALL}")
        return

    # Get last N rows
    display_data = summary_df.tail(rows).copy()

    if display_data.empty:
        print(
            f"{Fore.YELLOW}⚠️  No data to display in indicators table{Style.RESET_ALL}"
        )
        return

    # Select columns for display - including ALL Fibonacci levels
    columns_to_show = [
        "Open Time",
        "Close",
        "daily_diff",
        "rsi",
        "rsi_ma50",
        "short002",
        "short007",
        "short50",
        "long200",
        "level_100",
        "level_764",
        "level_618",
        "level_500",
        "level_382",
        "level_236",
        "level_000",
    ]

    # Filter existing columns
    available_columns = [col for col in columns_to_show if col in display_data.columns]

    if not available_columns:
        print(
            f"{Fore.RED}❌ No required columns found for indicators table{Style.RESET_ALL}"
        )
        return

    # Prepare data for tabulation
    table_data = []

    for _, row in display_data.iterrows():
        row_values = []
        for col in available_columns:
            value = row.get(col, np.nan)

            if col == "Open Time":
                if pd.notna(value):
                    if isinstance(value, str):
                        formatted_value = value[11:19]  # Extract HH:MM:SS
                    else:
                        formatted_value = value.strftime("%H:%M:%S")
                else:
                    formatted_value = "N/A"
            elif col == "daily_diff":
                formatted_value = value if pd.notna(value) else "N/A"
            elif pd.notna(value) and isinstance(value, (int, float)):
                if col in ["Close", "short002", "short007", "short50", "long200"] + [
                    "level_100",
                    "level_764",
                    "level_618",
                    "level_500",
                    "level_382",
                    "level_236",
                    "level_000",
                ]:
                    formatted_value = f"{value:.4f}"
                else:
                    formatted_value = f"{value:.2f}"
            else:
                formatted_value = "N/A"

            row_values.append(formatted_value)

        table_data.append(row_values)

    # Create friendly headers for ALL Fibonacci levels
    header_mapping = {
        "Open Time": "Time",
        "Close": "Close",
        "daily_diff": "Daily Diff %",
        "rsi": "RSI",
        "rsi_ma50": "RSI MA50",
        "short002": "MA2",
        "short007": "MA7",
        "short50": "MA50",
        "long200": "MA200",
        "level_100": "Fib 100%",
        "level_764": "Fib 76.4%",
        "level_618": "Fib 61.8%",
        "level_500": "Fib 50%",
        "level_382": "Fib 38.2%",
        "level_236": "Fib 23.6%",
        "level_000": "Fib 0%",
    }

    headers = [header_mapping.get(col, col) for col in available_columns]

    # Display table
    print(
        f"\n{Back.MAGENTA}{Fore.WHITE}📊 COMPLETE INDICATORS TABLE - LAST {len(display_data)} BARS 📊{Style.RESET_ALL}"
    )

    indicators_table = tabulate(
        table_data,
        headers=headers,
        tablefmt="fancy_grid",
        numalign="right",
        stralign="center",
        floatfmt=".4f",
    )

    print(f"{Fore.CYAN}{indicators_table}{Style.RESET_ALL}")
    print(f"{Back.MAGENTA}{Fore.WHITE}{'='*160}{Style.RESET_ALL}")

    # Quick stats table
    latest_row = display_data.iloc[-1]

    def safe_format(val, decimals=4):
        return f"{val:.{decimals}f}" if pd.notna(val) else "N/A"

    stats_headers = [
        "Total Bars",
        "Update #",
        "MA 14",
        "MA 100",
        "MA 350",
        "MA 500",
    ]

    stats_row = [
        len(summary_df),
        f"{update_count}" if update_count is not None else "Single Run",
        safe_format(latest_row.get("short21", np.nan)),
        safe_format(latest_row.get("long100", np.nan)),
        safe_format(latest_row.get("long350", np.nan)),
        safe_format(latest_row.get("long500", np.nan)),
    ]

    stats_table = tabulate(
        [stats_row],
        headers=stats_headers,
        tablefmt="fancy_grid",
        numalign="right",
        stralign="center",
        floatfmt=".4f",
    )

    print(f"\n{Back.GREEN}{Fore.BLACK}📊 QUICK STATS 📊{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{stats_table}{Style.RESET_ALL}")
    print(f"{Back.GREEN}{Fore.BLACK}{'='*160}{Style.RESET_ALL}\n")


def print_update_header(update_count, total_bars):
    """Print colored update header"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = (
        f"\n{Back.MAGENTA}{Fore.WHITE}🔄 UPDATE #{update_count} - {current_time} "
        f"- Total Bars: {total_bars} 🔄{Style.RESET_ALL}"
    )
    print(header)


def validate_rsi_calculation(df, sample_size=10):
    """
    Validate RSI calculation by showing sample values and checking ranges

    Args:
        df: DataFrame with calculated RSI
        sample_size: Number of sample values to display
    """

    if "rsi" not in df.columns:
        logger.error("RSI column not found in DataFrame")
        return

    # Remove NaN values for validation
    rsi_values = df["rsi"].dropna()

    if len(rsi_values) == 0:
        logger.error("No valid RSI values found")
        return

    logger.info("=== RSI Validation ===")
    logger.info(f"Total RSI values calculated: {len(rsi_values)}")
    logger.info(f"RSI range: {rsi_values.min():.4f} to {rsi_values.max():.4f}")
    logger.info(f"RSI mean: {rsi_values.mean():.4f}")
    logger.info(f"RSI std: {rsi_values.std():.4f}")

    # Check if RSI is within expected range (0-100)
    if rsi_values.min() < 0 or rsi_values.max() > 100:
        logger.warning("RSI values outside expected range (0-100)!")
    else:
        logger.info("RSI values are within expected range (0-100)")

    # Show sample values
    logger.info(f"\nSample RSI values (last {sample_size}):")
    for i, (idx, rsi_val) in enumerate(rsi_values.tail(sample_size).items()):
        close_price = df.loc[idx, "Close"] if "Close" in df.columns else "N/A"
        logger.info(f"  Bar {idx}: Close={close_price}, RSI={rsi_val:.4f}")


def check_file_updated(filepath, last_modified_time):
    """
    Check if file has been updated since last check

    Args:
        filepath: Path to file to check
        last_modified_time: Last known modification time

    Returns:
        tuple: (is_updated, new_modification_time)
    """
    try:
        if not os.path.exists(filepath):
            return False, last_modified_time

        current_modified_time = os.path.getmtime(filepath)

        if last_modified_time is None or current_modified_time > last_modified_time:
            return True, current_modified_time
        else:
            return False, last_modified_time

    except Exception as e:
        logger.error(f"Error checking file modification time: {e}")
        return False, last_modified_time


def continuous_indicator_calculator():
    """
    Continuously monitor and update indicators as the CSV file changes
    """
    INPUT_FILE = "historic_df.csv"
    OUTPUT_FILE = "pinescript_indicators.csv"

    if os.path.exists(OUTPUT_FILE):
        unique_id = str(uuid.uuid4())
        archive_name = f"pinescript_indicators_{unique_id}.csv"
        os.rename(OUTPUT_FILE, archive_name)
        logger.info(f"Archived existing indicators file to {archive_name}")

    last_modified_time = None
    update_count = 0

    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file {INPUT_FILE} not found!")
        logger.info("Please ensure your range bar data is saved as 'historic_df.csv'")
        return

    print(
        f"{Back.GREEN}{Fore.BLACK}🚀 Starting Continuous Indicator Calculator 🚀{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}📁 Input: {INPUT_FILE}")
    print(f"💾 Output: {OUTPUT_FILE}")
    print(f"⏱️  Update Interval: {UPDATE_INTERVAL} seconds")
    print(f"📊 Display Rows: {DISPLAY_ROWS}")
    print(f"🔄 Press Ctrl+C to stop{Style.RESET_ALL}\n")

    try:
        while True:
            # Check if file has been updated
            is_updated, last_modified_time = check_file_updated(
                INPUT_FILE, last_modified_time
            )

            if is_updated:
                update_count += 1

                try:
                    # Load and process the data
                    start_time = time.time()
                    df_with_indicators = load_and_process_range_bars(INPUT_FILE)
                    processing_time = time.time() - start_time

                    # Save results to CSV
                    save_indicators_to_csv(df_with_indicators, OUTPUT_FILE)

                    # Print update header
                    print_update_header(update_count, len(df_with_indicators))

                    # Display both table formats
                    display_simple_indicators_table(
                        df_with_indicators, DISPLAY_ROWS, update_count
                    )

                    # Log processing stats
                    total_bars = len(df_with_indicators)
                    valid_rsi = df_with_indicators["rsi"].notna().sum()
                    valid_rsi_ma50 = df_with_indicators["rsi_ma50"].notna().sum()

                    stats_msg = (
                        f"✅ Update #{update_count} completed in {processing_time:.2f}s | "
                        f"Bars: {total_bars} | RSI: {valid_rsi} | RSI MA50: {valid_rsi_ma50}"
                    )
                    logger.info(stats_msg)
                    print(f"{Fore.GREEN}{stats_msg}{Style.RESET_ALL}")

                except Exception as e:
                    logger.error(f"❌ Error during update #{update_count}: {e}")
                    print(
                        f"{Fore.RED}❌ Error during update #{update_count}: {e}{Style.RESET_ALL}"
                    )

            else:
                # File not updated, show waiting message every 30 seconds
                if update_count == 0 or (time.time() % 30 < UPDATE_INTERVAL):
                    print(
                        f"{Fore.YELLOW}⏳ Waiting for {INPUT_FILE} to update... (Update #{update_count + 1}){Style.RESET_ALL}"
                    )

            # Wait for next check
            time.sleep(UPDATE_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}🛑 Stopping continuous calculator...{Style.RESET_ALL}")
        logger.info("Continuous calculator stopped by user")
    except Exception as e:
        logger.error(f"❌ Critical error in continuous calculator: {e}")
        print(f"{Fore.RED}❌ Critical error: {e}{Style.RESET_ALL}")


def configure_settings():
    """Allow user to configure update interval and display rows"""
    global UPDATE_INTERVAL, DISPLAY_ROWS

    print(f"{Back.BLUE}{Fore.WHITE}⚙️  CONFIGURATION SETTINGS ⚙️ {Style.RESET_ALL}")
    print(f"Current settings:")
    print(f"  📍 Update Interval: {UPDATE_INTERVAL} seconds")
    print(f"  📊 Display Rows: {DISPLAY_ROWS}")

    try:
        # Update interval
        new_interval = input(
            f"\n🕐 Enter new update interval in seconds (current: {UPDATE_INTERVAL}): "
        ).strip()
        if new_interval and new_interval.replace(".", "").isdigit():
            UPDATE_INTERVAL = float(new_interval)
            print(f"✅ Update interval set to {UPDATE_INTERVAL} seconds")

        # Display rows
        new_rows = input(
            f"📊 Enter number of rows to display (current: {DISPLAY_ROWS}): "
        ).strip()
        if new_rows and new_rows.isdigit():
            DISPLAY_ROWS = int(new_rows)
            print(f"✅ Display rows set to {DISPLAY_ROWS}")

        print(f"\n{Fore.GREEN}🎯 Configuration updated successfully!{Style.RESET_ALL}\n")

    except Exception as e:
        print(f"{Fore.RED}❌ Configuration error: {e}{Style.RESET_ALL}")


def get_choice_with_timeout():
    def input_thread():
        try:
            choice = input(f"\nEnter your choice (1/2/3): {Style.RESET_ALL}").strip()
            q.put(choice)
        except:
            pass

    q = queue.Queue()
    t = threading.Thread(target=input_thread)
    t.daemon = True
    t.start()
    try:
        choice = q.get(timeout=timeout_que)
    except queue.Empty:
        print(
            f"\n{Fore.YELLOW}Timeout, defaulting to continuous mode (1){Style.RESET_ALL}"
        )
        choice = "1"
    return choice


if __name__ == "__main__":
    try:
        print(f"{Back.MAGENTA}{Fore.WHITE}{'='*80}{Style.RESET_ALL}")
        print(
            f"{Back.MAGENTA}{Fore.WHITE}🚀 ENHANCED PINESCRIPT TO PYTHON INDICATOR CALCULATOR 🚀{Style.RESET_ALL}"
        )
        print(f"{Back.MAGENTA}{Fore.WHITE}{'='*80}{Style.RESET_ALL}")

        # Ask user for mode
        print(f"\n{Fore.CYAN}Choose operation mode:")
        print("1. 🔄 Continuous mode (monitors file changes)")
        print("2. ⚙️  Configure settings first")
        print("3. 📊 Single run mode (run once and exit)")

        choice = get_choice_with_timeout()

        if choice == "2":
            configure_settings()
            choice = input(
                f"\n{Fore.CYAN}Now choose mode (1 for continuous, 3 for single): {Style.RESET_ALL}"
            ).strip()

        if choice == "1":
            # Continuous mode
            continuous_indicator_calculator()

        elif choice == "3":
            # Single run mode (original functionality)
            INPUT_FILE = "historic_df.csv"
            OUTPUT_FILE = "pinescript_indicators.csv"

            if os.path.exists(OUTPUT_FILE):
                unique_id = str(uuid.uuid4())
                archive_name = f"pinescript_indicators_{unique_id}.csv"
                os.rename(OUTPUT_FILE, archive_name)
                logger.info(f"Archived existing indicators file to {archive_name}")

            if not os.path.exists(INPUT_FILE):
                logger.error(f"Input file {INPUT_FILE} not found!")
                logger.info(
                    "Please ensure your range bar data is saved as 'historic_df.csv'"
                )
                exit(1)

            logger.info("=== Single Run Mode ===")
            logger.info(f"Input file: {INPUT_FILE}")
            logger.info(f"Output file: {OUTPUT_FILE}")

            # Load and process the data
            df_with_indicators = load_and_process_range_bars(INPUT_FILE)

            # Validate RSI calculation
            validate_rsi_calculation(df_with_indicators)

            # Save results to CSV
            save_indicators_to_csv(df_with_indicators, OUTPUT_FILE)

            # Display summary table
            display_simple_indicators_table(df_with_indicators, DISPLAY_ROWS)

            logger.info("=== Processing Complete ===")
            logger.info(f"Results saved to {OUTPUT_FILE}")
            logger.info(f"Processed {len(df_with_indicators)} bars successfully")

        else:
            print(f"{Fore.RED}❌ Invalid choice. Exiting.{Style.RESET_ALL}")

    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        print(f"{Fore.RED}❌ Critical error: {e}{Style.RESET_ALL}")
        raise
