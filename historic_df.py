import json
import logging
import os
import signal
import ssl
import sys
import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from queue import Queue, Empty
import pandas as pd
import pytz
import websocket
from binance.client import Client
from binance.exceptions import BinanceAPIException
import uuid

# Configuration
api_key = "your_api_key"  # Replace with your Binance API key
api_secret = "your_api_secret"  # Replace with your Binance API secret
client = Client(api_key, api_secret)
filename = "historic_df.csv"  # Output CSV file
symbol = "SOLFDUSD"  # Trading pair

# Enhanced WebSocket configuration
websocket_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
max_retries = 20
initial_retry_delay = 10
inactivity_timeout = 1800  # 30 minutes
health_check_interval = 45  # Increased for stability
initial_fetch_hours = 0
ping_interval = 60  # Increased from 25 to reduce SSL stress
ping_timeout = 30  # Increased from 15 for SSL tolerance
CONTEXT_ROWS = 2
fetch_pause = 1.5
UPDATE_THRESHOLD_HOURS = 10
status_interval = 180
current_bar_state_file = "current_bar.json"

# Circuit breaker configuration
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 10
CIRCUIT_BREAKER_TIMEOUT = 300  # 5 minutes
CIRCUIT_BREAKER_HALF_OPEN_TIMEOUT = 60  # 1 minute

# Timezone setup
UTC = pytz.UTC
LOCAL_TZ = pytz.timezone("Africa/Nairobi")


# Connection states
class ConnectionState(Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN"
    CIRCUIT_BREAKER_HALF_OPEN = "CIRCUIT_BREAKER_HALF_OPEN"


class FailureType(Enum):
    SSL_ERROR = "SSL_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    API_ERROR = "API_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


# Enhanced global variables
data_lock = threading.Lock()
connection_lock = threading.Lock()
last_trade_time = None
last_message_time = None
connection_state = ConnectionState.DISCONNECTED
last_close_time = None
current_bar = None
shutdown_requested = threading.Event()
consecutive_trade_errors = 0
symbol_config = None
gap_fill_queue = Queue()

# Circuit breaker variables
circuit_breaker_failures = 0
circuit_breaker_last_failure = None
circuit_breaker_last_success = None


# Enhanced performance monitoring
class PerformanceMonitor:
    def __init__(self):
        self.trades_processed = 0
        self.bars_created = 0
        self.start_time = time.time()
        self.connection_attempts = 0
        self.successful_connections = 0
        self.ssl_errors = 0
        self.network_errors = 0
        self.last_connection_time = None
        self.connection_uptime = 0
        self.lock = threading.Lock()

    def record_connection_attempt(self):
        with self.lock:
            self.connection_attempts += 1

    def record_successful_connection(self):
        with self.lock:
            self.successful_connections += 1
            self.last_connection_time = time.time()

    def record_connection_end(self):
        with self.lock:
            if self.last_connection_time:
                self.connection_uptime += time.time() - self.last_connection_time
                self.last_connection_time = None

    def record_ssl_error(self):
        with self.lock:
            self.ssl_errors += 1

    def record_network_error(self):
        with self.lock:
            self.network_errors += 1

    def get_connection_success_rate(self):
        with self.lock:
            if self.connection_attempts == 0:
                return 0.0
            return (self.successful_connections / self.connection_attempts) * 100

    def get_average_connection_uptime(self):
        with self.lock:
            if self.successful_connections == 0:
                return 0.0
            total_uptime = self.connection_uptime
            if self.last_connection_time:
                total_uptime += time.time() - self.last_connection_time
            return total_uptime / self.successful_connections

    def log_stats(self):
        with self.lock:
            runtime = time.time() - self.start_time
            trades_per_sec = self.trades_processed / runtime if runtime > 0 else 0
            bars_per_hour = (self.bars_created / runtime) * 3600 if runtime > 0 else 0
            success_rate = self.get_connection_success_rate()
            avg_uptime = self.get_average_connection_uptime()

            logger.info(
                f"Performance: {trades_per_sec:.1f} trades/sec, {bars_per_hour:.1f} bars/hour, "
                f"Connection success: {success_rate:.1f}%, Avg uptime: {avg_uptime:.1f}s, "
                f"SSL errors: {self.ssl_errors}, Network errors: {self.network_errors}"
            )


performance_monitor = PerformanceMonitor()

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to reduce log noise
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("historic_df.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Debug logger for detailed troubleshooting
debug_logger = logging.getLogger("debug")
debug_handler = logging.FileHandler("debug.log")
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter(
    "%(asctime)s [DEBUG] %(funcName)s:%(lineno)d - %(message)s"
)
debug_handler.setFormatter(debug_formatter)
debug_logger.addHandler(debug_handler)
debug_logger.setLevel(logging.DEBUG)

# ANSI color codes
COLOR_RESET = "\033[0m"
COLOR_CYAN = "\033[1;36m"
COLOR_GREEN = "\033[1;32m"
COLOR_YELLOW = "\033[1;33m"
COLOR_RED = "\033[1;31m"
COLOR_MAGENTA = "\033[1;35m"
COLOR_BLUE = "\033[1;34m"
COLOR_WHITE = "\033[1;37m"


def print_timestamped(message, color=COLOR_WHITE):
    """Print message with timestamp in local timezone and color."""
    timestamp = pd.Timestamp.now(tz=LOCAL_TZ).strftime("%H:%M:%S")
    print(f"{color}[{timestamp}] {message}{COLOR_RESET}")


def print_salutation():
    """Print startup salutation message."""
    salutation = f"""
    {COLOR_CYAN}=====================================================
    Enhanced Grok4 Trading Bot v2.0
    Production-Ready with Enhanced SSL & Circuit Breaker
    Symbol: {symbol}
    Connection: Enhanced SSL WebSocket
    WebSocket URL: {websocket_url}
    Timezone: {LOCAL_TZ}
    Circuit Breaker: Enabled
    ====================================================={COLOR_RESET}
    """
    logger.info(salutation)
    print_timestamped(salutation, COLOR_CYAN)


def classify_error(error):
    """Enhanced error classification for appropriate handling."""
    error_str = str(error).lower()
    debug_logger.debug(f"Classifying error: {error_str}")

    # SSL-specific errors
    ssl_indicators = [
        "eof occurred in violation of protocol",
        "ssl",
        "certificate",
        "handshake",
        "tls",
        "connection broken",
        "bad handshake",
    ]
    if any(indicator in error_str for indicator in ssl_indicators):
        performance_monitor.record_ssl_error()
        return FailureType.SSL_ERROR

    # Network errors
    network_indicators = [
        "network",
        "timeout",
        "connection",
        "unreachable",
        "dns",
        "host",
        "resolve",
        "refused",
    ]
    if any(indicator in error_str for indicator in network_indicators):
        performance_monitor.record_network_error()
        return FailureType.NETWORK_ERROR

    # API errors
    if "rate limit" in error_str or "429" in error_str or "binance" in error_str:
        return FailureType.API_ERROR

    # Timeout errors
    if "timeout" in error_str or "timed out" in error_str:
        return FailureType.TIMEOUT_ERROR

    return FailureType.UNKNOWN_ERROR


def get_ssl_context():
    """Create optimized SSL context for stable connections."""
    context = ssl.create_default_context()

    # Enhanced SSL configuration for stability
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED

    # Set secure protocol versions
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.set_ciphers(
        "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
    )

    # Connection optimization
    context.set_alpn_protocols(["http/1.1"])

    debug_logger.debug("Created optimized SSL context")
    return context


def update_connection_state(new_state):
    """Thread-safe connection state updates."""
    global connection_state
    with connection_lock:
        old_state = connection_state
        connection_state = new_state
        if old_state != new_state:
            logger.info(f"Connection state: {old_state.value} → {new_state.value}")
            debug_logger.debug(f"State transition: {old_state} to {new_state}")


def check_circuit_breaker():
    """Check if circuit breaker should prevent connection attempts."""
    global circuit_breaker_failures, circuit_breaker_last_failure

    current_time = time.time()

    # Check if we're in circuit breaker state
    if circuit_breaker_failures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD:
        if circuit_breaker_last_failure:
            time_since_failure = current_time - circuit_breaker_last_failure

            if connection_state == ConnectionState.CIRCUIT_BREAKER_OPEN:
                if time_since_failure >= CIRCUIT_BREAKER_TIMEOUT:
                    update_connection_state(ConnectionState.CIRCUIT_BREAKER_HALF_OPEN)
                    logger.warning("Circuit breaker entering HALF_OPEN state")
                    return True
                else:
                    remaining = CIRCUIT_BREAKER_TIMEOUT - time_since_failure
                    debug_logger.debug(
                        f"Circuit breaker OPEN, {remaining:.0f}s remaining"
                    )
                    return False
            elif connection_state == ConnectionState.CIRCUIT_BREAKER_HALF_OPEN:
                if time_since_failure >= CIRCUIT_BREAKER_HALF_OPEN_TIMEOUT:
                    # Reset circuit breaker after successful test period
                    circuit_breaker_failures = 0
                    circuit_breaker_last_failure = None
                    update_connection_state(ConnectionState.DISCONNECTED)
                    logger.info("Circuit breaker CLOSED - failures reset")
                    return True
                return True

        update_connection_state(ConnectionState.CIRCUIT_BREAKER_OPEN)
        logger.error(
            f"Circuit breaker OPEN - too many failures ({circuit_breaker_failures})"
        )
        return False

    return True


def record_connection_failure(failure_type):
    """Record connection failure for circuit breaker logic."""
    global circuit_breaker_failures, circuit_breaker_last_failure

    circuit_breaker_failures += 1
    circuit_breaker_last_failure = time.time()

    logger.warning(
        f"Connection failure recorded: {failure_type.value} (total: {circuit_breaker_failures})"
    )
    debug_logger.debug(f"Failure #{circuit_breaker_failures}: {failure_type}")

    if circuit_breaker_failures >= CIRCUIT_BREAKER_FAILURE_THRESHOLD:
        update_connection_state(ConnectionState.CIRCUIT_BREAKER_OPEN)


def record_connection_success():
    """Record successful connection."""
    global circuit_breaker_failures, circuit_breaker_last_failure, circuit_breaker_last_success

    circuit_breaker_last_success = time.time()

    # Gradually reduce failure count on successful connections
    if circuit_breaker_failures > 0:
        circuit_breaker_failures = max(0, circuit_breaker_failures - 2)
        logger.info(
            f"Connection success - failure count reduced to {circuit_breaker_failures}"
        )

    performance_monitor.record_successful_connection()
    debug_logger.debug("Connection success recorded")


def get_symbol_info(symbol):
    """Get symbol precision and tick size from Binance with retry logic."""
    for attempt in range(3):
        try:
            info = client.get_symbol_info(symbol)
            tick_size = None
            step_size = None

            for filter_item in info["filters"]:
                if filter_item["filterType"] == "PRICE_FILTER":
                    tick_size = float(filter_item["tickSize"])
                elif filter_item["filterType"] == "LOT_SIZE":
                    step_size = float(filter_item["stepSize"])

            price_precision = (
                len(str(tick_size).rstrip("0").split(".")[-1])
                if "." in str(tick_size)
                else 0
            )

            config = {
                "tick_size": tick_size,
                "step_size": step_size,
                "price_precision": price_precision,
                "base_asset": info["baseAsset"],
                "quote_asset": info["quoteAsset"],
                "range_size": tick_size,
                "max_bar_duration": 3600,
                "min_trades_per_bar": 1,
            }

            logger.info(f"Symbol config: {config}")
            print_timestamped(
                f"Symbol config loaded: tick_size={tick_size}, precision={price_precision}",
                COLOR_GREEN,
            )
            return config

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} to get symbol info failed: {e}")
            if attempt == 2:
                logger.error(f"All attempts to get symbol info failed: {e}")
                return None
            time.sleep(2**attempt)

    return None


def save_current_bar_state():
    """Save incomplete bar state for recovery with enhanced error handling."""
    global current_bar
    if current_bar:
        try:
            with data_lock:
                bar_copy = current_bar.copy()
                bar_copy["Open Time"] = bar_copy["Open Time"].isoformat()

                # Atomic write to prevent corruption
                temp_file = current_bar_state_file + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(bar_copy, f, indent=2)
                os.replace(temp_file, current_bar_state_file)

                debug_logger.debug(
                    f"Saved current bar state to {current_bar_state_file}"
                )
        except Exception as e:
            logger.error(f"Error saving current bar state: {e}")


def load_current_bar_state():
    """Restore incomplete bar state after restart with validation."""
    try:
        with open(current_bar_state_file, "r") as f:
            bar_data = json.load(f)

        # Validate required fields
        required_fields = ["Open Time", "Open", "High", "Low", "Close", "Volume"]
        for field in required_fields:
            if field not in bar_data:
                logger.warning(f"Invalid bar state: missing field {field}")
                return None

        bar_data["Open Time"] = pd.Timestamp(bar_data["Open Time"])

        # Validate timestamp is reasonable (not too old)
        now = pd.Timestamp.now(tz=UTC)
        if (now - bar_data["Open Time"]).total_seconds() > 7200:  # 2 hours
            logger.warning("Bar state too old, discarding")
            return None

        logger.info(f"Restored incomplete bar state from {current_bar_state_file}")
        print_timestamped("Restored incomplete bar state", COLOR_GREEN)
        return bar_data

    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        debug_logger.debug(f"No current bar state to restore: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading current bar state: {e}")
        return None


def cleanup_current_bar_state():
    """Remove current bar state file safely."""
    try:
        if os.path.exists(current_bar_state_file):
            os.remove(current_bar_state_file)
            debug_logger.debug(f"Cleaned up {current_bar_state_file}")
    except Exception as e:
        logger.error(f"Error cleaning up current bar state file: {e}")


def validate_trade(trade, last_trade=None):
    """Enhanced trade validation with more checks."""
    try:
        price = float(trade.get("price", 0))
        qty = float(trade.get("qty", 0))

        # Basic validation
        if price <= 0 or qty <= 0:
            debug_logger.debug(f"Invalid trade: price={price}, qty={qty}")
            return False

        # Price spike detection
        if last_trade:
            last_price = float(last_trade.get("price", 0))
            if last_price > 0:
                price_change = abs(price - last_price) / last_price
                if price_change > 0.15:  # 15% spike filter (increased tolerance)
                    logger.warning(f"Suspicious price movement: {price_change:.2%}")
                    return False

        # Quantity validation (reasonable trade size)
        if qty > 1000000:  # Sanity check for extremely large trades
            logger.warning(f"Suspicious trade quantity: {qty}")
            return False

        return True

    except (ValueError, TypeError) as e:
        debug_logger.debug(f"Trade validation error: {e}")
        return False


def count_csv_rows(filename):
    """Count total rows in CSV file efficiently with error handling."""
    try:
        if not os.path.exists(filename):
            return 0

        with open(filename, "r") as f:
            row_count = sum(1 for _ in f) - 1  # Subtract header

        debug_logger.debug(f"Counted {row_count} rows in {filename}")
        return max(0, row_count)  # Ensure non-negative

    except Exception as e:
        logger.error(f"Error counting rows in {filename}: {e}")
        return 0


def load_from_csv(filename):
    """Load CSV with enhanced error handling and validation."""
    try:
        if not os.path.exists(filename):
            debug_logger.debug(f"File {filename} does not exist")
            return None

        if os.path.getsize(filename) == 0:
            logger.warning(f"File {filename} is empty")
            return None

        total_rows = count_csv_rows(filename)
        if total_rows <= 0:
            logger.warning(f"File {filename} has no valid data")
            return None

        skip_rows = max(0, total_rows - CONTEXT_ROWS)
        rows_to_read = min(total_rows, CONTEXT_ROWS)

        df = pd.read_csv(
            filename,
            skiprows=skip_rows,
            nrows=rows_to_read,
            parse_dates=["Open Time", "Close Time"],
        )

        if df.empty:
            logger.warning(f"File {filename} contains no data after processing")
            return None

        # Enhanced timezone handling
        for col in ["Open Time", "Close Time"]:
            if col in df.columns:
                if df[col].dt.tz is None:
                    df[col] = df[col].dt.tz_localize(LOCAL_TZ).dt.tz_convert(UTC)
                else:
                    df[col] = df[col].dt.tz_convert(UTC)

        logger.info(f"Loaded {len(df)} rows from {filename} for continuity check")
        return df

    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return None


def save_to_csv(df, filename, append=True):
    """Save DataFrame with atomic writes to prevent corruption."""
    try:
        if df.empty:
            logger.warning("Attempted to save empty DataFrame")
            return

        df_copy = df.copy()

        # Convert timezone for storage
        for col in ["Open Time", "Close Time"]:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)

        # Atomic write using temporary file
        temp_filename = filename + ".tmp"
        mode = "a" if append and os.path.exists(filename) else "w"
        header = not (append and os.path.exists(filename))

        df_copy.to_csv(temp_filename, mode="w", header=True, index=False)

        if append and os.path.exists(filename):
            # Append to existing file
            with open(filename, "a") as target, open(temp_filename, "r") as source:
                source.readline()  # Skip header
                target.write(source.read())
            os.remove(temp_filename)
        else:
            # Replace file
            os.replace(temp_filename, filename)

        logger.info(
            f"{'Appended' if append else 'Saved'} {len(df_copy)} rows to {filename}"
        )

    except Exception as e:
        logger.error(f"Error saving to {filename}: {e}")
        # Clean up temp file on error
        try:
            if os.path.exists(filename + ".tmp"):
                os.remove(filename + ".tmp")
        except:
            pass


def check_api_rate_limit():
    """Enhanced API rate limit checking with backoff."""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = client.get_system_status()
            debug_logger.debug("API rate limit check: System status OK")
            return True

        except BinanceAPIException as e:
            if e.status_code == 429:
                wait_time = 60 * (2**attempt)  # Exponential backoff
                logger.warning(
                    f"API rate limit exceeded. Waiting {wait_time} seconds..."
                )
                time.sleep(wait_time)
                if attempt == max_attempts - 1:
                    return False
                continue
            else:
                logger.error(f"API error during rate limit check: {e}")
                return False

        except Exception as e:
            logger.error(f"Unexpected error in rate limit check: {e}")
            if attempt == max_attempts - 1:
                return False
            time.sleep(2**attempt)

    return False


def fetch_historical_trades_batch(start_time, end_time):
    """Fetch historical trades with enhanced error handling and circuit breaker awareness."""
    trades = []
    current_start = start_time
    chunk_size = timedelta(hours=1)
    retry_count = 0
    total_hours = int((end_time - start_time).total_seconds() / 3600)
    current_hour = 0

    debug_logger.debug(
        f"Fetching historical trades from {start_time} to {end_time} ({total_hours} hours)"
    )

    while current_start < end_time and not shutdown_requested.is_set():
        # Check circuit breaker state
        if not check_circuit_breaker():
            logger.warning("Circuit breaker preventing historical fetch")
            break

        current_hour += 1
        chunk_end = min(current_start + chunk_size, end_time)
        end_time_ms = int(chunk_end.timestamp() * 1000)
        last_id = None
        chunk_trades = []

        while True:
            if shutdown_requested.is_set():
                logger.info("Shutdown requested during historical fetch")
                return trades

            try:
                if not check_api_rate_limit():
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error("Max retries reached for rate limit check")
                        return trades
                    continue

                batch = client.get_historical_trades(
                    symbol=symbol, limit=1000, fromId=last_id
                )
                if not batch:
                    debug_logger.debug("No more trades in chunk")
                    break

                chunk_trades.extend(
                    [
                        t
                        for t in batch
                        if t["time"] >= int(current_start.timestamp() * 1000)
                    ]
                )

                last_id = batch[-1]["id"] + 1
                debug_logger.debug(
                    f"Fetched batch of {len(batch)} trades, last ID: {last_id}"
                )

                if batch[-1]["time"] >= end_time_ms:
                    break

                retry_count = 0

            except Exception as e:
                failure_type = classify_error(e)
                logger.error(f"{failure_type.value} during historical fetch: {e}")

                if retry_count >= max_retries:
                    logger.error("Max retries reached for historical fetch")
                    return trades

                # Adaptive delay based on error type
                if failure_type == FailureType.SSL_ERROR:
                    sleep_time = 5
                elif failure_type == FailureType.API_ERROR:
                    sleep_time = 30
                else:
                    sleep_time = initial_retry_delay * (1.5**retry_count)

                logger.warning(f"Retrying historical fetch in {sleep_time} seconds...")
                time.sleep(sleep_time)
                retry_count += 1

        trades.extend(chunk_trades)
        logger.info(
            f"Fetched {len(chunk_trades)} trades for hour {current_hour}/{total_hours}"
        )

        if current_start + chunk_size < end_time and not shutdown_requested.is_set():
            debug_logger.debug(f"Pausing for {fetch_pause} seconds before next fetch")
            time.sleep(fetch_pause)

        current_start += chunk_size

    filtered_trades = [
        t for t in trades if t["time"] <= int(end_time.timestamp() * 1000)
    ]
    logger.info(f"Fetched {len(filtered_trades)} filtered historical trades")
    return filtered_trades


def should_close_bar(current_bar, new_trade_time, range_size):
    """Enhanced bar closure logic with validation."""
    if current_bar is None:
        return False

    try:
        range_span = current_bar["range_max"] - current_bar["range_min"]

        # Close if range exceeded
        if range_span >= range_size:
            debug_logger.debug(f"Bar closed by range: {range_span} >= {range_size}")
            return True

        # Close if max duration exceeded (backup mechanism)
        time_elapsed = (new_trade_time - current_bar["Open Time"]).total_seconds()
        max_bar_duration = (
            symbol_config.get("max_bar_duration", 3600) if symbol_config else 3600
        )

        if time_elapsed > max_bar_duration:
            debug_logger.debug(
                f"Bar closed by time: {time_elapsed} > {max_bar_duration}"
            )
            return True

        return False

    except Exception as e:
        logger.error(f"Error in should_close_bar: {e}")
        return False


def aggregate_trades_to_range_bars(trades, range_size):
    """Enhanced trade aggregation with better error handling."""
    try:
        if not trades:
            logger.warning("No trades to aggregate")
            return pd.DataFrame(), None

        trades_df = pd.DataFrame(
            trades,
            columns=[
                "id",
                "price",
                "qty",
                "quoteQty",
                "time",
                "isBuyerMaker",
                "isBestMatch",
            ],
        )

        # Data type conversion with validation
        for col in ["price", "qty", "quoteQty"]:
            trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce")

        # Remove invalid trades
        trades_df = trades_df.dropna(subset=["price", "qty", "quoteQty"])
        if trades_df.empty:
            logger.warning("No valid trades after data cleaning")
            return pd.DataFrame(), None

        trades_df["time"] = pd.to_datetime(trades_df["time"], unit="ms").dt.tz_localize(
            UTC
        )

        range_bars = []
        current_bar = None
        last_trade = None

        for _, trade in trades_df.iterrows():
            if not validate_trade(trade, last_trade):
                debug_logger.debug(f"Invalid trade data, skipping: {trade['price']}")
                continue

            price = trade["price"]
            volume = trade["qty"]
            quote_volume = trade["quoteQty"]
            trade_time = trade["time"]

            if current_bar is None:
                current_bar = {
                    "Open Time": trade_time,
                    "Open": price,
                    "High": price,
                    "Low": price,
                    "Close": price,
                    "Volume": volume,
                    "Quote Asset Volume": quote_volume,
                    "Number of Trades": 1,
                    "Taker Buy Base Asset Volume": volume
                    if not trade["isBuyerMaker"]
                    else 0,
                    "Taker Buy Quote Asset Volume": quote_volume
                    if not trade["isBuyerMaker"]
                    else 0,
                    "range_min": price,
                    "range_max": price,
                }
                debug_logger.debug(f"Started new range bar at price {price:.6f}")
            else:
                current_bar["High"] = max(current_bar["High"], price)
                current_bar["Low"] = min(current_bar["Low"], price)
                current_bar["Close"] = price
                current_bar["Volume"] += volume
                current_bar["Quote Asset Volume"] += quote_volume
                current_bar["Number of Trades"] += 1

                if not trade["isBuyerMaker"]:
                    current_bar["Taker Buy Base Asset Volume"] += volume
                    current_bar["Taker Buy Quote Asset Volume"] += quote_volume

                current_bar["range_min"] = min(current_bar["range_min"], price)
                current_bar["range_max"] = max(current_bar["range_max"], price)

                if should_close_bar(current_bar, trade_time, range_size):
                    current_bar["Close Time"] = trade_time
                    current_bar["Ignore"] = 0
                    range_bars.append(current_bar.copy())
                    performance_monitor.bars_created += 1

                    logger.info(
                        f"Closed range bar: Open={current_bar['Open']:.6f}, Close={current_bar['Close']:.6f}"
                    )

                    current_bar = {
                        "Open Time": trade_time,
                        "Open": price,
                        "High": price,
                        "Low": price,
                        "Close": price,
                        "Volume": volume,
                        "Quote Asset Volume": quote_volume,
                        "Number of Trades": 1,
                        "Taker Buy Base Asset Volume": volume
                        if not trade["isBuyerMaker"]
                        else 0,
                        "Taker Buy Quote Asset Volume": quote_volume
                        if not trade["isBuyerMaker"]
                        else 0,
                        "range_min": price,
                        "range_max": price,
                    }
                    debug_logger.debug(f"Started new range bar at price {price:.6f}")

            last_trade = trade
            performance_monitor.trades_processed += 1

        if not range_bars:
            debug_logger.debug("No range bars created from trades")
            return pd.DataFrame(), current_bar

        range_bars_df = pd.DataFrame(range_bars)[
            [
                "Open Time",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Close Time",
                "Quote Asset Volume",
                "Number of Trades",
                "Taker Buy Base Asset Volume",
                "Taker Buy Quote Asset Volume",
                "Ignore",
            ]
        ]
        logger.info(
            f"Created {len(range_bars_df)} range bars from {len(trades_df)} trades"
        )
        return range_bars_df, current_bar

    except Exception as e:
        logger.error(f"Error aggregating trades to range bars: {e}")
        return pd.DataFrame(), None


def detect_data_gap():
    """Enhanced data gap detection with better validation."""
    now_utc = pd.Timestamp.now(tz=UTC)
    last_bar_time = None

    try:
        if os.path.exists(filename):
            df = load_from_csv(filename)
            if df is not None and not df.empty:
                last_bar_time = df["Close Time"].iloc[-1]

        current_bar_state = load_current_bar_state()
        if current_bar_state and "Open Time" in current_bar_state:
            bar_open_time = current_bar_state["Open Time"]
            if isinstance(bar_open_time, str):
                bar_open_time = pd.Timestamp(bar_open_time, tz=UTC)
            if last_bar_time is None or bar_open_time > last_bar_time:
                last_bar_time = bar_open_time

        if last_bar_time is not None:
            gap_seconds = (now_utc - last_bar_time).total_seconds()
            if gap_seconds > 60:  # 1 minute threshold
                logger.info(f"Data gap detected: {gap_seconds:.1f} seconds")
                return last_bar_time, now_utc

        return None, None

    except Exception as e:
        logger.error(f"Error detecting data gap: {e}")
        return None, None


def validate_data_continuity(new_bars, existing_bars):
    """Enhanced data continuity validation."""
    if existing_bars is None or existing_bars.empty or new_bars.empty:
        return True

    try:
        last_existing_close = float(existing_bars["Close"].iloc[-1])
        first_new_open = float(new_bars["Open"].iloc[0])
        price_gap = abs(first_new_open - last_existing_close) / last_existing_close

        # More tolerant price gap for crypto volatility
        if price_gap > 0.20:  # 20% price jump indicates possible data issue
            logger.warning(f"Large price gap detected: {price_gap:.2%}")
            return False

        # Check time continuity
        last_existing_time = existing_bars["Close Time"].iloc[-1]
        first_new_time = new_bars["Open Time"].iloc[0]
        time_gap = (first_new_time - last_existing_time).total_seconds()

        # Allow reasonable time gaps
        if time_gap > 7200:  # 2 hours
            logger.warning(f"Large time gap detected: {time_gap:.0f} seconds")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating data continuity: {e}")
        return False


def gap_fill_worker():
    """Background thread for processing gap fill requests."""
    while not shutdown_requested.is_set():
        try:
            # Wait for gap fill request with timeout
            gap_request = gap_fill_queue.get(timeout=30)
            if gap_request is None:  # Shutdown signal
                break

            start_time, end_time = gap_request
            logger.info(f"Processing gap fill request: {start_time} to {end_time}")

            # Check if we should process this request
            if not check_circuit_breaker():
                logger.warning("Circuit breaker preventing gap fill")
                gap_fill_queue.task_done()
                continue

            trades = fetch_historical_trades_batch(start_time, end_time)

            if trades:
                range_size = symbol_config["range_size"] if symbol_config else 0.01
                range_bars_df, remaining_bar = aggregate_trades_to_range_bars(
                    trades, range_size
                )

                if not range_bars_df.empty:
                    existing_df = (
                        load_from_csv(filename)
                        if os.path.exists(filename)
                        else pd.DataFrame()
                    )

                    if validate_data_continuity(range_bars_df, existing_df):
                        save_to_csv(range_bars_df, filename, append=True)
                        logger.info(f"Gap filled with {len(range_bars_df)} range bars")

                        # Update global state safely
                        global current_bar, last_close_time
                        with data_lock:
                            if remaining_bar and (
                                current_bar is None
                                or remaining_bar["Open Time"]
                                > current_bar.get(
                                    "Open Time", pd.Timestamp.min.tz_localize(UTC)
                                )
                            ):
                                current_bar = remaining_bar
                            if not range_bars_df.empty:
                                last_close_time = range_bars_df["Close Time"].iloc[-1]
                    else:
                        logger.error("Gap fill data continuity validation failed")
                else:
                    logger.warning("No range bars created for gap fill")
            else:
                logger.warning("No trades fetched for gap fill")

            gap_fill_queue.task_done()

        except Empty:
            # Timeout waiting for gap fill request - normal operation
            continue
        except Exception as e:
            logger.error(f"Error in gap fill worker: {e}")
            try:
                gap_fill_queue.task_done()
            except:
                pass


def fill_data_gap_async(start_time, end_time):
    """Queue gap fill request for background processing."""
    try:
        gap_fill_queue.put((start_time, end_time), timeout=5)
        logger.info(f"Queued gap fill request: {start_time} to {end_time}")
    except:
        logger.warning("Gap fill queue is full, skipping request")


def startup_data_recovery():
    """Enhanced startup data recovery with better error handling."""
    global current_bar, last_close_time

    try:
        logger.info("Starting enhanced data recovery process...")
        print_timestamped("Starting enhanced data recovery process...", COLOR_BLUE)

        existing_df = (
            load_from_csv(filename) if os.path.exists(filename) else pd.DataFrame()
        )
        current_bar = load_current_bar_state()

        # Detect and handle data gaps
        gap_start, gap_end = detect_data_gap()

        if gap_start and gap_end:
            gap_duration = (gap_end - gap_start).total_seconds()
            logger.info(f"Data gap detected: {gap_duration:.0f} seconds")
            print_timestamped(
                f"Data gap detected: {gap_duration:.0f} seconds", COLOR_YELLOW
            )

            # Only fill gaps less than 4 hours to avoid overwhelming startup
            if gap_duration <= 14400:  # 4 hours
                logger.info("Gap within reasonable range, filling synchronously")
                trades = fetch_historical_trades_batch(gap_start, gap_end)

                if trades:
                    range_size = symbol_config["range_size"] if symbol_config else 0.01
                    range_bars_df, remaining_bar = aggregate_trades_to_range_bars(
                        trades, range_size
                    )

                    if not range_bars_df.empty and validate_data_continuity(
                        range_bars_df, existing_df
                    ):
                        save_to_csv(range_bars_df, filename, append=True)
                        logger.info(
                            f"Successfully filled gap with {len(range_bars_df)} bars"
                        )
                        print_timestamped(
                            f"Gap filled with {len(range_bars_df)} bars", COLOR_GREEN
                        )

                        if remaining_bar:
                            current_bar = remaining_bar
                        last_close_time = range_bars_df["Close Time"].iloc[-1]
                    else:
                        logger.warning("Gap fill validation failed")
                else:
                    logger.warning("No trades retrieved for gap fill")
            else:
                logger.warning(
                    f"Gap too large ({gap_duration:.0f}s), will fill asynchronously"
                )
                print_timestamped(
                    "Large gap detected, will fill in background", COLOR_YELLOW
                )
                fill_data_gap_async(gap_start, gap_end)
        else:
            logger.info("No data gap detected")
            print_timestamped("No data gap detected", COLOR_GREEN)

        # Set last_close_time from existing data if not set
        if last_close_time is None and os.path.exists(filename):
            df = load_from_csv(filename)
            if df is not None and not df.empty:
                last_close_time = df["Close Time"].iloc[-1]

        cleanup_current_bar_state()  # Clean up old state file
        logger.info("Data recovery process completed")
        print_timestamped("Data recovery process completed", COLOR_GREEN)

    except Exception as e:
        logger.error(f"Error during startup data recovery: {e}")
        print_timestamped(f"Data recovery error: {e}", COLOR_RED)


def process_trade(trade):
    """Enhanced trade processing with better error handling and state management."""
    global current_bar, last_close_time, consecutive_trade_errors

    try:
        with data_lock:
            price = trade["price"]
            volume = trade["qty"]
            quote_volume = trade["quoteQty"]
            trade_time = trade["time"]
            is_buyer_maker = trade["isBuyerMaker"]
            range_size = symbol_config["range_size"] if symbol_config else 0.01

            if current_bar is None:
                current_bar = {
                    "Open Time": trade_time,
                    "Open": price,
                    "High": price,
                    "Low": price,
                    "Close": price,
                    "Volume": volume,
                    "Quote Asset Volume": quote_volume,
                    "Number of Trades": 1,
                    "Taker Buy Base Asset Volume": volume if not is_buyer_maker else 0,
                    "Taker Buy Quote Asset Volume": quote_volume
                    if not is_buyer_maker
                    else 0,
                    "range_min": price,
                    "range_max": price,
                }
                debug_logger.debug(f"Started new range bar at price {price:.6f}")
            else:
                # Update existing bar
                current_bar["High"] = max(current_bar["High"], price)
                current_bar["Low"] = min(current_bar["Low"], price)
                current_bar["Close"] = price
                current_bar["Volume"] += volume
                current_bar["Quote Asset Volume"] += quote_volume
                current_bar["Number of Trades"] += 1

                if not is_buyer_maker:
                    current_bar["Taker Buy Base Asset Volume"] += volume
                    current_bar["Taker Buy Quote Asset Volume"] += quote_volume

                current_bar["range_min"] = min(current_bar["range_min"], price)
                current_bar["range_max"] = max(current_bar["range_max"], price)

                if should_close_bar(current_bar, trade_time, range_size):
                    # Close current bar
                    current_bar["Close Time"] = trade_time
                    current_bar["Ignore"] = 0

                    # Create DataFrame and save
                    bar_df = pd.DataFrame([current_bar])
                    bar_df = bar_df[
                        [
                            "Open Time",
                            "Open",
                            "High",
                            "Low",
                            "Close",
                            "Volume",
                            "Close Time",
                            "Quote Asset Volume",
                            "Number of Trades",
                            "Taker Buy Base Asset Volume",
                            "Taker Buy Quote Asset Volume",
                            "Ignore",
                        ]
                    ]

                    save_to_csv(bar_df, filename, append=True)
                    performance_monitor.bars_created += 1

                    logger.info(
                        f"Range bar closed: Price={current_bar['Close']:.6f}, Range={current_bar['range_max'] - current_bar['range_min']:.6f}"
                    )
                    print_timestamped(
                        f"CSV updated - Range bar closed - Price: {current_bar['Close']:.6f}",
                        COLOR_GREEN,
                    )

                    last_close_time = current_bar["Close Time"]

                    # Start new bar
                    current_bar = {
                        "Open Time": trade_time,
                        "Open": price,
                        "High": price,
                        "Low": price,
                        "Close": price,
                        "Volume": volume,
                        "Quote Asset Volume": quote_volume,
                        "Number of Trades": 1,
                        "Taker Buy Base Asset Volume": volume
                        if not is_buyer_maker
                        else 0,
                        "Taker Buy Quote Asset Volume": quote_volume
                        if not is_buyer_maker
                        else 0,
                        "range_min": price,
                        "range_max": price,
                    }
                    debug_logger.debug(f"Started new range bar at price {price:.6f}")

            # Periodic state saving
            if current_bar and current_bar["Number of Trades"] % 100 == 0:
                save_current_bar_state()

            performance_monitor.trades_processed += 1
            consecutive_trade_errors = 0

    except Exception as e:
        consecutive_trade_errors += 1
        logger.error(f"Trade processing error #{consecutive_trade_errors}: {e}")
        debug_logger.exception("Trade processing exception details")

        if consecutive_trade_errors >= 10:
            logger.critical(
                "Too many consecutive trade errors. This may indicate a fundamental issue."
            )
            record_connection_failure(FailureType.UNKNOWN_ERROR)
            raise Exception("Trade processing failure - too many consecutive errors")


def signal_handler(sig, frame):
    """Enhanced shutdown signal handler."""
    logger.info("Shutdown signal received. Initiating graceful shutdown...")
    print_timestamped("Shutdown signal received. Cleaning up...", COLOR_YELLOW)

    shutdown_requested.set()
    save_current_bar_state()

    # Signal gap fill worker to stop
    try:
        gap_fill_queue.put(None, timeout=1)
    except:
        pass


def on_open(ws):
    """Enhanced WebSocket open handler."""
    global last_message_time, last_trade_time

    update_connection_state(ConnectionState.CONNECTED)
    last_message_time = time.time()
    last_trade_time = time.time()

    record_connection_success()

    logger.info("WebSocket connection established successfully")
    print_timestamped("WebSocket connection established", COLOR_GREEN)

    debug_logger.debug(f"WebSocket opened with URL: {ws.url}")


def on_message(ws, message):
    """Enhanced message handler with better error handling."""
    global last_trade_time, last_message_time

    try:
        data = json.loads(message)
        last_message_time = time.time()

        if data.get("e") == "trade":
            trade = {
                "price": float(data["p"]),
                "qty": float(data["q"]),
                "quoteQty": float(data["p"]) * float(data["q"]),
                "time": pd.Timestamp(int(data["T"]), unit="ms", tz=UTC),
                "isBuyerMaker": data["m"],
            }

            last_trade_time = time.time()
            debug_logger.debug(f"Received trade: Price={data['p']}, Qty={data['q']}")

            if validate_trade(trade):
                process_trade(trade)
            else:
                logger.warning(
                    f"Invalid trade received: price={trade['price']}, qty={trade['qty']}"
                )

        elif data.get("e") == "24hrTicker":
            # Handle ticker messages if any
            debug_logger.debug("Received 24hr ticker message")
            last_message_time = time.time()

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in WebSocket message: {e}")
        debug_logger.debug(f"Invalid JSON message: {message[:200]}")
    except Exception as e:
        logger.error(f"Error processing WebSocket message: {e}")
        debug_logger.exception("WebSocket message processing exception")


def on_error(ws, error):
    """Enhanced WebSocket error handler with detailed classification."""
    global connection_state

    failure_type = classify_error(error)
    error_context = {
        "error": str(error),
        "type": failure_type.value,
        "connection_state": connection_state.value,
        "websocket_url": ws.url if hasattr(ws, "url") else "unknown",
    }

    logger.error(f"WebSocket {failure_type.value}: {error}")
    print_timestamped(f"WebSocket {failure_type.value}: {error}", COLOR_RED)
    debug_logger.error(f"WebSocket error details: {error_context}")

    # Update connection state based on error type
    if failure_type == FailureType.SSL_ERROR:
        update_connection_state(ConnectionState.DISCONNECTED)
        record_connection_failure(failure_type)
        # Force close for SSL errors to trigger immediate reconnection
        try:
            ws.close()
        except:
            pass
    elif failure_type in [FailureType.NETWORK_ERROR, FailureType.TIMEOUT_ERROR]:
        update_connection_state(ConnectionState.RECONNECTING)
        record_connection_failure(failure_type)
    else:
        update_connection_state(ConnectionState.DISCONNECTED)
        record_connection_failure(failure_type)


def on_close(ws, close_status_code, close_msg):
    """Enhanced WebSocket close handler."""
    update_connection_state(ConnectionState.DISCONNECTED)
    performance_monitor.record_connection_end()

    close_info = (
        f"Code: {close_status_code}, Message: {close_msg}"
        if close_status_code
        else "Normal close"
    )
    logger.info(f"WebSocket closed - {close_info}")
    print_timestamped(f"WebSocket closed - {close_info}", COLOR_YELLOW)

    debug_logger.debug(
        f"WebSocket close details: code={close_status_code}, msg={close_msg}"
    )


def enhanced_health_monitor(ws):
    """Enhanced health monitoring with adaptive thresholds."""
    global last_message_time, last_trade_time

    consecutive_ping_failures = 0
    max_ping_failures = 5  # Increased tolerance
    last_ping_time = time.time()

    while (
        connection_state == ConnectionState.CONNECTED
        and not shutdown_requested.is_set()
    ):
        time.sleep(health_check_interval)
        current_time = time.time()

        # Check for trade inactivity
        time_since_trade = current_time - last_trade_time
        if time_since_trade > inactivity_timeout:
            logger.warning(
                f"No trades received for {time_since_trade:.0f} seconds. Closing WebSocket."
            )
            print_timestamped(
                f"Trade inactivity detected: {time_since_trade:.0f}s", COLOR_YELLOW
            )
            try:
                ws.close()
            except:
                pass
            break

        # Enhanced ping failure detection
        time_since_message = current_time - last_message_time
        time_since_ping = current_time - last_ping_time

        if time_since_message > ping_interval * 2:
            consecutive_ping_failures += 1
            logger.warning(
                f"Communication gap detected ({consecutive_ping_failures}/{max_ping_failures}). "
                f"Last message: {time_since_message:.1f}s ago"
            )

            if consecutive_ping_failures >= max_ping_failures:
                logger.error(
                    "Multiple ping failures detected. Forcing WebSocket reconnection."
                )
                print_timestamped(
                    "Multiple communication failures - forcing reconnection", COLOR_RED
                )
                record_connection_failure(FailureType.TIMEOUT_ERROR)
                try:
                    ws.close()
                except:
                    pass
                break
        else:
            consecutive_ping_failures = 0  # Reset on successful communication

        last_ping_time = current_time


def enhanced_status_thread():
    """Enhanced status reporting with comprehensive metrics."""
    counter = 0
    while not shutdown_requested.is_set():
        time.sleep(status_interval)

        if shutdown_requested.is_set():
            break

        try:
            row_count = count_csv_rows(filename)
            last_trade_str = (
                pd.Timestamp(last_trade_time, unit="s", tz=LOCAL_TZ).strftime(
                    "%H:%M:%S"
                )
                if last_trade_time
                else "None"
            )

            current_price = current_bar["Close"] if current_bar else "N/A"
            range_progress = "N/A"

            if current_bar and symbol_config:
                range_span = current_bar["range_max"] - current_bar["range_min"]
                range_progress = (
                    f"{(range_span / symbol_config['range_size'] * 100):.1f}%"
                )

            # Enhanced status message
            msg = (
                f"Status: {symbol} @ {current_price}, Range: {range_progress}, "
                f"Rows: {row_count}, Last trade: {last_trade_str}, "
                f"State: {connection_state.value}"
            )

            logger.info(msg)
            print_timestamped(msg, COLOR_MAGENTA)

            counter += 1
            if counter % 3 == 0:  # Every 9 minutes
                performance_monitor.log_stats()

                # Circuit breaker status
                if circuit_breaker_failures > 0:
                    logger.info(
                        f"Circuit breaker status: {circuit_breaker_failures} failures"
                    )

        except Exception as e:
            logger.error(f"Error in status thread: {e}")


def create_websocket_connection():
    """Create WebSocket connection with enhanced SSL configuration."""
    try:
        ssl_context = get_ssl_context()

        ws = websocket.WebSocketApp(
            websocket_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        debug_logger.debug(f"Created WebSocket app for URL: {websocket_url}")
        return ws, ssl_context

    except Exception as e:
        logger.error(f"Error creating WebSocket connection: {e}")
        return None, None


def websocket_loop():
    """Enhanced WebSocket loop with circuit breaker and adaptive reconnection."""
    global last_trade_time, last_message_time, last_close_time

    retry_count = 0
    connection_start_time = None
    consecutive_short_connections = 0

    while not shutdown_requested.is_set():
        try:
            # Check circuit breaker
            if not check_circuit_breaker():
                logger.info("Circuit breaker is OPEN, waiting before retry...")
                print_timestamped(
                    "Circuit breaker OPEN - waiting for reset", COLOR_YELLOW
                )
                time.sleep(30)  # Check every 30 seconds
                continue

            performance_monitor.record_connection_attempt()
            logger.info(f"WebSocket connection attempt #{retry_count + 1}")
            print_timestamped(
                f"WebSocket connection attempt #{retry_count + 1}", COLOR_BLUE
            )

            # Progressive delay with circuit breaker awareness
            if retry_count > 0:
                base_delay = min(
                    initial_retry_delay * (1.3 ** min(retry_count, 8)), 120
                )

                # Additional delay for consecutive short connections
                if consecutive_short_connections >= 3:
                    base_delay *= 2
                    logger.warning(
                        f"Multiple short connections detected, using extended delay: {base_delay:.1f}s"
                    )

                logger.info(f"Waiting {base_delay:.1f} seconds before reconnection...")
                print_timestamped(
                    f"Reconnection delay: {base_delay:.1f}s", COLOR_YELLOW
                )

                for _ in range(int(base_delay)):
                    if shutdown_requested.is_set():
                        return
                    time.sleep(1)

            # Reset state for new connection
            update_connection_state(ConnectionState.CONNECTING)
            last_message_time = time.time()
            connection_start_time = time.time()

            # Create WebSocket connection
            ws, ssl_context = create_websocket_connection()
            if ws is None:
                logger.error("Failed to create WebSocket connection")
                retry_count += 1
                continue

            # Start health monitoring thread
            health_thread = threading.Thread(target=enhanced_health_monitor, args=(ws,))
            health_thread.daemon = True
            health_thread.start()

            # Run WebSocket with enhanced configuration
            ws.run_forever(
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                skip_utf8_validation=False,  # Keep validation for data integrity
                sslopt={"context": ssl_context} if ssl_context else None,
            )

            # Analyze connection results
            connection_duration = (
                time.time() - connection_start_time if connection_start_time else 0
            )
            logger.info(f"Connection ended after {connection_duration:.1f} seconds")

            if connection_duration < 300:  # Less than 5 minutes indicates issues
                consecutive_short_connections += 1
                logger.warning(
                    f"Short connection duration. Consecutive short connections: {consecutive_short_connections}"
                )

                if consecutive_short_connections >= 5:
                    logger.error(
                        "Multiple consecutive short connections. Taking extended break."
                    )
                    print_timestamped(
                        "Multiple short connections - extended break needed", COLOR_RED
                    )
                    time.sleep(300)  # 5 minute break
                    consecutive_short_connections = 0

                retry_count += 1
            else:
                consecutive_short_connections = 0
                logger.info(f"Good connection duration: {connection_duration:.1f}s")
                retry_count = max(
                    0, retry_count - 2
                )  # Reduce retry count for good connections

            # Handle data gap if needed
            if not shutdown_requested.is_set() and last_close_time is not None:
                now_utc = pd.Timestamp.now(tz=UTC)
                gap_seconds = (now_utc - last_close_time).total_seconds()

                if gap_seconds > 120:  # 2 minute threshold
                    logger.info(f"Post-connection gap detected: {gap_seconds:.0f}s")
                    fill_data_gap_async(last_close_time, now_utc)

        except Exception as e:
            if shutdown_requested.is_set():
                logger.info(
                    "Shutdown requested during WebSocket error. Exiting cleanly."
                )
                break

            failure_type = classify_error(e)
            logger.error(f"Unexpected {failure_type.value} in WebSocket loop: {e}")
            print_timestamped(f"WebSocket loop error: {failure_type.value}", COLOR_RED)
            debug_logger.exception("WebSocket loop exception details")

            record_connection_failure(failure_type)
            retry_count += 1

            if retry_count >= max_retries:
                logger.critical(
                    f"Max retries ({max_retries}) reached. Circuit breaker will activate."
                )
                print_timestamped(
                    "Max retries reached - activating circuit breaker", COLOR_RED
                )
                time.sleep(60)  # Brief pause before circuit breaker logic takes over
                retry_count = 0  # Reset to allow circuit breaker to manage retries

        finally:
            update_connection_state(ConnectionState.DISCONNECTED)


def rotate_csv_file(filename, max_rows=150000):
    """Enhanced CSV rotation with better backup management."""
    try:
        current_rows = count_csv_rows(filename)
        if current_rows > max_rows:
            timestamp = int(time.time())
            backup_name = f"{filename}.backup.{timestamp}"

            # Create backup
            import shutil

            shutil.copy2(filename, backup_name)

            # Keep only recent rows in main file
            df = pd.read_csv(filename, parse_dates=["Open Time", "Close Time"])
            recent_df = df.tail(max_rows // 2)  # Keep half for continuity
            recent_df.to_csv(filename, index=False)

            logger.info(
                f"CSV rotated: {current_rows} → {len(recent_df)} rows, backup: {backup_name}"
            )
            print_timestamped(f"CSV rotated, backup created: {backup_name}", COLOR_BLUE)
            return True

    except Exception as e:
        logger.error(f"Error rotating CSV file: {e}")

    return False


if __name__ == "__main__":
    # Enhanced signal handling
    signal.signal(signal.SIGUSR1, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Initialize global state
        last_trade_time = time.time()
        last_message_time = time.time()
        last_close_time = None
        current_bar = None
        consecutive_trade_errors = 0
        circuit_breaker_failures = 0
        circuit_breaker_last_failure = None

        print_salutation()

        # Initialize symbol configuration
        logger.info("Fetching symbol configuration...")
        print_timestamped("Fetching symbol configuration...", COLOR_BLUE)
        symbol_config = get_symbol_info(symbol)

        if not symbol_config:
            logger.critical(
                f"Could not get configuration for symbol {symbol}. Exiting."
            )
            print_timestamped(
                f"Could not get configuration for symbol {symbol}. Exiting.", COLOR_RED
            )
            sys.exit(1)

        # Start gap fill worker thread
        gap_fill_thread = threading.Thread(target=gap_fill_worker, daemon=True)
        gap_fill_thread.start()
        logger.info("Gap fill worker thread started")

        # Check existing data and perform recovery
        logger.info("Checking existing data and performing recovery...")
        print_timestamped("Checking existing data...", COLOR_BLUE)

        now_utc = pd.Timestamp.now(tz=UTC)

        if os.path.exists(filename):
            archive_name = f"{filename}.archive.{uuid.uuid4().hex}"
            os.rename(filename, archive_name)
            logger.info(f"Archived existing CSV as {archive_name}")
            print_timestamped(f"Archived existing CSV as {archive_name}", COLOR_YELLOW)
        logger.info("Starting fresh data collection")
        print_timestamped("Starting fresh data collection", COLOR_GREEN)

        # Check if CSV rotation is needed
        if os.path.exists(filename):
            rotate_csv_file(filename)

        # Final status before starting WebSocket
        row_count = count_csv_rows(filename) if os.path.exists(filename) else 0
        range_size = symbol_config["range_size"]

        startup_complete_msg = (
            f"Setup complete. Starting WebSocket data collection.\n"
            f"Current rows: {row_count}, Range size: {range_size}, "
            f"Circuit breaker: {CIRCUIT_BREAKER_FAILURE_THRESHOLD} failure threshold"
        )

        logger.info(startup_complete_msg)
        print_timestamped(startup_complete_msg, COLOR_MAGENTA)

        # Start status monitoring thread
        status_thread = threading.Thread(target=enhanced_status_thread, daemon=True)
        status_thread.start()
        logger.info("Status monitoring thread started")

        # Start main WebSocket loop
        logger.info("Starting enhanced WebSocket data collection...")
        print_timestamped("Starting enhanced WebSocket data collection...", COLOR_BLUE)
        websocket_loop()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down gracefully...")
        print_timestamped(
            "Keyboard interrupt received. Shutting down gracefully...", COLOR_YELLOW
        )
        shutdown_requested.set()

    except Exception as e:
        logger.critical(f"Critical error during startup: {e}")
        print_timestamped(f"Critical startup error: {e}", COLOR_RED)
        debug_logger.exception("Critical startup exception details")

    finally:
        # Cleanup operations
        logger.info("Performing final cleanup...")
        print_timestamped("Performing final cleanup...", COLOR_YELLOW)

        # Save current state
        save_current_bar_state()

        # Signal gap fill worker to stop
        try:
            gap_fill_queue.put(None, timeout=1)
        except:
            pass

        # Final performance report
        performance_monitor.log_stats()

        # Final status
        final_row_count = count_csv_rows(filename) if os.path.exists(filename) else 0
        logger.info(f"Shutdown complete. Final CSV rows: {final_row_count}")
        print_timestamped(
            f"Shutdown complete. Final CSV rows: {final_row_count}", COLOR_WHITE
        )


def get_csv_last_modified_time(filename):
    """Get the last modified time of the CSV file with enhanced error handling."""
    try:
        if os.path.exists(filename):
            mtime = os.path.getmtime(filename)
            last_modified = pd.Timestamp(mtime, unit="s", tz=UTC)
            debug_logger.debug(f"CSV last modified time: {last_modified}")
            return last_modified
        return None
    except Exception as e:
        logger.error(f"Error getting last modified time for {filename}: {e}")
        return None
