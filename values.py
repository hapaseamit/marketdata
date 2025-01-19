headers = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 "
        + "Safari/537.36 Edg/108.0.1462.76"
    ),
    "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,mr;q=0.7",
    "accept-encoding": "gzip, deflate, br",
}

website = "https://www.nseindia.com/"

strikes = {
    "ce_price": 23200,
    "pe_price": 23200,
    "expiryDate": "16-Jan-2025",
}

tasks = {
    "nifty_opt": {
        "rest_url": "api/liveEquity-derivatives?index=nse50_opt",
        "csv_columns": [
            "time",
            "niftyoptcevolume",
            "niftyoptpevolume",
            "niftyoptceoi",
            "niftyoptpeoi",
        ],
        "symbol": "niftyopt",
        "strikes": strikes,
    },
    "nifty_chain": {
        "rest_url": "api/option-chain-indices?symbol=NIFTY",
        "csv_columns": [
            "time",
            "niftychaincevolume",
            "niftychainpevolume",
            "niftychainceoi",
            "niftychainpeoi",
        ],
        "symbol": "niftychain",
        "strikes": strikes,
    },
}
