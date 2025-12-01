#!/usr/bin/env python3
"""
Trading Bot Entry Point

Run this script to start the trading bot.
"""

import sys
from src.trading_bot.bot import TradingBot


def main():
    """Main entry point."""
    print("=" * 70)
    print("AI Trading Bot")
    print("=" * 70)
    
    # Create bot instance
    bot = TradingBot()
    
    # Initialize
    if not bot.initialize():
        print("❌ Failed to initialize bot. Check logs.")
        sys.exit(1)
    
    # Start
    if not bot.start():
        print("❌ Failed to start bot. Check logs.")
        sys.exit(1)
    
    # Run main loop
    try:
        bot.run()
    except KeyboardInterrupt:
        print("\n🛑 Received interrupt signal, shutting down...")
    finally:
        bot.stop()
        print("✓ Bot stopped successfully")


if __name__ == "__main__":
    main()
