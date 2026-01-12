from pyspark.sql.functions import col, when, lit, least

class ArbitrageCalculator:
    
    @staticmethod
    def buy_with_quote(asks, quote_amount):
        """
        Calculates how much base asset (base_qty) can be bought 
        with a specific quote amount (e.g., 500 USDT).
        Iterates through the 'asks' order book.
        """
        if asks is None:
            return (0.0, 0.0, 0.0, 0)

        remaining = float(quote_amount)
        base_qty = 0.0
        cost_used = 0.0
        levels_used = 0

        for lvl in asks:
            if remaining <= 1e-12:
                break
            if lvl is None:
                continue

            try:
                price = float(lvl["price"]) if lvl["price"] is not None else 0.0
                qty   = float(lvl["qty"])   if lvl["qty"]   is not None else 0.0
            except Exception:
                continue

            if price <= 0 or qty <= 0:
                continue

            level_cost = price * qty
            # Take either the whole level or just enough to spend remaining funds
            take_cost = level_cost if level_cost <= remaining else remaining
            take_qty = take_cost / price

            base_qty += take_qty
            cost_used += take_cost
            remaining -= take_cost
            levels_used += 1

        vwap = (cost_used / base_qty) if base_qty > 0 else 0.0
        return (base_qty, vwap, cost_used, levels_used)

    @staticmethod
    def sell_base_for_quote(bids, base_qty):
        """
        Calculates how much quote asset (USDT) is received 
        by selling a specific amount of base asset.
        Iterates through the 'bids' order book.
        """
        if bids is None:
            return (0.0, 0.0, 0)

        remaining = float(base_qty)
        revenue = 0.0
        sold = 0.0
        levels_used = 0

        for lvl in bids:
            if remaining <= 1e-12:
                break
            if lvl is None:
                continue

            try:
                price = float(lvl["price"]) if lvl["price"] is not None else 0.0
                qty   = float(lvl["qty"])   if lvl["qty"]   is not None else 0.0
            except Exception:
                continue

            if price <= 0 or qty <= 0:
                continue

            # Sell either the whole level volume or what we have left
            take_qty = qty if qty <= remaining else remaining
            
            revenue += take_qty * price
            sold += take_qty
            remaining -= take_qty
            levels_used += 1

        vwap = (revenue / sold) if sold > 0 else 0.0
        return (revenue, vwap, levels_used)
        
    @staticmethod
    def calculate_bbo_arbitrage(df, fee_rate):
        """
        Applies BBO arbitrage logic to a joined DataFrame.
        Expected columns: bin_ask, bin_bid, krk_ask, krk_bid, etc.
        """
        # 1. Binance -> Kraken Logic
        # Buy on Binance (Ask), Sell on Kraken (Bid)
        vol_b2k = least(col("bin_ask_q"), col("krk_bid_q"))
        revenue_b2k = col("krk_bid") * (1 - fee_rate)
        cost_b2k = col("bin_ask") * (1 + fee_rate)
        profit_b2k = vol_b2k * (revenue_b2k - cost_b2k)
        
        roi_b2k = (col("krk_bid") - col("bin_ask")) / col("bin_ask") * 100.0

        # 2. Kraken -> Binance Logic
        # Buy on Kraken (Ask), Sell on Binance (Bid)
        vol_k2b = least(col("krk_ask_q"), col("bin_bid_q"))
        revenue_k2b = col("bin_bid") * (1 - fee_rate)
        cost_k2b = col("krk_ask") * (1 + fee_rate)
        profit_k2b = vol_k2b * (revenue_k2b - cost_k2b)

        roi_k2b = (col("bin_bid") - col("krk_ask")) / col("krk_ask") * 100.0

        # 3. Selection (The "Signals" Logic)
        return df.withColumn("profit_usdt", when(profit_b2k > profit_k2b, profit_b2k).otherwise(profit_k2b)) \
                 .withColumn("roi_pct", when(roi_b2k > roi_k2b, roi_b2k).otherwise(roi_k2b)) \
                 .withColumn("direction", when(roi_b2k > roi_k2b, lit("Binance->Kraken")).otherwise(lit("Kraken->Binance"))) \
                 .withColumn("exec_volume", when(roi_b2k > roi_k2b, vol_b2k).otherwise(vol_k2b)) \
                 .filter(col("profit_usdt") > 0)