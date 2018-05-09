import numpy as np
import pandas as pd
from scipy import stats


def slope(ts):
    x = np.arange(len(ts))
    log_ts = np.log(ts)
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, log_ts)
    annualized_slope = (np.power(np.exp(slope), 250) - 1) * 100
    return annualized_slope * (r_value ** 2)

class MarketCap(CustomFactor):
    inputs = [USEquityPricing.close, morningstar.valuation.shares_outstanding]
    window_length = 1

    def compute(self, today, assets, out, close, shares):
        out[:] = close[-1] * shares[-1]

    # Creating a filter for all the stocks within the market cap requirements
def make_pipeline(context,sma_window_length, market_cap_limit):
    pipe = Pipeline()

    # Now only stocks in the top N largest companies by market cap
    market_cap = MarketCap()
    top_N_market_cap = market_cap.top(market_cap_limit)

    #Other filters to make sure we are getting a clean universe. Primary and domestic shares only.
    is_primary_share = morningstar.share_class_reference.is_primary_share.latest
    is_not_adr = ~morningstar.share_class_reference.is_depositary_receipt.latest

    #### TREND FITLER ###########
    #### If current stock price is bellow sma_window_length(100) moving average price, do not buy.

    if context.use_stock_trend_filter:
        latest_price = USEquityPricing.close.latest
        sma = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=sma_window_length)
        above_sma = (latest_price > sma)
        initial_screen = (above_sma & top_N_market_cap & is_primary_share & is_not_adr)
        log.info("Init: Stock trend filter ON")
    else: #### TREND FITLER OFF  ##############
        initial_screen = (top_N_market_cap & is_primary_share & is_not_adr)
        log.info("Init: Stock trend filter OFF")

    pipe.add(market_cap, "market_cap")

    pipe.set_screen(initial_screen)

    return pipe

    # Create pipeline before market open of tradeable stocks.
def before_trading_start(context, data):
    context.selected_universe = pipeline_output('screen')
    context.assets = context.selected_universe.index

def initialize(context):
    context.market = sid(8554)                      # Stock or fund used for market filter.
    context.market_window = 100                     # Period lookback for market filter.
    context.atr_window = 20                         # Period lookback for ATR calculation.
    context.talib_window = context.atr_window + 5   # Used in position sizing for ATR calculation. Not sure what +5 does.
    context.risk_factor = 0.01                     # 0.01 = less position, more % but more risk. Take this out and replace with my method.#######

    context.momentum_window_length = 60             # Period lookback for momentum calculations.
    context.market_cap_limit = 500                  # Limit number of stocks based on market cap.
    context.rank_table_percentile = .3             # Top .x of stocks on ranking table.
    context.significant_position_difference = 0.1   # Rebalance only if target weight is greater than .x difference.
    context.min_momentum = 0.000                    # Minimum adjusted momentum requirement.
    context.leverage_factor = 1.0                   # 1=2154%. Guy's version is 1.4=3226%. Determines how much leverage is used.
    context.use_stock_trend_filter = 0              # Either 0 = Off, 1 = On.
    context.sma_window_length = 200                 # Used for the stock trend filter.
    context.use_market_trend_filter = 1             # Either 0 = Off, 1 = On. Filter on SPY. 0 wont work!
    context.use_average_true_range = 1              # Either 0 = Off, 1 = On. Manage risk with individual stock volatility.
    context.average_true_rage_multipl_factor = 1    # Change the weight of the ATR. 1327%.

    # Bring up pipeline for display as a screen.
    attach_pipeline(make_pipeline(context, context.sma_window_length,
                                  context.market_cap_limit), 'screen')

    # Schedule my rebalance function
    schedule_function(rebalance,
                      date_rules.month_start(),
                      time_rules.market_open(hours=1))

    # Cancel all open orders at the end of each day.
    schedule_function(cancel_open_orders, date_rules.every_day(), time_rules.market_close())
    set_slippage(slippage.FixedSlippage(spread=0.00))

def cancel_open_orders(context, data):
    open_orders = get_open_orders()
    for security in open_orders:
        for order in open_orders[security]:
            cancel_order(order)

    #record(lever=context.account.leverage,
    record(exposure=context.account.leverage)

def handle_data(context, data):
    pass

def rebalance(context, data):
    highs = data.history(context.assets, "high", context.talib_window, "1d")
    lows = data.history(context.assets, "low", context.talib_window, "1d")
    closes = data.history(context.assets, "price", context.market_window, "1d")

    estimated_cash_balance = context.portfolio.cash
    slopes = closes[context.selected_universe.index].tail(context.momentum_window_length).apply(slope)

    print("Made it")
    print(slopes.order(ascending=False).head(10))
    slopes = slopes[slopes > context.min_momentum]
    ranking_table = slopes[slopes > slopes.quantile(1 - context.rank_table_percentile)].order(ascending=False)
    log.info( len(ranking_table.index))
    # close positions that are no longer in the top of the ranking table
    positions = context.portfolio.positions
    for security in positions:
        price = data.current(security, "price")
        position_size = positions[security].amount
        if data.can_trade(security) and security not in ranking_table.index:
            order_target(security, 0, style=LimitOrder(price))
            estimated_cash_balance += price * position_size
        elif data.can_trade(security):
            new_position_size = get_position_size(context, highs[security], lows[security], closes[security],security)
            if significant_change_in_position_size(context, new_position_size, position_size):
                estimated_cost = price * (new_position_size * context.leverage_factor - position_size)
                order_target(security, new_position_size * context.leverage_factor, style=LimitOrder(price))
                estimated_cash_balance -= estimated_cost


    # Market history is not used with the trend filter disabled
    # Removed for efficiency
    if context.use_market_trend_filter:
        market_history = data.history(context.market, "price", context.market_window, "1d")  ##SPY##
        current_market_price = market_history[-1]
        average_market_price = market_history.mean()
    else:
        average_market_price = 0

    if (current_market_price > average_market_price) :  #if average is 0 then jump in
        for security in ranking_table.index:
            if data.can_trade(security) and security not in context.portfolio.positions:
                new_position_size = get_position_size(context, highs[security], lows[security], closes[security],
                                                     security)
                estimated_cost = data.current(security, "price") * new_position_size * context.leverage_factor
                if estimated_cash_balance > estimated_cost:
                    order_target(security, new_position_size * context.leverage_factor, style=LimitOrder(data.current(security, "price")))
                    estimated_cash_balance -= estimated_cost


    # Original position sizing is based off of ATR. Use my new method here #######
def get_position_size(context, highs, lows, closes, security):
    try:
        average_true_range = talib.ATR(highs.ffill().dropna().tail(context.talib_window),
                                       lows.ffill().dropna().tail(context.talib_window),
                                       closes.ffill().dropna().tail(context.talib_window),
                                       context.atr_window)[-1] # [-1] gets the last value, as all talib methods are rolling calculations#
      #  if not context.use_average_true_range: #average_true_range
       #     average_true_range = 1 #divide by 1 gives... same initial number
        #    context.average_true_rage_multipl_factor = 1

        return (context.portfolio.portfolio_value * context.risk_factor)  / (average_true_range * context.average_true_rage_multipl_factor)
    except:
        log.warn('Insufficient history to calculate risk adjusted size for {0.symbol}'.format(security))
        return 0


def significant_change_in_position_size(context, new_position_size, old_position_size):
    return np.abs((new_position_size - old_position_size)  / old_position_size) > context.significant_position_difference
