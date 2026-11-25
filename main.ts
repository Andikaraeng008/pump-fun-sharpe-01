// Pump.fun Sharpe Sprint — cleanest paper bot.
// Strategy: quiet-accumulation snipe + fast take-profit.
//
// What the kit's 24h dev window actually shows (measured directly from
// the frozen dataset, then confirmed by running the house grader):
//  - Fresh coins spike hard and then mean-revert: by the time a coin
//    reaches 2 SOL of real reserves its price is already +17% (median)
//    and the next 10 minutes return -12% (median). Momentum chasing,
//    dip buying and pullback entries all lose, in-sample and
//    out-of-sample, once the harness's real execution is modelled
//    (orders fill 2 slots late, at the post-trade reserves of that slot,
//    and fills worse than the slippage cap are rejected).
//  - The big "moonshots" (+1000% completions) are untradable: their whole
//    create-to-complete sequence happens inside ONE slot, so any order
//    decided on that batch fills after completion and is rejected.
//  - The least-bad edge left is to buy only when a coin is CALM: its last
//    10 trades span under 2% of price range, with at least 2 SOL of real
//    reserves and net buying. Those coins are near breakeven on price
//    before fees (win rate ~33%), so the bot keeps holds very short
//    (~2 minutes) and exits fast on a +15% take-profit, a -12% hard stop,
//    or a -15% trailing stop, and it refuses to chase: buys carry a 1%
//    slippage cap, so a fill that would land more than 1% above the
//    decision price is rejected for free and retried while the coin is
//    still calm. A mint counts as traded only once its buy actually
//    fills, so rejected orders never burn the one-entry-per-mint rule.
//  - Backtest (faithful replay of the dev window, 4x 6h windows):
//    -0.10 SOL / 24h, win rate 33%, 109 round trips, Sharpe/hour
//    +0.77 / -0.29 / +0.06 / -0.76 per window (2 of 4 windows
//    positive). The dev window is a hard regime for every long-only
//    bot tested (~40 variants, all negative); this is the closest to
//    breakeven and the only one with positive Sharpe windows, so it is
//    the entry with the best shot on the unseen final window.
//  - Size is capped by the harness at 1% of real reserves, so the bot
//    sizes each buy at min(0.05 SOL, 95% of the cap) and never below
//    0.01 SOL (the guardrail's round-trip floor; tx fees alone are ~1%
//    of a 0.01 SOL buy, so smaller is worse).
//
// Risk discipline: max 5 open positions (harness cap), one filled entry
// per mint, hard stop -12%, trailing stop -15% from peak, take-profit
// +15%, max hold ~2 minutes, never buy a coin that already ran +300%
// from creation, never buy when sellers outweigh buyers in cumulative
// volume, never trade a completed curve. Deterministic: no clock, no
// environment, no randomness; every decision comes from the
// hello/batch/orders/slot stream alone.

import { runBot, LAMPORTS_PER_SOL } from './vendor/makefun-bot.ts';

const SOL = LAMPORTS_PER_SOL; // 1_000_000_000n
const MAX_POSITIONS = 5;
const MIN_BUY = (SOL * 1n) / 100n; // 0.01 SOL: the guardrail's round-trip floor
const MAX_BUY = (SOL * 5n) / 100n; // 0.05 SOL per buy
const MIN_REAL_SOL = 2n * SOL; // need >= 2 SOL of real reserves to enter
const MAX_AGE_SLOTS = 4500; // ~30 min after creation (slot ~= 0.4 s)
const HOLD_SLOTS = 300; // ~2 min max hold
const HARD_STOP_NUM = 88; // exit at -12% from entry price
const TRAIL_NUM = 85; // exit at -15% from the peak since entry
const TAKE_PROFIT_NUM = 115; // exit at +15% from entry price
const MAX_RISE_NUM = 400; // never chase a coin already +300% from creation
const BUY_SLIPPAGE_BPS = 100; // 1%: refuse a buy that fills >1% above decision
const SELL_SLIPPAGE_BPS = 500; // 5% on exits
const QUIET_TRADES = 10; // calm window: last 10 trades
const QUIET_RANGE_NUM = 2; // ...spanning under 2% of price

interface MintStat {
  createdSlot: number;
  createPrice: number; // SOL per token at creation (or first sight)
  trades: number;
  buySol: bigint; // cumulative buy volume (lamports)
  sellSol: bigint; // cumulative sell volume (lamports)
  prices: number[]; // last QUIET_TRADES trade prices (SOL per token)
}

interface OpenTrade {
  entryPrice: number; // SOL per token actually paid (cost / tokens)
  peakPrice: number; // highest mark since entry
  buySlot: number; // slot the position first appeared in the account
}

function markPrice(curve: { virtual_quote_reserves: bigint; virtual_token_reserves: bigint }): number {
  const vt = curve.virtual_token_reserves;
  if (vt === 0n) return 0;
  return Number(curve.virtual_quote_reserves) / Number(vt);
}

await runBot({
  onBatch(state, batch) {
    const orders: { side: 'buy'; mint: string; quote_lamports: bigint; max_slippage_bps: number }[] = [];
    const sells: { side: 'sell'; mint: string; token_amount: bigint; max_slippage_bps: number }[] = [];
    const stats = (state as unknown as { __stats?: Map<string, MintStat> }).__stats;
    const open = (state as unknown as { __open?: Map<string, OpenTrade> }).__open;
    const traded = (state as unknown as { __traded?: Set<string> }).__traded;
    // Per-run scratch attached to the state object (never leaves the run).
    const st = stats ?? new Map<string, MintStat>();
    const op = open ?? new Map<string, OpenTrade>();
    const td = traded ?? new Set<string>();
    (state as unknown as { __stats?: Map<string, MintStat> }).__stats = st;
    (state as unknown as { __open?: Map<string, OpenTrade> }).__open = op;
    (state as unknown as { __traded?: Set<string> }).__traded = td;

    // Fold this batch's events into per-mint stats, in chain order.
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        if (!st.has(e.mint)) {
          st.set(e.mint, {
            createdSlot: e.slot,
            createPrice: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
            trades: 0,
            buySol: 0n,
            sellSol: 0n,
            prices: [],
          });
        }
      } else if (e.event === 'TradeEvent') {
        let s = st.get(e.mint);
        if (s === undefined) {
          s = {
            createdSlot: e.slot,
            createPrice: Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves),
            trades: 0,
            buySol: 0n,
            sellSol: 0n,
            prices: [],
          };
          st.set(e.mint, s);
        }
        s.trades += 1;
        if (e.is_buy) s.buySol += BigInt(e.sol_amount);
        else s.sellSol += BigInt(e.sol_amount);
        s.prices.push(Number(e.virtual_quote_reserves) / Number(e.virtual_token_reserves));
        if (s.prices.length > QUIET_TRADES) s.prices.shift();
      } else if (e.event === 'CompleteEvent') {
        st.delete(e.mint);
      }
    }

    // Sync open trades with the account: a position that appears is a new
    // fill (only a filled buy marks its mint as traded, so a rejected buy
    // can be retried while the coin is still calm); one that vanished was
    // closed (by us or by the harness).
    for (const [mint, p] of state.positions) {
      const held = op.get(mint);
      if (held === undefined) {
        const cost = Number(p.cost_lamports);
        const toks = Number(p.token_amount);
        const entry = toks > 0 ? cost / toks : 0;
        op.set(mint, { entryPrice: entry, peakPrice: entry, buySlot: batch.slot });
        td.add(mint);
      }
    }
    for (const mint of [...op.keys()]) {
      if (!state.positions.has(mint)) op.delete(mint);
    }

    // Exits first: every open trade, marked at the latest known reserves.
    const pendingSell = new Set<string>();
    for (const p of state.pending.values()) {
      if (p.order.side === 'sell') pendingSell.add(p.order.mint);
    }
    for (const [mint, t] of op) {
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete || pendingSell.has(mint)) continue;
      const px = markPrice(curve);
      if (px <= 0 || t.entryPrice <= 0) continue;
      if (px > t.peakPrice) t.peakPrice = px;
      const ageSlots = batch.slot - t.buySlot;
      const stopOut = px * 100 <= t.entryPrice * HARD_STOP_NUM;
      const tookProfit = px * 100 >= t.entryPrice * TAKE_PROFIT_NUM;
      const trailed = px * 100 <= t.peakPrice * TRAIL_NUM;
      if (stopOut || tookProfit || trailed || ageSlots >= HOLD_SLOTS) {
        const held = state.positions.get(mint);
        if (held !== undefined && held.token_amount > 0n) {
          sells.push({ side: 'sell', mint, token_amount: held.token_amount, max_slippage_bps: SELL_SLIPPAGE_BPS });
        }
      }
    }

    // Entries: one filled entry per mint, only while a slot is free, only
    // on calm, liquid coins with net buying that have not run away.
    const pendingBuy = new Set<string>();
    for (const p of state.pending.values()) {
      if (p.order.side === 'buy') pendingBuy.add(p.order.mint);
    }
    if (state.committedPositionCount() < MAX_POSITIONS) {
      for (const e of batch.events) {
        if (e.event !== 'TradeEvent') continue;
        if (state.committedPositionCount() + orders.length >= MAX_POSITIONS) break;
        const mint = e.mint;
        if (td.has(mint) || op.has(mint) || pendingBuy.has(mint)) continue;
        const curve = state.curves.get(mint);
        const s = st.get(mint);
        if (curve === undefined || curve.complete || s === undefined) continue;
        if (s.trades < QUIET_TRADES) continue;
        if (batch.slot - s.createdSlot > MAX_AGE_SLOTS) continue;
        if (curve.real_quote_reserves < MIN_REAL_SOL) continue;
        if (s.buySol <= s.sellSol) continue;
        // Calm: the last 10 trades span under 2% of price.
        let lo = Number.POSITIVE_INFINITY;
        let hi = 0;
        for (const p of s.prices) {
          if (p < lo) lo = p;
          if (p > hi) hi = p;
        }
        if (s.prices.length < QUIET_TRADES || lo <= 0) continue;
        if ((hi - lo) * 100 >= lo * QUIET_RANGE_NUM) continue;
        const px = markPrice(curve);
        if (px <= 0 || s.createPrice <= 0) continue;
        if (px * 100 > s.createPrice * MAX_RISE_NUM) continue;
        // Size: as much as the 1%-of-real-reserve cap allows, up to 0.05
        // SOL, with a 5% safety margin under the cap so the harness never
        // rejects the order for size.
        const cap = state.maxBuyLamports(mint);
        let size = cap * 95n / 100n;
        if (size > MAX_BUY) size = MAX_BUY;
        if (size < MIN_BUY) continue;
        if (state.freeCashLamports() - size - state.txFeeLamports() <= 0n) continue;
        orders.push({ side: 'buy', mint, quote_lamports: size, max_slippage_bps: BUY_SLIPPAGE_BPS });
      }
    }

    return [...sells, ...orders];
  },
});
