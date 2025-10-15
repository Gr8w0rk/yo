#!/usr/bin/env node
/**
 * Crypto Top Gainers Tracker – SUPER REAL-TIME with ENHANCED AUTO-TRADING
 * - Instant push on every new mint & price tick (no polling ticks for UI)
 * - Top list recomputed immediately after each change
 * - Enhanced auto-trading with dynamic rules, trailing stop-loss, and real-time execution
 * - ULTRA-FAST P&L updates for immediate visual feedback
 * ---
 * V2 UPDATE: Now tracks coins post-graduation.
 * V3 UPDATE: Full auto-trading with configurable buy/sell rules
 */

import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import express from 'express';
import { WebSocketServer } from 'ws';
import cron from 'node-cron';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';
import bs58 from 'bs58';
import http from 'http';
import { createPumpTradingService } from './pumpTrading.mjs';
import {
  getAssociatedTokenAddress,
  TOKEN_PROGRAM_ID,
  ASSOCIATED_TOKEN_PROGRAM_ID,
} from '@solana/spl-token';

import {
  Connection,
  PublicKey,
  Keypair,
  LAMPORTS_PER_SOL,
  VersionedTransaction,
  SendTransactionError
} from '@solana/web3.js';

// ---------- Setup File-based Logging ----------
const __filename_log = fileURLToPath(import.meta.url);
const __dirname_log = path.dirname(__filename_log);

// Create a 'logs' directory if it doesn't exist
const logDir = path.join(__dirname_log, 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
  console.log(`Created logging directory at: ${logDir}`);
}

// Create a unique log file for this server session
const logTimestamp = new Date().toISOString().replace(/[:.]/g, '-');
const logFileName = `server-run-${logTimestamp}.log`;
const logStream = fs.createWriteStream(path.join(logDir, logFileName), { flags: 'a' });

// Redirect console.log and console.error to the log file and the original console
const originalLog = console.log;
const originalError = console.error;

console.log = function(...args) {
    const message = args.map(arg => (typeof arg === 'object' && arg !== null) ? JSON.stringify(arg, null, 2) : String(arg)).join(' ');
    logStream.write(`[INFO] ${new Date().toISOString()}: ${message}\n`);
    originalLog.apply(console, args);
};

console.error = function(...args) {
    const message = args.map(arg => (typeof arg === 'object' && arg !== null) ? JSON.stringify(arg, null, 2) : String(arg)).join(' ');
    logStream.write(`[ERROR] ${new Date().toISOString()}: ${message}\n`);
    originalError.apply(console, args);
};

console.log(`Logging for this session is being saved to: ${logFileName}`);

const trackingDataFileName = 'tracking-data.json';
console.log(`Tracking data for this session will be saved to: ${trackingDataFileName}`);

const priceTicksFileName = `price-ticks-${logTimestamp}.jsonl`; // Use .jsonl extension
const priceLogStream = fs.createWriteStream(path.join(logDir, priceTicksFileName), { flags: 'a' });

console.log(`Price tick data for this session will be saved to: ${priceTicksFileName}`);

// ---------- Environment Variables ----------
const {
  SOLANA_RPC_URL,
  SOLANA_WS_URL,
  SOL_PRICE_USD_FALLBACK,
  WEB_PORT = '8787',

  PUMP_FUN_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P',
  PUMP_FUN_AMM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA',
  GRPC_ADDR,
  GEYSER_PROTO_DIR,

  TOP_COINS_LIMIT = '10',
  MIN_MARKET_CAP = '1000',
  MAX_COINS_TRACKED = '1000',

  // Trading configuration
  PRIVATE_KEY,
  SLIPPAGE_PERCENT = '5',
  MAX_SOL_PER_TRADE = '0.1'
} = process.env;

const DEFAULT_SLIPPAGE_PERCENT = Number.isFinite(Number(SLIPPAGE_PERCENT))
  ? Number(SLIPPAGE_PERCENT)
  : 5;

const PUMPSWAP_PROGRAM_ID_STRING = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

if (!SOLANA_RPC_URL) {
  console.error('❌ Missing SOLANA_RPC_URL in .env');
  process.exit(1);
}

// ---------- Price Calculation Constants ----------
const PUMP_CURVE_TOKEN_DECIMALS = 6;
// 8-byte discriminator at offset 0 in Pump bonding-curve account
const PUMP_CURVE_STATE_SIGNATURE = new Uint8Array([0x17,0xb7,0xf8,0x37,0x60,0xd8,0xac,0x60]);

// ---------- Pyth Pull Oracle Configuration ----------
const PYTH_HERMES_API = 'https://hermes.pyth.network';
const SOL_USD_PRICE_ID = 'ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d'; // SOL/USD price feed ID

// State variables to hold the live price
let solPriceUSD = parseFloat(SOL_PRICE_USD_FALLBACK); // Start with fallback
let lastPriceUpdate = 0;

// ---------- Solana Connection & Wallet ----------
const connection = new Connection(SOLANA_RPC_URL, {
  commitment: 'processed',
  wsEndpoint: SOLANA_WS_URL
});

const PUMP_FUN_PROGRAM_ID = new PublicKey(PUMP_FUN_PROGRAM);
const PUMPSWAP_PROGRAM_ID = new PublicKey(PUMPSWAP_PROGRAM_ID_STRING);
const TOKEN_METADATA_PROGRAM_ID = new PublicKey('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s');
const WSOL_MINT = 'So11111111111111111111111111111111111111112';

let wallet = null;
if (PRIVATE_KEY && PRIVATE_KEY.trim()) {
  try {
    const sk = bs58.decode(PRIVATE_KEY.trim());
    wallet = Keypair.fromSecrlet tradingService = null;
if (wallet) {
  try {
    tradingService = createPumpTradingService({
      connection,
      wallet,
      pumpProgramId: PUMP_FUN_PROGRAM_ID,
      pumpswapProgramId: PUMPSWAP_PROGRAM_ID,
    });
    console.log('[trading] Pump trading service initialized');
    
    // Test RPC connection
    tradingService.testRPCConnection().then(() => {
      console.log('[trading] ✅ RPC connection test passed');
    }).catch((error) => {
      console.error('[trading] ❌ RPC connection test failed:', error.message);
    });
  } catch (e) {
    console.error('[trading] failed to init trading service:', e.message);
  }
}   console.log('[trading] Pump trading service initialized');
  } catch (e) {
    console.error('[trading] failed to init trading service:', e.message);
  }
}

// ---------- Auto-Trading State ----------
const autoTradingPositions = new Map(); // mint -> position config and state
const manualPositions = new Map(); // mint -> manually tracked position state

// ---------- State Management ----------
/** @type {Map<string, {
 * mint:string,name:string,symbol:string,
 * launchTime:number,launchPrice:number,launchMarketCap:number,
 * currentPrice:number,marketCapUSD:number,highestPrice:number,
 * lastUpdated:number,resolved:boolean,priceHistory:Array<{price:number,timestamp:number}>,
 * status: 'pump' | 'graduating' | 'raydium' | 'failed',
 * ammId: string | null,
 * lastTxSig: string | null,
 * graduationMarketCap: number | undefined // To store the market cap at graduation
 * }>} */
const trackedCoins = new Map(); // mint -> coin data
const seenMints = new Set();
let topCoinsLimit = parseInt(TOP_COINS_LIMIT);
const MIN_MCAP = parseInt(MIN_MARKET_CAP);
const MAX_TRACKED = parseInt(MAX_COINS_TRACKED);

// Real-time price monitoring for auto-trading
const realTimePriceMonitors = new Map(); // mint -> { subscriptionId: number, type: 'pump' | 'raydium' }
const priceUpdateIntervals = new Map(); // mint -> interval for ultra-fast updates
const MAX_RECENT_TRANSACTIONS = 250;
const recentTransactions = [];
const seenTransactionSignatures = new Set();
const pendingTransactionFetches = new Set();
const transactionLogSubscriptions = [];

/**
 * Reads a JSON Lines (.jsonl) file, groups the price ticks by coin,
 * combines them with the final coin metadata, and saves the result as a
 * standard, grouped JSON file.
 * @param {string} jsonlFilePath - The path to the input .jsonl file.
 * @param {string} jsonFilePath - The path to the output .json file.
 * @param {Map} finalTrackedCoins - The live trackedCoins Map from the application.
 */
function convertJsonlToJson(jsonlFilePath, jsonFilePath, finalTrackedCoins) {
  try {
    console.log(`[shutdown-convert] Starting grouped conversion of ${jsonlFilePath}...`);

    const fileContent = fs.readFileSync(jsonlFilePath, 'utf8');
    const lines = fileContent.split('\n').filter(line => line.trim() !== '');

    if (lines.length === 0) {
        console.log('[shutdown-convert] No price ticks were logged, skipping conversion.');
        return;
    }

    const ticks = lines.map(line => JSON.parse(line));

    // 1. Group all price ticks from the log file by their mint address
    const priceHistoryByMint = new Map();
    for (const tick of ticks) {
        if (!priceHistoryByMint.has(tick.mint)) {
            priceHistoryByMint.set(tick.mint, []);
        }
        // We only need the price and timestamp for the history array
        priceHistoryByMint.get(tick.mint).push({
            price: tick.priceUSD,
            timestamp: tick.timestamp
        });
    }

    // 2. Combine the full price history with the final coin metadata
    const finalOutput = [];
    for (const [mint, coinData] of finalTrackedCoins.entries()) {
        const completePriceHistory = priceHistoryByMint.get(mint) || coinData.priceHistory;

        // Create the final object for this coin
        const finalCoinObject = {
            ...coinData, // Copy all existing metadata (launchPrice, status, etc.)
            priceHistory: completePriceHistory // Overwrite with the complete history
        };
        finalOutput.push(finalCoinObject);
    }

    // 3. Write the final, grouped array to the .json file
    fs.writeFileSync(jsonFilePath, JSON.stringify(finalOutput, null, 2));

    console.log(`[shutdown-convert] ✅ Successfully converted and grouped data for ${finalOutput.length} coins to ${jsonFilePath}`);
  } catch (error) {
    console.error(`[shutdown-convert] ❌ Failed to convert .jsonl to .json:`, error.message);
  }
}

/**
 * Fetch real-time SOL/USD price from Pyth's Hermes API (Pull Oracle)
 */
async function fetchRealTimeSOLPrice() {
  try {
    // Fetch from Pyth Hermes API
    const response = await fetch(`${PYTH_HERMES_API}/v2/updates/price/latest?ids[]=${SOL_USD_PRICE_ID}`);

    if (!response.ok) {
      throw new Error(`Pyth API responded with ${response.status}`);
    }

    const data = await response.json();
    if (!data.parsed || !data.parsed.length) {
      throw new Error('No price data returned from Pyth API');
    }

    const priceData = data.parsed[0];
    const price = priceData.price.price;
    const expo = priceData.price.expo;
    
    // Calculate the actual price: price * 10^expo
    const actualPrice = parseFloat(price) * Math.pow(10, expo);
    
    // Sanity check
    if (actualPrice < 1 || actualPrice > 10000) {
      throw new Error(`Unrealistic SOL price: $${actualPrice}`);
    }

    return actualPrice;

  } catch (error) {
    console.error(`[pyth-oracle] ❌ Pyth fetch failed:`, error.message);
    return null; // Return null on failure
  }
}

/**
 * Alternative price source as backup (CoinGecko API)
 */

async function fetchBackupSOLPrice() {
  try {
    console.log('[backup-price] Fetching SOL price from CoinGecko...');
    const response = await fetch('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd');
    const data = await response.json();
    const price = data?.solana?.usd;
    
    if (!price) throw new Error('Invalid backup price data');
    
    console.log(`[backup-price] ✅ CoinGecko SOL/USD: $${price.toFixed(4)}`);
    return price;
    
  } catch (error) {
    console.error('[backup-price] ❌ Failed:', error.message);
    return null;
  }
}

/**
 * Smart price fetching with fallback chain.
 * This is the main function you will call.
 */

async function updateSOLPriceWithFallback() {
  let newPrice = await fetchRealTimeSOLPrice(); // Try Pyth first
  
  if (!newPrice) {
    console.log('[price-update] Primary source failed, trying backup...');
    newPrice = await fetchBackupSOLPrice(); // Try CoinGecko if Pyth fails
  }

  if (newPrice) {
    // If the price has changed, broadcast it to the UI
    if (Math.abs(newPrice - solPriceUSD) > 0.01) {
       broadcast({
          type: 'solPriceUpdate',
          solPriceUSD: newPrice,
          updateTime: Date.now()
        });
    }
    solPriceUSD = newPrice;
    lastPriceUpdate = Date.now();
  } else {
    console.error('[price-update] ❌ All price sources failed. Using last known price.');
  }
}

//PumpSwap

/**
 * Calculates the price of a token in a PumpSwap/pumpFunAmm pool by fetching
 * the balances of its two token vaults.
 * @param {PublicKey} baseVault The public key of the vault holding the base token (memecoin).
 * @param {PublicKey} quoteVault The public key of the vault holding the quote token (SOL).
 * @returns {Promise<{priceSOL: number, priceUSD: number, marketCapUSD: number}|null>}
 */
async function getPriceFromVaults(baseVault, quoteVault) {
  try {
    const [baseBalance, quoteBalance] = await Promise.all([
      connection.getTokenAccountBalance(baseVault),
      connection.getTokenAccountBalance(quoteVault)
    ]);

    if (!baseBalance.value.uiAmount || !quoteBalance.value.uiAmount) {
      return null;
    }
    
    const baseReserves = baseBalance.value.uiAmount;
    const quoteReserves = quoteBalance.value.uiAmount;

    if (baseReserves === 0) return null;

    const priceInSol = quoteReserves / baseReserves;
    const priceUSD = priceInSol * solPriceUSD;
    const marketCapUSD = priceInSol * 1_000_000_000 * solPriceUSD;

    return {
      priceSOL: priceInSol,
      priceUSD: priceUSD,
      marketCapUSD: marketCapUSD,
      timestamp: Date.now()
    };
  } catch (error) {
    // It's common for this to fail briefly if an account is not yet initialized
    return null;
  }
}
/**
 * Sets up real-time monitoring for a graduated coin by listening to its token vaults.
 * This is more robust than parsing the AMM account state.
 * @param {string} mint The token mint address.
 * @param {string} baseVaultAddr The address of the base token vault.
 * @param {string} quoteVaultAddr The address of the quote token vault.
 */
function setupVaultPriceMonitoring(mint, baseVaultAddr, quoteVaultAddr) {
  if (!baseVaultAddr || !quoteVaultAddr) {
    console.warn(`[price-monitor] Skipping PumpSwap monitor for ${mint}: missing vault addresses`);
    return;
  }

  const existing = realTimePriceMonitors.get(mint);
  if (existing) {
    if (existing.type === 'pumpswap-vaults') {
      return;
    }
    try {
      const subs = Array.isArray(existing.subscriptionId) ? existing.subscriptionId : [existing.subscriptionId];
      subs.filter((id) => typeof id === 'number').forEach((id) => {
        connection.removeAccountChangeListener(id);
      });
    } catch (removeErr) {
      console.error(`[price-monitor] Failed to clear previous monitor for ${mint.slice(0, 8)}:`, removeErr.message);
    } finally {
      realTimePriceMonitors.delete(mint);
    }
  }

  let baseVault;
  let quoteVault;
  try {
    baseVault = new PublicKey(baseVaultAddr);
    quoteVault = new PublicKey(quoteVaultAddr);
  } catch (e) {
    console.error(`[price-monitor] Invalid vault address for ${mint.slice(0, 8)}:`, e.message);
    return;
  }

  const handleVaultChange = async () => {
    const coin = trackedCoins.get(mint);
    if (!coin) return;

    const parsed = await getPriceFromVaults(baseVault, quoteVault);
    if (!parsed || !parsed.priceUSD) return;

    // Create a structured object for the price tick
    const tickData = {
        timestamp: Date.now(),
        mint: mint,
        name: coin.name,
        symbol: coin.symbol,
        priceUSD: parsed.priceUSD,
        marketCapUSD: parsed.marketCapUSD
    };

    priceLogStream.write(JSON.stringify(tickData) + '\n');
    
    // Update coin state
    coin.currentPrice = parsed.priceUSD;
    coin.marketCapUSD = parsed.marketCapUSD;
    coin.lastUpdated = Date.now();
    
    if (!coin.priceHistory) coin.priceHistory = [];
    coin.priceHistory.push({ price: parsed.priceUSD, timestamp: Date.now() });
    if (coin.priceHistory.length > 100) {
      coin.priceHistory = coin.priceHistory.slice(-100);
    }
    
    if (parsed.priceUSD > (coin.highestPrice || 0)) {
      coin.highestPrice = parsed.priceUSD;
    }

    // Check auto-trading and manual position rules
    await checkAutoTradingRules(mint, parsed.priceUSD);
    await evaluateManualPosition(mint, parsed.priceUSD);
    
    // Broadcast update
    broadcast({
      type: 'priceUpdate',
      coin: {
        mint,
        currentPrice: parsed.priceUSD,
        gainPercent: computeGainPercent(parsed.priceUSD, coin.launchPrice),
        marketCapUSD: parsed.marketCapUSD,
        status: coin.status
      }
    });
    
    pushFullTopGainers();
  };

 // Subscribe to both vaults
  const sub1 = connection.onAccountChange(baseVault, handleVaultChange, 'processed');
  const sub2 = connection.onAccountChange(quoteVault, handleVaultChange, 'processed');

  realTimePriceMonitors.set(mint, { subscriptionId: [sub1, sub2], type: 'pumpswap-vaults' });
  console.log(`[price-monitor] ⚡ Monitoring PumpSwap vaults for ${mint.slice(0,8)}`);
  
  // Trigger initial price fetch
  setTimeout(handleVaultChange, 1000);
}

/**
 * Process coin graduation from pump.fun to PumpSwap
 */
async function processGraduation(mint, baseVaultAddr, quoteVaultAddr, poolAddress = null) {
  const coin = trackedCoins.get(mint);
  if (!coin) return;

  const eligibleStatuses = new Set(['pump', 'graduating', 'pumpswap']);
  if (!eligibleStatuses.has(coin.status)) {
    return;
  }

  const alreadyGraduated = coin.status === 'pumpswap';

  console.log(`[graduation] 🎓 Processing graduation for ${coin.symbol || mint}`);
  console.log(`[graduation]   Base Vault: ${baseVaultAddr}`);
  console.log(`[graduation]   Quote Vault: ${quoteVaultAddr}`);

  // Capture the market cap at the moment of graduation
  if (!alreadyGraduated) {
    coin.graduationMarketCap = coin.marketCapUSD;
  }

  // Update coin status
  coin.status = 'pumpswap';
  coin.baseVault = baseVaultAddr;
  coin.quoteVault = quoteVaultAddr;
  if (poolAddress) {
    coin.ammId = poolAddress;
  }
  coin.lastUpdated = Date.now();

  // Remove pump.fun monitoring
  const monitor = realTimePriceMonitors.get(mint);
  if (monitor) {
    try {
      if (Array.isArray(monitor.subscriptionId)) {
        monitor.subscriptionId.forEach(subId => {
          connection.removeAccountChangeListener(subId);
        });
      } else {
        connection.removeAccountChangeListener(monitor.subscriptionId);
      }
      realTimePriceMonitors.delete(m/**
 * Find PumpSwap pool data for a given mint
 */
async function findPumpSwapPool(mint) {
  try {
    // First try to derive the pool PDA
    const poolPDA = derivePumpSwapPoolPDA(mint);
    const poolAccount = await connection.getAccountInfo(poolPDA, 'processed');
    
    if (poolAccount) {
      console.log(`[pool-discovery] Found PumpSwap pool PDA for ${mint} at ${poolPDA.toBase58()}`);
      
      // Derive vault PDAs
      const baseVault = derivePumpSwapTokenVaultPDA(poolPDA.toBase58());
      const quoteVault = derivePumpSwapSolVaultPDA(poolPDA.toBase58());
      
      return {
        poolAddress: poolPDA.toBase58(),
        baseVault: baseVault.toBase58(),
        quoteVault: quoteVault.toBase58(),
      };
    }

    // Fallback to program accounts search
    const baseMint = new PublicKey(mint);
    const filters = [
      {
        memcmp: {
          offset: 43,
          bytes: baseMint.toBase58(),
        },
      },
    ];

    const accounts = await connection.getProgramAccounts(PUMPSWAP_PROGRAM_ID, {
      commitment: 'processed',
      filters,
    });

    if (!accounts.length) {
      console.log(`[pool-discovery] No PumpSwap pool found yet for ${mint}`);
      return null;
    }

    const account = accounts[0];
    const data = account.account?.data;
    if (!data || data.length < 203) {
      console.warn(`[pool-discovery] PumpSwap pool account for ${mint} is missing expected data length`);
      return null;
    }

    const baseVaultOffset = 139;
    const quoteVaultOffset = 171;

    const baseVault = new PublicKey(data.slice(baseVaultOffset, baseVaultOffset + 32)).toBase58();
    const quoteVault = new PublicKey(data.slice(quoteVaultOffset, quoteVaultOffset + 32)).toBase58();
    const poolAddress = account.pubkey.toBase58();

    console.log(`[pool-discovery] Found PumpSwap pool for ${mint} at ${poolAddress}`);

    return {
      poolAddress,
      baseVault,
      quoteVault,
    };
  } catch (error) {
    console.error(`[pool-discovery] Error finding pool for ${mint}:`, error.message);
    return null;
  }
}

/**
 * Derive PumpSwap pool PDA for a given token mint
 */
function derivePumpSwapPoolPDA(tokenMint) {
  const mint = new PublicKey(tokenMint);
  return PublicKey.findProgramAddressSync(
    [Buffer.from('pool'), mint.toBuffer()],
    PUMPSWAP_PROGRAM_ID
  )[0];
}

/**
 * Derive PumpSwap pool token vault PDA
 */
function derivePumpSwapTokenVaultPDA(poolAddress) {
  const pool = new PublicKey(poolAddress);
  return PublicKey.findProgramAddressSync(
    [Buffer.from('token_vault'), pool.toBuffer()],
    PUMPSWAP_PROGRAM_ID
  )[0];
}

/**
 * Derive PumpSwap pool SOL vault PDA
 */
function derivePumpSwapSolVaultPDA(poolAddress) {
  const pool = new PublicKey(poolAddress);
  return PublicKey.findProgramAddressSync(
    [Buffer.from('sol_vault'), pool.toBuffer()],
    PUMPSWAP_PROGRAM_ID
  )[0];
}9;
    const quoteVaultOffset = 171;

    const baseVault = new PublicKey(data.slice(baseVaultOffset, baseVaultOffset + 32)).toBase58();
    const quoteVault = new PublicKey(data.slice(quoteVaultOffset, quoteVaultOffset + 32)).toBase58();
    const poolAddress = account.pubkey.toBase58();

    console.log(`[pool-discovery] Found PumpSwap pool for ${mint} at ${poolAddress}`);

    return {
      poolAddress,
      baseVault,
      quoteVault,
    };
  } catch (error) {
    console.error(`[pool-discovery] Error finding pool for ${mint}:`, error.message);
    return null;
  }
}

// ---------- Utility Functions ----------

function isSendTransactionError(error) {
  if (!error) return false;
  if (error instanceof SendTransactionError) return true;
  return typeof error.getLogs === 'function' && typeof error.transactionError === 'object';
}

function parseAnchorErrorFromLogs(logs) {
  if (!Array.isArray(logs)) return null;
  for (const line of logs) {
    const match = line.match(/Error Code:\s*(\w+).*Error Number:\s*(\d+).*Error Message:\s*(.+)/i);
    if (match) {
      return {
        code: match[1],
        number: Number(match[2]),
        message: match[3]?.trim(),
      };
    }
  }
  return null;
}

async function describeSolanaError(error, { context = 'trade', mint } = {}) {
  const details = {
    message: error?.message || 'Unknown error',
    reason: null,
    logs: null,
    anchor: null,
    transactionMessage: null,
  };

  if (!error) return details;

  if (isSendTransactionError(error)) {
    try {
      const logs = await error.getLogs(connection);
      if (Array.isArray(logs)) {
        details.logs = logs.slice(-20);
        const anchor = parseAnchorErrorFromLogs(logs);
        if (anchor) {
          details.anchor = anchor;
          details.reason = anchor.code;
          details.message = anchor.message || details.message;
        }
      }
    } catch (logErr) {
      const scope = mint ? `${context}:${mint}` : context;
      console.error(`[${scope}] Failed to fetch transaction logs:`, logErr.message);
    }

    const txMessage = error.transactionError?.message;
    if (txMessage) {
      details.transactionMessage = txMessage;
      if (!details.anchor) {
        details.message = txMessage;
      }
    }

    if (details.message) {
      details.message = details.message.replace(/\nCatch the `SendTransactionError`.*$/, '').trim();
    }
  } else if (error?.message) {
    details.message = error.message;
  }

  if (!details.message) {
    details.message = 'Unknown transaction error';
  }

  return details;
}

function errorSuggestsGraduation(error, info) {
  const anchorCode = info?.anchor?.code || info?.reason || null;
  if (anchorCode) {
    const normalized = String(anchorCode);
    if (['BondingCurveComplete', 'BondingCurveClosed', 'BondingCurveFinished'].includes(normalized)) {
      return true;
    }
  }

  const textFragments = [];
  if (error?.message) textFragments.push(error.message);
  if (info?.message && info.message !== error?.message) textFragments.push(info.message);
  if (info?.transactionMessage) textFragments.push(info.transactionMessage);

  const normalizedFragments = textFragments
    .map((fragment) => fragment.toLowerCase())
    .filter(Boolean);

  if (!normalizedFragments.length) {
    return false;
  }

  const graduationPatterns = [
    /missing account:\s*bonding curve/i,
    /missing account:\s*associated bonding curve/i,
    /missing account:\s*creator vault/i,
    /pump\.fun bonding curve account missing discriminator/i,
    /unexpected pump\.fun bonding curve discriminator/i,
    /account .*bonding curve.* does not exist/i,
    /could not find account .*bonding curve/i,
    /bonding curve (?:is )?complete/i,
    /bonding curve .*closed/i,
  ];

  return normalizedFragments.some((fragment) =>
    graduationPatterns.some((regex) => regex.test(fragment)));
}

async function ensureGraduatedCoin(mint) {
  const coin = trackedCoins.get(mint);
  if (!coin) return null;

  if (coin.status === 'pumpswap' && coin.ammId) {
    return coin;
  }

  const poolInfo = await findPumpSwapPool(mint);
  if (!poolInfo) {
    return coin.status === 'pumpswap' ? coin : null;
  }

  await processGraduation(mint, poolInfo.baseVault, poolInfo.quoteVault, poolInfo.poolAddress);
  return trackedCoins.get(mint) || coin;
}

async function tradeWithGraduationFallback({
  mint,
  side,
  pumpfunParams,
  pumpswapParams = {},
  context = 'trade',
}) {
  if (!tradingService) {
    throw new Error('Trading service not configured');
  }

  const isBuy = side === 'buy';
  let coinRef = trackedCoins.get(mint) || null;
  const swapParams = { ...(pumpswapParams || {}) };
  const { forcePumpswap = false, lastKnownPool = null } = swapParams;
  delete swapParams.forcePumpswap;
  delete swapParams.lastKnownPool;

  const executePumpswap = async (reason) => {
    if (!coinRef || coinRef.status !== 'pumpswap' || !coinRef.ammId) {
      const updated = await ensureGraduatedCoin(mint);
      if (updated) {
        coinRef = updated;
      }
    }

    let pool = swapParams.pool || coinRef?.ammId || lastKnownPool || null;
    if (!pool) {
      // Try to find the pool using the new function
      try {
        pool = await tradingService.getGraduatedPoolAddress(mint);
        console.log(`[pump-trading] Found pool for graduated coin: ${pool}`);
      } catch (error) {
        const err = new Error('Liquidity migrated to PumpSwap, but the pool is not ready yet. Try again soon.');
        err.code = 'PUMPSWAP_POOL_NOT_READY';
        err.__solana = { reason: reason || 'PUMPSWAP_POOL_NOT_READY', message: err.message };
        throw err;
      }
    }

    pool = String(pool);
    const method = isBuy ? 'buyPumpswap' : 'sellPumpswap';

    if (typeof tradingService[method] !== 'function') {
      const err = new Error('PumpSwap trading method not available. The token has graduated but PumpSwap trading is not implemented.');
      err.code = 'Custom:6005';
      err.__solana = { reason: 'PUMPSWAP_METHOD_NOT_AVAILABLE', message: err.message };
      throw err;
    }

    const result = await tradingService[method]({ ...swapParams, pool });

    if (coinRef) {
      coinRef.status = 'pumpswap';
      coinRef.ammId = pool;
    }

    return {
      result,
      usedPumpswap: true,
      pool,
      fallbackReason: reason || null,
    };
  };

  if (forcePumpswap || (coinRef?.status === 'pumpswap' && (swapParams.pool || coinRef.ammId || lastKnownPool))) {
    return executePumpswap(null);
  }

  const method = isBuy ? 'buyPumpFun' : 'sellPumpFun';

  try {
    const result = await tradingService[method](pumpfunParams);
    return {
      result,
      usedPumpswap: false,
      pool: coinRef?.ammId ?? null,
      fallbackReason: null,
    };
  } catch (error) {
    const info = await describeSolanaError(error, { context, mint });
    const graduationDetected = errorSuggestsGraduation(error, info);

    if (graduationDetected) {
      if (!info.reason) {
        info.reason = info.anchor?.code || 'BondingCurveClosed';
      }
      console.log(
        `[${context}] Detected graduation for ${mint}. Attempting PumpSwap fallback... (${info.reason})`,
      );
      try {
        const fallback = await executePumpswap(info.reason);
        fallback.initialError = info;
        fallback.logs = info.logs || null;
        return fallback;
      } catch (fallbackError) {
        if (!fallbackError.__solana) {
          fallbackError.__solana = info;
        } else if (!fallbackError.__solana.previous) {
          fallbackError.__solana.previous = info;
        }
        throw fallbackError;
      }
    }

    error.__solana = info;
    throw error;
  }
}

function isValidBase58(str) {
  try { return bs58.decode(str).length === 32 && str !== WSOL_MINT; }
  catch { return false; }
}
function sanitizeMint(raw) {
  if (!raw) return null;
  let s = String(raw).trim().replace(/[^1-9A-HJ-NP-Za-km-z]/g, '');
  try {
    if (s === WSOL_MINT) return null;
    if (bs58.decode(s).length === 32) return s;
  } catch {}

  for (const len of [45, 44, 43, 42]) {
    if (s.length >= len) {
      const cand = s.slice(0, len);
      try {
        if (bs58.decode(cand).length === 32 && cand !== WSOL_MINT) return cand;
      } catch {}
    }
  }
  return null;
}

function diffNewMints(meta) {
  if (!meta) return [];
  const pre = meta.preTokenBalances ?? meta.pre_token_balances ?? [];
  const post = meta.postTokenBalances ?? meta.post_token_balances ?? [];
  const preSet = new Set(pre.map((b) => sanitizeMint(b?.mint)).filter(Boolean));
  const postMints = post.map((b) => sanitizeMint(b?.mint)).filter(Boolean);
  return Array.from(new Set(postMints.filter((m) => m && !preSet.has(m))));
}

function parseBigIntField(value, fieldName, { allowZero = true } = {}) {
  if (value === undefined || value === null) return null;
  let result;
  if (typeof value === 'bigint') {
    result = value;
  } else if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error(`${fieldName} must be a finite number`);
    result = BigInt(Math.trunc(value));
  } else if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.length === 0) return null;
    if (!/^-?\d+$/.test(trimmed)) {
      throw new Error(`${fieldName} must be an integer string`);
    }
    result = BigInt(trimmed);
  } else {
    throw new Error(`${fieldName} must be a number, string, or bigint`);
  }

  if (result < 0n) {
    throw new Error(`${fieldName} must be non-negative`);
  }
  if (!allowZero && result === 0n) {
    throw new Error(`${fieldName} must be greater than zero`);
  }
  return result;
}

function parsePriorityFeeLamports(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    throw new Error('priorityFeeSol must be a non-negative number');
  }
  const lamports = Math.floor(numeric * LAMPORTS_PER_SOL);
  if (!Number.isFinite(lamports) || lamports < 0) {
    throw new Error('priorityFeeSol results in an invalid lamport value');
  }
  if (lamports > Number.MAX_SAFE_INTEGER) {
    throw new Error('priorityFeeSol results in a lamport value that exceeds safe range');
  }
  return BigInt(lamports);
}

function deriveBondingCurvePDA(mint) {
  const m = new PublicKey(mint);
  return PublicKey.findProgramAddressSync(
    [Buffer.from('bonding-curve'), m.toBuffer()],
    PUMP_FUN_PROGRAM_ID
  )[0];
}

function deriveMetadataPDA(mint) {
  return PublicKey.findProgramAddressSync(
    [Buffer.from('metadata'), TOKEN_METADATA_PROGRAM_ID.toBuffer(), new PublicKey(mint).toBuffer()],
    TOKEN_METADATA_PROGRAM_ID
  )[0];
}

// ---------- Price Calculation ----------
function parseBondingCurveData(buf) {
  if (!buf || buf.length < 0x31) return null;
  const id = buf.subarray(0, 8);
  if (!id.equals(Buffer.from(PUMP_CURVE_STATE_SIGNATURE))) return null;

  try {
    const vTok = buf.readBigUInt64LE(0x08);
    const vSol = buf.readBigUInt64LE(0x10);
    const rTok = buf.readBigUInt64LE(0x18);
    const rSol = buf.readBigUInt64LE(0x20);
    const total = buf.readBigUInt64LE(0x28);

    const priceSOL = (Number(vSol) / LAMPORTS_PER_SOL) / (Number(vTok) / 10**PUMP_CURVE_TOKEN_DECIMALS);
    const priceUSD = priceSOL * solPriceUSD;
    const marketCapUSD = priceSOL * 1_000_000_000 * solPriceUSD;
    const progress = ((Number(rSol) / LAMPORTS_PER_SOL) / 85) * 100;

    return {
      priceSOL,
      priceUSD,
      marketCapUSD,
      progress,
      timestamp: Date.now()
    };
  } catch {
    return null;
  }
}

async function getCurveSnapshot(mint) {
  try {
    const ai = await connection.getAccountInfo(deriveBondingCurvePDA(mint), 'processed');
    if (!ai) return null;
    return parseBondingCurveData(ai.data);
  } catch {
    return null;
  }
}

async function getUiTokenBalance(mintPk, ownerPk) {
  const ata = await getAssociatedTokenAddress(mintPk, ownerPk, false, TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID);
  const bal = await connection.getTokenAccountBalance(ata).catch(() => null);
  if (!bal) return { uiAmount: 0, decimals: 0, ata };
  const uiAmount = Number(bal.value?.uiAmountString ?? "0");
  const decimals = Number(bal.value?.decimals ?? 0);
  return { uiAmount, decimals, ata };
}

async function fetchTokenMetadata(mint) {
  try {
    const metaPDA = deriveMetadataPDA(mint);
    const ai = await connection.getAccountInfo(metaPDA, 'processed');
    if (!ai?.data?.length) return null;

    let o = 1 + 32 + 32; // key + updateAuth + mint
    const readStr = () => {
      const len = ai.data.readUInt32LE(o);
      o += 4;
      const s = ai.data.subarray(o, o + len).toString('utf8');
      o += len;
      return s.replace(/\0/g, '').trim();
    };

    const name = readStr();
    const symbol = readStr();
    return { name, symbol };
  } catch {
    return null;
  }
}

async function resolveName(mint) {
  const tries = [250, 500, 1000, 2000];
  for (let i = 0; i < tries.length; i++) {
    const r = await fetchTokenMetadata(mint);
    if (r?.name) return r;
    await new Promise(r => setTimeout(r, tries[i]));
  }
  return null;
}
// ---------- AUTO-TRADING LOGIC ----------

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBoolean(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true') return true;
    if (normalized === 'false') return false;
  }
  return Boolean(value);
}

/**
 * Check if a buy should be triggered based on configured rules
 */
async function shouldTriggerBuy(mint, config, currentPrice, currentMcap) {
  if (!config.autoBuyEnabled || !config.enabled) return false;
  if (config.hasBought) return false; // Already bought

  const coin = trackedCoins.get(mint);
  if (!coin) return false;

  switch (config.buyTrigger) {
    case 'immediately':
      return true;

    case 'price_drop':
      // Trigger if price dropped by specified percentage from initial price
      const dropPercent = ((coin.launchPrice - currentPrice) / coin.launchPrice) * 100;
      return dropPercent >= config.buyTriggerValue;

    case 'price_target':
      // Trigger if price reaches target
      return currentPrice <= config.buyTriggerValue;

    case 'mcap_target':
      // Trigger if market cap reaches target
      return currentMcap >= config.buyTriggerValue;

    default:
      return false;
  }
}

/**
 * Execute an auto-buy
 */
async function executeAutoBuy(mint, config) {
  if (!tradingService) {
    console.error('[auto-trade] Trading service not available');
    return false;
  }

  try {
    const coin = trackedCoins.get(mint);
    console.log(`[auto-trade] Executing auto-buy for ${coin?.symbol || mint}`);
    console.log(`[auto-trade] Amount: ${config.buyAmount} SOL`);

    if (!coin) {
      const message = 'Coin not currently tracked';
      config.lastError = message;
      config.lastErrorTime = Date.now();
      console.error(`[auto-trade] ${message}`);
      return false;
    }

    const buyAmountSol = toNumber(config.buyAmount, 0);
    if (buyAmountSol <= 0) {
      const message = 'Buy amount must be greater than zero';
      config.lastError = message;
      config.lastErrorTime = Date.now();
      console.error(`[auto-trade] ${message}`);
      return false;
    }

    let priceSol = coin.currentPriceSOL ?? null;
    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const priceUsd = coin.currentPrice ?? coin.launchPrice ?? 0;
      if (priceUsd > 0 && solPriceUSD > 0) {
        priceSol = priceUsd / solPriceUSD;
      }
    }

    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const snapshot = await getCurveSnapshot(mint);
      if (snapshot?.priceSOL) {
        priceSol = snapshot.priceSOL;
        coin.currentPriceSOL = snapshot.priceSOL;
        if (snapshot.priceUSD) {
          coin.currentPrice = snapshot.priceUSD;
        }
        if (snapshot.marketCapUSD) {
          coin.marketCapUSD = snapshot.marketCapUSD;
        }
        coin.lastUpdated = Date.now();
      }
    }

    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const message = 'Unable to determine current price for auto-buy';
      console.error(`[auto-trade] ${message}`);
      config.lastError = message;
      config.lastErrorTime = Date.now();
      return false;
    }

    const decimals = coin.decimals ?? 6;
    const decimalFactor = Math.pow(10, decimals);
    const desiredTokens = buyAmountSol / priceSol;
    const tokenAmountBaseUnitsNumber = Math.max(1, Math.floor(desiredTokens * decimalFactor));
    const tokenAmountBaseUnits = BigInt(tokenAmountBaseUnitsNumber);
    const slippagePercent = Math.max(0, toNumber(config.slippagePercent, DEFAULT_SLIPPAGE_PERCENT));
    const spendLamports = Math.floor(buyAmountSol * LAMPORTS_PER_SOL);
    const maxSolCostLamports = Math.floor(spendLamports * (1 + slippagePercent / 100));
    const maxSolCost = BigInt(maxSolCostLamports);

    if (tokenAmountBaseUnits <= 0n || maxSolCost <= 0n) {
      const message = 'Calculated trade size is not valid';
      console.error(`[auto-trade] ${message}`, { tokenAmountBaseUnitsNumber, maxSolCostLamports });
      config.lastError = message;
      config.lastErrorTime = Date.now();
      return false;
    }

    let priorityFeeLamports = null;
    try {
      priorityFeeLamports = parsePriorityFeeLamports(config.priorityFeeSol);
    } catch (err) {
      const message = `Invalid priority fee: ${err.message}`;
      console.error(`[auto-trade] ${message}`);
      config.lastError = err.message;
      config.lastErrorTime = Date.now();
      return false;
    }

    const trade = await tradeWithGraduationFallback({
      mint,
      side: 'buy',
      context: 'auto-trade:buy',
      pumpfunParams: {
        mint,
        tokenAmount: tokenAmountBaseUnits,
        maxSolCost,
        trackVolume: true,
        priorityFeeLamports,
      },
      pumpswapParams: {
        pool: config.lastKnownPool || coin?.ammId || null,
        baseAmount: tokenAmountBaseUnits,
        maxQuoteAmount: maxSolCost,
        trackVolume: true,
        priorityFeeLamports,
      },
    });

    const { result, usedPumpswap, pool } = trade;

    console.log(`[auto-trade] Buy executed via ${usedPumpswap ? 'PumpSwap' : 'pump.fun'}: ${result.signature}`);

    if (usedPumpswap && pool) {
      config.lastKnownPool = pool;
    }

    // Update position state
    config.hasBought = true;
    config.buyPrice = coin.currentPrice;
    config.buyPriceSol = priceSol;
    config.buyTime = Date.now();
    config.tokenAmountBaseUnits = tokenAmountBaseUnits.toString();
    config.tokenDecimals = decimals;
    config.highestPriceSinceEntry = coin.currentPrice;
    config.status = 'active';
    config.signature = result.signature;
    config.estimatedEntryLamports = spendLamports;
    config.maxSpendLamports = maxSolCostLamports;
    config.lastError = null;

    broadcastPositionUpdate(mint, config, coin.currentPrice);
    return true;

  } catch (error) {
    const info = error.__solana || await describeSolanaError(error, {
      context: 'auto-trade:buy',
      mint,
    });
    console.error(`[auto-trade] Buy failed for ${mint}:`, info.message);
    if (info.logs) {
      console.error('[auto-trade] Buy logs:', info.logs);
    }
    config.lastError = info.message || error.message;
    config.lastErrorTime = Date.now();
    return false;
  }
}

/**
 * Execute an auto-sell
 */
async function executeAutoSell(mint, config, reason) {
  if (!tradingService) {
    console.error('[auto-trade] Trading service not available');
    return false;
  }

  try {
    const coin = trackedCoins.get(mint);
    const symbolOrMint = coin?.symbol || mint;
    console.log(`[auto-trade] Executing auto-sell for ${symbolOrMint}`);
    console.log(`[auto-trade] Reason: ${reason}`);

    if (!coin) {
      const message = 'Coin not currently tracked';
      config.lastError = message;
      config.lastErrorTime = Date.now();
      console.error(`[auto-trade] ${message}`);
      return false;
    }

    if (config.buyPrice > 0 && Number.isFinite(config.buyPrice) && Number.isFinite(coin.currentPrice)) {
      const pnlPercent = ((coin.currentPrice - config.buyPrice) / config.buyPrice) * 100;
      console.log(`[auto-trade] P&L: ${pnlPercent.toFixed(2)}%`);
    } else {
      console.log('[auto-trade] P&L: n/a');
    }

    const decimals = config.tokenDecimals ?? coin.decimals ?? 6;
    const decimalFactor = Math.pow(10, decimals);
    let tokenAmountBaseUnits = 0n;

    if (config.tokenAmountBaseUnits != null) {
      try {
        tokenAmountBaseUnits = BigInt(config.tokenAmountBaseUnits);
      } catch (err) {
        console.error('[auto-trade] Failed to parse stored tokenAmountBaseUnits:', err.message);
      }
    }

    if (tokenAmountBaseUnits <= 0n && config.tokenAmount != null) {
      const fallbackTokens = toNumber(config.tokenAmount, 0);
      if (fallbackTokens > 0) {
        tokenAmountBaseUnits = BigInt(Math.floor(fallbackTokens * decimalFactor));
      }
    }

    if (tokenAmountBaseUnits <= 0n) {
      const message = 'No token balance recorded for auto-sell';
      console.error(`[auto-trade] ${message}`);
      config.lastError = message;
      config.lastErrorTime = Date.now();
      return false;
    }

    const slippagePercent = Math.max(0, toNumber(config.slippagePercent, DEFAULT_SLIPPAGE_PERCENT));
    let minSolOutput = 0n;

    let priceSol = coin.currentPriceSOL ?? null;
    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const priceUsd = coin.currentPrice ?? config.sellPrice ?? 0;
      if (priceUsd > 0 && solPriceUSD > 0) {
        priceSol = priceUsd / solPriceUSD;
      }
    }

    if (priceSol && Number.isFinite(priceSol) && priceSol > 0) {
      const tokensFloat = Number(tokenAmountBaseUnits) / decimalFactor;
      const expectedSol = tokensFloat * priceSol;
      const minLamports = Math.max(
        0,
        Math.floor(expectedSol * (1 - slippagePercent / 100) * LAMPORTS_PER_SOL)
      );
      minSolOutput = BigInt(minLamports);
    } else {
      const estimatedEntryLamports = Math.max(0, toNumber(config.estimatedEntryLamports, 0));
      if (estimatedEntryLamports > 0) {
        const minLamports = Math.max(
          0,
          Math.floor(estimatedEntryLamports * (1 - slippagePercent / 100))
        );
        minSolOutput = BigInt(minLamports);
      }
    }

    let priorityFeeLamports = null;
    try {
      priorityFeeLamports = parsePriorityFeeLamports(config.priorityFeeSol);
    } catch (err) {
      const message = `Invalid priority fee: ${err.message}`;
      console.error(`[auto-trade] ${message}`);
      config.lastError = err.message;
      config.lastErrorTime = Date.now();
      return false;
    }

    const coinPool = config.lastKnownPool || coin?.ammId || null;
    const trade = await tradeWithGraduationFallback({
      mint,
      side: 'sell',
      context: 'auto-trade:sell',
      pumpfunParams: {
        mint,
        tokenAmount: tokenAmountBaseUnits,
        minSolOutput,
        priorityFeeLamports,
      },
      pumpswapParams: {
        pool: coinPool,
        baseAmount: tokenAmountBaseUnits,
        minQuoteAmount: minSolOutput,
        priorityFeeLamports,
      },
    });

    const { result, usedPumpswap, pool } = trade;

    console.log(`[auto-trade] Sell executed via ${usedPumpswap ? 'PumpSwap' : 'pump.fun'}: ${result.signature}`);

    if (usedPumpswap && pool) {
      config.lastKnownPool = pool;
    }

    // Update position state
    config.status = 'closed';
    config.sellPrice = coin.currentPrice;
    config.sellPriceSol = priceSol ?? null;
    config.sellTime = Date.now();
    config.sellReason = reason;
    config.sellSignature = result.signature;
    config.finalPnL = ((coin.currentPrice - config.buyPrice) / config.buyPrice) * 100;
    config.lastError = null;

    broadcastPositionUpdate(mint, config, coin.currentPrice);

    setTimeout(() => {
      autoTradingPositions.delete(mint);
      broadcastAllPositions();
    }, 60000);

    return true;

  } catch (error) {
    const info = error.__solana || await describeSolanaError(error, {
      context: 'auto-trade:sell',
      mint,
    });
    console.error(`[auto-trade] Sell failed for ${mint}:`, info.message);
    if (info.logs) {
      console.error('[auto-trade] Sell logs:', info.logs);
    }
    config.lastError = info.message || error.message;
    config.lastErrorTime = Date.now();
    return false;
  }
}

async function refreshManualPositionBalance(position) {
  if (!wallet) return position;
  try {
    const mintPk = new PublicKey(position.mint);
    const ata = await getAssociatedTokenAddress(
      mintPk,
      wallet.publicKey,
      false,
      TOKEN_PROGRAM_ID,
      ASSOCIATED_TOKEN_PROGRAM_ID
    );
    const balance = await connection.getTokenAccountBalance(ata).catch(() => null);
    if (balance?.value?.amount) {
      position.tokenAmountBaseUnits = balance.value.amount;
      position.tokenDecimals = balance.value.decimals ?? 6;
      const divisor = Math.pow(10, position.tokenDecimals);
      position.tokens = divisor > 0 ? Number(balance.value.amount) / divisor : 0;
    }
  } catch (err) {
    console.error(`[manual-position] Failed to refresh balance for ${position.mint}:`, err.message);
  }
  return position;
}

function updateManualPositionMetrics(position, currentPrice) {
  position.currentPrice = currentPrice;
  if (!position.highestPriceSinceEntry || currentPrice > position.highestPriceSinceEntry) {
    position.highestPriceSinceEntry = currentPrice;
  }
  if (position.buyPrice > 0) {
    position.pnlPercent = ((currentPrice - position.buyPrice) / position.buyPrice) * 100;
  } else {
    position.pnlPercent = 0;
  }
  if (position.highestPriceSinceEntry > 0) {
    position.drawdownPercent = ((position.highestPriceSinceEntry - currentPrice) / position.highestPriceSinceEntry) * 100;
  } else {
    position.drawdownPercent = 0;
  }
  if (position.stopLossPercent != null && position.stopLossPercent > 0) {
    position.stopLossPrice = position.buyPrice * (1 - position.stopLossPercent / 100);
    position.priceToLossRatio = position.stopLossPrice > 0 ? currentPrice / position.stopLossPrice : null;
  } else {
    position.stopLossPrice = null;
    position.priceToLossRatio = null;
  }
  position.lastUpdate = Date.now();
}

function serializeManualPosition(position) {
  return {
    mint: position.mint,
    buyPrice: position.buyPrice,
    currentPrice: position.currentPrice ?? position.buyPrice,
    highestPriceSinceEntry: position.highestPriceSinceEntry ?? position.buyPrice,
    pnlPercent: position.pnlPercent ?? 0,
    drawdownPercent: position.drawdownPercent ?? 0,
    stopLossPrice: position.stopLossPrice ?? null,
    priceToLossRatio: position.priceToLossRatio ?? null,
    takeProfitPercent: position.takeProfitPercent ?? 0,
    stopLossPercent: position.stopLossPercent ?? 0,
    trailingStopEnabled: !!position.trailingStopEnabled,
    trailingStopPercent: position.trailingStopPercent ?? 0,
    trailingActivationPercent: position.trailingActivationPercent ?? 0,
    status: position.status ?? 'active',
    buyTime: position.buyTime ?? null,
    sellTime: position.sellTime ?? null,
    sellPrice: position.sellPrice ?? null,
    sellSignature: position.sellSignature ?? null,
    sellReason: position.sellReason ?? null,
    signature: position.signature ?? null,
    tokens: position.tokens ?? null,
    tokenAmountBaseUnits: position.tokenAmountBaseUnits ?? null,
    tokenDecimals: position.tokenDecimals ?? 6,
    finalPnL: position.finalPnL ?? null,
    lastUpdate: position.lastUpdate ?? Date.now(),
    source: position.source ?? 'manual',
  };
}

function broadcastManualPositionUpdate(mint) {
  const position = manualPositions.get(mint);
  if (!position) return;
  broadcast({
    type: 'manualPositionUpdate',
    position: serializeManualPosition(position),
    timestamp: Date.now()
  });
}

function broadcastManualPositions() {
  const positions = Array.from(manualPositions.values()).map(serializeManualPosition);
  broadcast({
    type: 'manualPositions',
    positions,
    timestamp: Date.now()
  });
}

async function executeManualSell(mint, position, reason) {
  if (!tradingService) {
    console.error('[manual-position] Trading service not available');
    return false;
  }
  try {
    await refreshManualPositionBalance(position);
    const tokenAmount = BigInt(position.tokenAmountBaseUnits || '0');
    if (tokenAmount <= 0n) {
      console.warn(`[manual-position] No token balance available for ${mint}`, position.tokenAmountBaseUnits);
      position.status = 'closed';
      position.sellReason = 'No balance to sell';
      position.sellTime = Date.now();
      manualPositions.set(mint, position);
      broadcastManualPositionUpdate(mint);
      setTimeout(() => {
        manualPositions.delete(mint);
        broadcastManualPositions();
      }, 10000);
      return false;
    }

    const coin = trackedCoins.get(mint);
    const coinPool = position.lastKnownPool || coin?.ammId || null;

    const trade = await tradeWithGraduationFallback({
      mint,
      side: 'sell',
      context: 'manual-position:sell',
      pumpfunParams: {
        mint,
        tokenAmount,
        minSolOutput: 0n,
      },
      pumpswapParams: {
        pool: coinPool,
        baseAmount: tokenAmount,
        minQuoteAmount: 0n,
      },
    });

    const { result, usedPumpswap, pool } = trade;

    if (usedPumpswap && pool) {
      position.lastKnownPool = pool;
    }

    position.status = 'closed';
    position.sellReason = reason;
    position.sellSignature = result.signature;
    position.sellTime = Date.now();
    position.sellPrice = coin?.currentPrice ?? position.currentPrice ?? position.buyPrice;
    position.finalPnL = position.buyPrice > 0
      ? ((position.sellPrice - position.buyPrice) / position.buyPrice) * 100
      : null;

    manualPositions.set(mint, position);
    broadcastManualPositionUpdate(mint);

    setTimeout(() => {
      manualPositions.delete(mint);
      broadcastManualPositions();
    }, 60000);

    return true;
  } catch (err) {
    const info = err.__solana || await describeSolanaError(err, {
      context: 'manual-position:sell',
      mint,
    });
    console.error(`[manual-position] Sell failed for ${mint}:`, info.message);
    if (info.logs) {
      console.error('[manual-position] Sell logs:', info.logs);
    }
    position.lastError = info.message || err.message;
    position.lastErrorTime = Date.now();
    manualPositions.set(mint, position);
    broadcastManualPositionUpdate(mint);
    return false;
  }
}

async function evaluateManualPosition(mint, currentPrice) {
  const position = manualPositions.get(mint);
  if (!position || position.status !== 'active') return;

  updateManualPositionMetrics(position, currentPrice);

  const changePercent = position.pnlPercent ?? 0;

  if (position.takeProfitPercent && position.takeProfitPercent > 0 && changePercent >= position.takeProfitPercent) {
    await executeManualSell(mint, position, `Take profit reached: +${position.takeProfitPercent}%`);
    return;
  }

  if (position.trailingStopEnabled) {
    const activation = position.trailingActivationPercent ?? 0;
    if (changePercent >= activation && position.highestPriceSinceEntry > 0) {
      const dropFromHighest = ((position.highestPriceSinceEntry - currentPrice) / position.highestPriceSinceEntry) * 100;
      if (dropFromHighest >= (position.trailingStopPercent ?? 0)) {
        await executeManualSell(mint, position, `Trailing stop triggered: -${dropFromHighest.toFixed(2)}% from peak`);
        return;
      }
    }
  }

  if (position.stopLossPercent && position.stopLossPercent > 0 && changePercent <= -position.stopLossPercent) {
    await executeManualSell(mint, position, `Stop loss hit: ${changePercent.toFixed(2)}%`);
    return;
  }

  manualPositions.set(mint, position);
  broadcastManualPositionUpdate(mint);
}

async function createManualPositionFromTrade({
  mint,
  coin,
  tokenAmountRaw,
  maxSolCostRaw,
  positionConfig,
  signature
}) {
  if (!positionConfig?.trackPosition) return;

  const decimals = coin?.decimals ?? 6;
  const tokenAmountBaseUnits = tokenAmountRaw?.toString?.() ?? '0';
  let tokensHeld = null;
  if (tokenAmountRaw != null) {
    const divisor = Math.pow(10, decimals);
    tokensHeld = divisor > 0 ? Number(tokenAmountRaw) / divisor : 0;
  }

  const stopLossPercent = Number.isFinite(Number(positionConfig.stopLossPercent))
    ? Number(positionConfig.stopLossPercent)
    : 0;
  const takeProfitPercent = Number.isFinite(Number(positionConfig.takeProfitPercent))
    ? Number(positionConfig.takeProfitPercent)
    : 0;
  const trailingStopPercent = Number.isFinite(Number(positionConfig.trailingStopPercent))
    ? Number(positionConfig.trailingStopPercent)
    : 0;
  const trailingActivationPercent = Number.isFinite(Number(positionConfig.trailingActivationPercent))
    ? Number(positionConfig.trailingActivationPercent)
    : 0;

  const existing = manualPositions.get(mint) || { mint };
  const currentPrice = coin?.currentPrice ?? existing.currentPrice ?? 0;

  const position = {
    ...existing,
    mint,
    source: 'manual',
    status: 'active',
    buyTime: Date.now(),
    buyPrice: currentPrice,
    currentPrice,
    highestPriceSinceEntry: currentPrice,
    stopLossPercent,
    takeProfitPercent,
    trailingStopEnabled: !!positionConfig.trailingStopEnabled,
    trailingStopPercent,
    trailingActivationPercent,
    lastKnownPool: coin?.ammId || existing.lastKnownPool || null,
    signature: signature || existing.signature || null,
    tokenAmountBaseUnits,
    tokenDecimals: decimals,
    tokens: tokensHeld ?? existing.tokens ?? null,
    estimatedSolSpent: maxSolCostRaw != null
      ? Number(maxSolCostRaw) / LAMPORTS_PER_SOL
      : existing.estimatedSolSpent ?? null,
  };

  updateManualPositionMetrics(position, currentPrice);
  manualPositions.set(mint, position);
  broadcastManualPositionUpdate(mint);
  broadcastManualPositions();

  // Attempt to refresh real token balance once transaction settles
  setTimeout(async () => {
    try {
      const latest = manualPositions.get(mint);
      if (!latest || latest.status !== 'active') return;
      await refreshManualPositionBalance(latest);
      updateManualPositionMetrics(
        latest,
        trackedCoins.get(mint)?.currentPrice ?? latest.currentPrice ?? latest.buyPrice
      );
      manualPositions.set(mint, latest);
      broadcastManualPositionUpdate(mint);
    } catch (err) {
      console.error(`[manual-position] Post-buy balance refresh failed for ${mint}:`, err.message);
    }
  }, 5000);
}

/**
 * Check if auto-trading rules are met and execute trades
 */
async function checkAutoTradingRules(mint, currentPrice) {
  const config = autoTradingPositions.get(mint);
  if (!config || !config.enabled) return;

  const coin = trackedCoins.get(mint);
  if (!coin) return;

  // Update highest price since entry (for trailing stop)
  if (config.hasBought && currentPrice > config.highestPriceSinceEntry) {
    config.highestPriceSinceEntry = currentPrice;
  }

  // Check buy trigger
  if (!config.hasBought && config.autoBuyEnabled) {
    const shouldBuy = await shouldTriggerBuy(mint, config, currentPrice, coin.marketCapUSD);
    if (shouldBuy) {
      await executeAutoBuy(mint, config);
      return;
    }
  }

  // Check sell triggers (only if we have bought)
  if (config.hasBought && config.autoSellEnabled && config.status === 'active') {
    const priceChangePercent = ((currentPrice - config.buyPrice) / config.buyPrice) * 100;

    // Check take profit
    if (priceChangePercent >= config.takeProfitPercent) {
      await executeAutoSell(mint, config, `Take profit reached: +${config.takeProfitPercent}%`);
      return;
    }

    // Check trailing stop loss
    if (config.trailingStopEnabled && priceChangePercent >= config.trailingActivationPercent) {
      const dropFromHighest = ((config.highestPriceSinceEntry - currentPrice) / config.highestPriceSinceEntry) * 100;
      
      if (dropFromHighest >= config.trailingStopPercent) {
        await executeAutoSell(mint, config, `Trailing stop triggered: -${dropFromHighest.toFixed(2)}% from peak`);
        return;
      }
    }

    // Check fixed stop loss
    if (priceChangePercent <= -config.stopLossPercent) {
      await executeAutoSell(mint, config, `Stop loss hit: ${priceChangePercent.toFixed(2)}%`);
      return;
    }
  }
}

function setupRealTimePriceMonitoring(mint) {
  if (realTimePriceMonitors.has(mint)) return; // Already monitoring

  const pda = deriveBondingCurvePDA(mint);
  
  try {
    const subscriptionId = connection.onAccountChange(pda, async (acc) => {
      const parsed = parseBondingCurveData(acc.data);
      if (!parsed || !parsed.priceUSD) return;
      
      const coin = trackedCoins.get(mint);

      // Update coin data
      if (coin) {

        const tickData = {
            timestamp: Date.now(),
            mint: mint,
            name: coin.name,
            symbol: coin.symbol,
            priceUSD: parsed.priceUSD,
            marketCapUSD: parsed.marketCapUSD
        };

        priceLogStream.write(JSON.stringify(tickData) + '\n');

        if (!coin.priceHistory) coin.priceHistory = [];
        coin.priceHistory.push({ price: parsed.priceUSD, timestamp: Date.now() });
        if (coin.priceHistory.length > 100) {
          coin.priceHistory = coin.priceHistory.slice(-100);
        }
        
        coin.currentPrice = parsed.priceUSD;
        coin.currentPriceSOL = parsed.priceSOL;
        coin.marketCapUSD = parsed.marketCapUSD;
        coin.lastUpdated = Date.now();
        if (parsed.priceUSD > (coin.highestPrice || 0)) {
          coin.highestPrice = parsed.priceUSD;
        }
      }

      // Check auto-trading and manual position rules
      await checkAutoTradingRules(mint, parsed.priceUSD);
      await evaluateManualPosition(mint, parsed.priceUSD);
      
      // Update UI immediately for gainers table
      broadcast({
        type: 'priceUpdate',
        coin: {
          mint,
          currentPrice: parsed.priceUSD,
          gainPercent: coin ? computeGainPercent(parsed.priceUSD, coin.launchPrice) : 0,
          marketCapUSD: parsed.marketCapUSD,
          status: coin.status
        }
      });

      pushFullTopGainers();
    }, 'processed');

    realTimePriceMonitors.set(mint, { subscriptionId, type: 'pump' });
    
  } catch (error) {
    console.error(`[price-monitor] Failed to setup monitoring for ${mint}:`, error.message);
  }
}

// ---------- App / HTTP ----------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function createHttpServerWithFallback(appInstance, startPort, maxOffset = 0) {
  let lastError = null;
  const maxOffsetSafe = Math.max(0, Number.isFinite(maxOffset) ? maxOffset : 0);

  for (let offset = 0; offset <= maxOffsetSafe; offset += 1) {
    const port = startPort + offset;
    const server = http.createServer(appInstance);

    try {
      await new Promise((resolve, reject) => {
        const handleError = (error) => {
          server.removeListener('listening', handleListening);
          reject(error);
        };
        const handleListening = () => {
          server.removeListener('error', handleError);
          resolve();
        };
        server.once('error', handleError);
        server.once('listening', handleListening);
        server.listen(port, '0.0.0.0');
      });

      if (offset > 0) {
        console.warn(`[web] Preferred port ${startPort} unavailable. Serving on ${port}.`);
      }

      return { server, port };
    } catch (error) {
      lastError = error;

      if (error?.code === 'EADDRINUSE') {
        if (offset < maxOffsetSafe) {
          console.warn(`[web] Port ${port} already in use, trying ${port + 1}...`);
        } else {
          console.error(`[web] Port ${port} already in use and no fallback ports remain.`);
        }
        try { server.close(); } catch (_) {}
        continue;
      }

      try { server.close(); } catch (_) {}
      throw error;
    }
  }

  throw lastError || new Error(`Unable to find an open port starting at ${startPort}`);
}

const app = express();
app.use(express.json());
app.use(express.static(__dirname));

app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// API endpoint (kept for compatibility; UI is WS-driven now)
app.get('/api/top-gainers', (_req, res) => {
  const limit = parseInt(_req.query.limit) || topCoinsLimit;

  const list = currentTopGainersArray().slice(0, limit);
  res.json({
    topGainers: list,
    totalTracked: trackedCoins.size,
    solPriceUSD: solPriceUSD
  });
});

app.get('/api/transactions', (_req, res) => {
  res.json({
    transactions: recentTransactions,
    timestamp: Date.now()
  });
});

// Reset
app.post('/api/reset', (_req, res) => {
  console.log('[reset] Clearing all tracked coins and starting fresh');
  trackedCoins.clear();
  seenMints.clear();
  
  // Clear price monitors
  for (const [mint, monitor] of realTimePriceMonitors.entries()) {
    try {
      connection.removeAccountChangeListener(monitor.subscriptionId);
    } catch (e) {
      console.error(`[reset] Failed to remove listener for ${mint}:`, e.message);
    }
  }
  realTimePriceMonitors.clear();
  
  // Clear ultra-fast intervals
  for (const [mint, interval] of priceUpdateIntervals.entries()) {
    clearInterval(interval);
  }
  priceUpdateIntervals.clear();
  
  broadcast({ type: 'reset', message: 'Tracking reset - starting fresh' });
  pushFullTopGainers();
  broadcastStats();
  res.json({ success: true, message: 'All tracking data cleared', timestamp: Date.now() });
});

// Wallet balance
app.get("/api/balance", async (_req, res) => {
  if (!wallet) return res.status(400).json({ error: "Wallet not configured" });
  try {
    const lamports = await connection.getBalance(wallet.publicKey);
    res.json({
      address: wallet.publicKey.toBase58(),
      balance: lamports / LAMPORTS_PER_SOL
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update limit
app.post('/api/update-limit', (req, res) => {
  const newLimit = parseInt(req.body.limit);
  if (newLimit && newLimit > 0 && newLimit <= 1000) {
    topCoinsLimit = newLimit;
    broadcast({ type: 'limitUpdated', newLimit: topCoinsLimit });
    pushFullTopGainers();
    res.json({ success: true, newLimit: topCoinsLimit });
  } else {
    res.status(400).json({ error: 'Invalid limit. Must be between 1 and 1000.' });
  }
});

// Auto-trading configuration endpoint
app.post('/api/auto-trade/config', (req, res) => {
  try {
    const config = req.body;
    if (!config.mint) {
      return res.status(400).json({ error: 'Mint address is required' });
    }

    // Initialize or update position
    const position = autoTradingPositions.get(config.mint) || {
      mint: config.mint,
      hasBought: false,
      status: 'waiting',
      slippagePercent: DEFAULT_SLIPPAGE_PERCENT
    };

    const buyAmount = toNumber(config.buyAmount, position.buyAmount ?? 0);
    const buyTriggerValue = toNumber(config.buyTriggerValue, position.buyTriggerValue ?? 0);
    const takeProfitPercent = toNumber(config.takeProfitPercent, position.takeProfitPercent ?? 0);
    const stopLossPercent = toNumber(config.stopLossPercent, position.stopLossPercent ?? 0);
    const trailingStopPercent = toNumber(config.trailingStopPercent, position.trailingStopPercent ?? 0);
    const trailingActivationPercent = toNumber(
      config.trailingActivationPercent,
      position.trailingActivationPercent ?? 0
    );
    const slippagePercent = toNumber(
      config.slippagePercent,
      position.slippagePercent ?? DEFAULT_SLIPPAGE_PERCENT
    );
    const priorityFeeSol = Math.max(
      0,
      toNumber(config.priorityFeeSol, position.priorityFeeSol ?? 0)
    );

    // Update configuration
    Object.assign(position, {
      enabled: toBoolean(config.enabled, position.enabled ?? false),
      autoBuyEnabled: toBoolean(config.autoBuyEnabled, position.autoBuyEnabled ?? true),
      buyAmount,
      buyTrigger: config.buyTrigger || position.buyTrigger || 'immediately',
      buyTriggerValue,
      autoSellEnabled: toBoolean(config.autoSellEnabled, position.autoSellEnabled ?? true),
      takeProfitPercent,
      stopLossPercent,
      trailingStopEnabled: toBoolean(config.trailingStopEnabled, position.trailingStopEnabled ?? false),
      trailingStopPercent,
      trailingActivationPercent,
      slippagePercent,
      priorityFeeSol,
      lastUpdated: Date.now()
    });

    autoTradingPositions.set(config.mint, position);
    console.log(`[auto-trade] Configuration saved for ${config.mint}`);

    broadcastAllPositions();

    res.json({ success: true, position });
  } catch (error) {
    console.error('[auto-trade] Configuration error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Close auto-trading position
app.post('/api/auto-trade/close', async (req, res) => {
  try {
    const { mint } = req.body;
    if (!mint) {
      return res.status(400).json({ error: 'Mint address is required' });
    }

    const position = autoTradingPositions.get(mint);
    if (!position) {
      return res.status(404).json({ error: 'Position not found' });
    }

    // If we have an active buy, sell it
    if (position.hasBought && position.status === 'active') {
      const coin = trackedCoins.get(mint);
      await executeAutoSell(mint, position, 'Manual close');
    } else {
      // Just remove the configuration
      autoTradingPositions.delete(mint);
    }

    broadcastAllPositions();

    res.json({ success: true, message: 'Position closed' });
  } catch (error) {
    console.error('[auto-trade] Close error:', error);
    res.status(500).json({ error: error.message });
  }
});
app.post('/api/trade/pumpfun/buy-sol', async (req, res) => {
  if (!tradingService) {
    return res.status(400).json({ error: 'Trading wallet not configured' });
  }

  try {
    const {
      mint,
      solAmount,
      slippagePercent = DEFAULT_SLIPPAGE_PERCENT,
      trackVolume = false,
    } = req.body || {};

    if (!mint) {
      return res.status(400).json({ error: 'mint is required' });
    }

    const spendSol = toNumber(solAmount, 0);
    if (!Number.isFinite(spendSol) || spendSol <= 0) {
      return res.status(400).json({ error: 'solAmount must be greater than zero' });
    }

    const coin = trackedCoins.get(mint);
    if (!coin) {
      return res.status(404).json({ error: 'Coin not currently tracked' });
    }

    let priceSol = coin.currentPriceSOL ?? null;
    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const priceUsd = coin.currentPrice ?? coin.launchPrice ?? 0;
      if (priceUsd > 0 && solPriceUSD > 0) {
        priceSol = priceUsd / solPriceUSD;
      }
    }

    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      const snapshot = await getCurveSnapshot(mint);
      if (snapshot?.priceSOL) {
        priceSol = snapshot.priceSOL;
        coin.currentPriceSOL = snapshot.priceSOL;
        if (snapshot.priceUSD) {
          coin.currentPrice = snapshot.priceUSD;
        }
        if (snapshot.marketCapUSD) {
          coin.marketCapUSD = snapshot.marketCapUSD;
        }
        coin.lastUpdated = Date.now();
      }
    }

    if (!priceSol || !Number.isFinite(priceSol) || priceSol <= 0) {
      return res.status(400).json({ error: 'Unable to determine current price for this mint' });
    }

    let priorityFeeLamports = null;
    try {
      priorityFeeLamports = parsePriorityFeeLamports(req.body?.priorityFeeSol);
    } catch (err) {
      return res.status(400).json({ error: err.message || 'Invalid priority fee' });
    }

    const decimals = Number.isFinite(coin.decimals) ? coin.decimals : 6;
    const decimalFactor = Math.pow(10, decimals);
    const desiredTokens = spendSol / priceSol;
    const tokenAmountBaseUnitsNumber = Math.max(1, Math.floor(desiredTokens * decimalFactor));
    const tokenAmount = BigInt(tokenAmountBaseUnitsNumber);

    const spendLamports = Math.floor(spendSol * LAMPORTS_PER_SOL);
    const slip = Math.max(0, toNumber(slippagePercent, DEFAULT_SLIPPAGE_PERCENT));
    const maxSolCostLamports = Math.max(1, Math.floor(spendLamports * (1 + slip / 100)));
    const maxSolCost = BigInt(maxSolCostLamports);

    if (tokenAmount <= 0n || maxSolCost <= 0n) {
      return res.status(400).json({ error: 'Calculated trade size is invalid' });
    }

    const trade = await tradeWithGraduationFallback({
      mint,
      side: 'buy',
      context: 'trade:pumpfun-buy-sol',
      pumpfunParams: {
        mint,
        tokenAmount,
        maxSolCost,
        trackVolume: !!trackVolume,
        priorityFeeLamports,
      },
      pumpswapParams: {
        pool: coin.ammId || null,
        baseAmount: tokenAmount,
        maxQuoteAmount: maxSolCost,
        trackVolume: !!trackVolume,
        priorityFeeLamports,
      },
    });

    const { result, usedPumpswap, pool, fallbackReason } = trade;

    coin.lastTxSig = result.signature;
    coin.lastUpdated = Date.now();
    if (usedPumpswap && pool) {
      coin.status = 'pumpswap';
      coin.ammId = pool;
    }

    res.json({
      ok: true,
      signature: result.signature,
      tokenAmount: tokenAmount.toString(),
      maxSolCostLamports,
      lastValidBlockHeight: result.lastValidBlockHeight,
      route: usedPumpswap ? 'pumpswap' : 'pumpfun',
      pool: pool ?? null,
      fallbackReason: fallbackReason ?? null,
    });
  } catch (error) {
    const info = error.__solana || await describeSolanaError(error, {
      context: 'trade:pumpfun-buy-sol',
      mint: req.body?.mint,
    });
    console.error('[trade:pumpfun-buy-sol] Failed to submit quick buy:', info.message);
    if (info.logs) {
      console.error('[trade:pumpfun-buy-sol] Logs:', info.logs);
    }
    res.status(500).json({
      error: info.message || error.message || 'Quick pump.fun buy failed',
      reason: info.reason || error.code || null,
      logs: info.logs || null,
    });
  }
});

app.post('/api/trade/pumpfun', async (req, res) => {
  if (!tradingService) {
    return res.status(400).json({ error: 'Trading wallet not configured' });
  }
  try {
    const {
      mint,
      side,
      tokenAmount,
      maxSolCostLamports,
      minSolOutputLamports,
      trackVolume = false,
      positionConfig
    } = req.body || {};

    if (!mint || !side) {
      return res.status(400).json({ error: 'mint and side are required' });
    }
    let priorityFeeLamports = null;
    try {
      priorityFeeLamports = parsePriorityFeeLamports(req.body?.priorityFeeSol);
    } catch (err) {
      return res.status(400).json({ error: err.message || 'Invalid priority fee' });
    }
    const tokenAmountRaw = parseBigIntField(tokenAmount, 'tokenAmount', { allowZero: false });
    const coin = trackedCoins.get(mint);
    const fallbackPool = coin?.ammId || positionConfig?.lastKnownPool || null;
    let tradeResult;
    let maxSolCostRaw = null;
    let minSolOutputRaw = null;

    if (side === 'buy') {
      maxSolCostRaw = parseBigIntField(maxSolCostLamports, 'maxSolCostLamports', { allowZero: false });
      tradeResult = await tradeWithGraduationFallback({
        mint,
        side: 'buy',
        context: 'trade:pumpfun:buy',
        pumpfunParams: {
          mint,
          tokenAmount: tokenAmountRaw,
          maxSolCost: maxSolCostRaw,
          trackVolume: !!trackVolume,
          priorityFeeLamports,
        },
        pumpswapParams: {
          pool: fallbackPool,
          baseAmount: tokenAmountRaw,
          maxQuoteAmount: maxSolCostRaw,
          trackVolume: !!trackVolume,
          priorityFeeLamports,
        },
      });
    } else if (side === 'sell') {
      minSolOutputRaw = parseBigIntField(minSolOutputLamports, 'minSolOutputLamports', { allowZero: true });
      if (minSolOutputRaw == null) {
        minSolOutputRaw = 0n;
      }
      tradeResult = await tradeWithGraduationFallback({
        mint,
        side: 'sell',
        context: 'trade:pumpfun:sell',
        pumpfunParams: {
          mint,
          tokenAmount: tokenAmountRaw,
          minSolOutput: minSolOutputRaw,
          priorityFeeLamports,
        },
        pumpswapParams: {
          pool: fallbackPool,
          baseAmount: tokenAmountRaw,
          minQuoteAmount: minSolOutputRaw,
          priorityFeeLamports,
        },
      });
    } else {
      return res.status(400).json({ error: 'Invalid side. Use "buy" or "sell".' });
    }

    const { result, usedPumpswap, pool, fallbackReason } = tradeResult;

    if (coin) {
      coin.lastTxSig = result.signature;
      coin.lastUpdated = Date.now();
      if (usedPumpswap && pool) {
        coin.status = 'pumpswap';
        coin.ammId = pool;
      }
    }

    if (side === 'buy' && positionConfig?.trackPosition) {
      await createManualPositionFromTrade({
        mint,
        coin,
        tokenAmountRaw,
        maxSolCostRaw,
        positionConfig,
        signature: result.signature
      });
    }

    res.json({
      ok: true,
      signature: result.signature,
      lastValidBlockHeight: result.lastValidBlockHeight,
      route: usedPumpswap ? 'pumpswap' : 'pumpfun',
      pool: pool ?? null,
      fallbackReason: fallbackReason ?? null,
    });
  } catch (error) {
    const info = error.__solana || await describeSolanaError(error, {
      context: 'trade:pumpfun',
      mint: req.body?.mint,
    });
    console.error('[trade:pumpfun] Failed to submit trade:', info.message);
    if (info.logs) {
      console.error('[trade:pumpfun] Logs:', info.logs);
    }
    res.status(500).json({
      error: info.message || error.message || 'Pump.fun trade failed',
      reason: info.reason || error.code || null,
      logs: info.logs || null,
    });
  }
});

app.post('/api/trade/pumpswap', async (req, res) => {
  if (!tradingService) {
    return res.status(400).json({ error: 'Trading wallet not configured' });
  }
  try {
    const {
      pool,
      mint,
      side,
      baseAmount,
      maxQuoteAmount,
      minQuoteAmount,
      trackVolume = false,
      positionConfig,
    } = req.body || {};

    let poolAddress = pool;
    if (!poolAddress && mint) {
      const tracked = trackedCoins.get(mint);
      if (tracked?.ammId) {
        poolAddress = tracked.ammId;
      }
    }
    const coin = mint ? trackedCoins.get(mint) : null;

    if (!poolAddress) {
      return res.status(400).json({ error: 'Pool address (or tracked mint with ammId) is required' });
    }
    if (!side) {
      return res.status(400).json({ error: 'side is required' });
    }

    let priorityFeeLamports = null;
    try {
      priorityFeeLamports = parsePriorityFeeLamports(req.body?.priorityFeeSol);
    } catch (err) {
      return res.status(400).json({ error: err.message || 'Invalid priority fee' });
    }

    const baseAmountRaw = parseBigIntField(baseAmount, 'baseAmount', { allowZero: false });
    if (baseAmountRaw === null) {
      return res.status(400).json({ error: 'baseAmount is required' });
    }

    let maxQuoteRaw = null;
    let minQuoteRaw = null;
    let result;

    if (side === 'buy') {
      maxQuoteRaw = parseBigIntField(maxQuoteAmount, 'maxQuoteAmount', { allowZero: false });
      if (maxQuoteRaw === null) {
        return res.status(400).json({ error: 'maxQuoteAmount is required for buys' });
      }

      if (typeof tradingService.buyPumpswap !== 'function') {
        return res.status(500).json({
          error: 'PumpSwap buy method not available. The token has graduated but PumpSwap trading is not implemented.',
          code: 'Custom:6005'
        });
      }

      result = await tradingService.buyPumpswap({
        pool: poolAddress,
        baseAmount: baseAmountRaw,
        maxQuoteAmount: maxQuoteRaw,
        trackVolume: !!trackVolume,
        priorityFeeLamports,
      });
    } else if (side === 'sell') {
      minQuoteRaw = parseBigIntField(minQuoteAmount, 'minQuoteAmount', { allowZero: false });
      if (minQuoteRaw === null) {
        return res.status(400).json({ error: 'minQuoteAmount is required for sells' });
      }

      if (typeof tradingService.sellPumpswap !== 'function') {
        return res.status(500).json({
          error: 'PumpSwap sell method not available. The token has graduated but PumpSwap trading is not implemented.',
          code: 'Custom:6005'
        });
      }

      result = await tradingService.sellPumpswap({
        pool: poolAddress,
        baseAmount: baseAmountRaw,
        minQuoteAmount: minQuoteRaw,
        priorityFeeLamports,
      });
    } else {
      return res.status(400).json({ error: 'Invalid side. Use "buy" or "sell".' });
    }

    if (coin) {
      if (!coin.ammId && poolAddress) {
        coin.ammId = poolAddress;
      }
      coin.lastTxSig = result.signature;
      coin.lastUpdated = Date.now();
    }

    if (side === 'buy' && positionConfig?.trackPosition && mint) {
      await createManualPositionFromTrade({
        mint,
        coin,
        tokenAmountRaw: baseAmountRaw,
        maxSolCostRaw: maxQuoteRaw,
        positionConfig,
        signature: result.signature
      });
    }

    res.json({
      ok: true,
      signature: result.signature,
      lastValidBlockHeight: result.lastValidBlockHeight,
    });
  } catch (error) {
    console.error('[trade:pumpswap] Failed to submit trade:', error);
    res.status(500).json({ error: error.message || 'PumpSwap trade failed' });
  }
});

// Test endpoint for PumpSwap functionality
app.get('/api/test/pumpswap/:mint', async (req, res) => {
  if (!tradingService) {
    return res.status(400).json({ error: 'Trading wallet not configured' });
  }

  try {
    const { mint } = req.params;
    
    // Test RPC connection
    await tradingService.testRPCConnection();
    
    // Try to find pool for the mint
    const poolAddress = await tradingService.getGraduatedPoolAddress(mint);
    
    // Get pool info
    const poolInfo = await tradingService.getPoolInfo(mint);
    
    res.json({
      ok: true,
      mint,
      poolAddress,
      poolInfo: {
        address: poolInfo.address,
        tokenMint: poolInfo.tokenMint.toBase58(),
        tokenVault: poolInfo.tokenVault.toBase58(),
        solVault: poolInfo.solVault.toBase58(),
        tokenReserve: poolInfo.tokenReserve.toString(),
        solReserve: poolInfo.solReserve.toString(),
        feeNumerator: poolInfo.feeNumerator.toString(),
        feeDenominator: poolInfo.feeDenominator.toString()
      }
    });
  } catch (error) {
    console.error('[test:pumpswap] Test failed:', error);
    res.status(500).json({ 
      error: error.message || 'PumpSwap test failed',
      details: error.toString()
    });
  }
});

(async function initialize() {
  console.log('[init] 🔥 Fetching initial SOL/USD price...');
  await updateSOLPriceWithFallback();
  console.log(`[init] ✅ SOL/USD initialized at $${solPriceUSD.toFixed(4)}`);
})();

const desiredWebPort = Number.isFinite(Number(WEB_PORT)) ? Number(WEB_PORT) : 8787;
const parsedFallbackEnv = Number(process.env.WEB_PORT_MAX_OFFSET);
const fallbackRange = Number.isFinite(parsedFallbackEnv) ? Math.max(0, parsedFallbackEnv) : 20;

let httpServer;
let activeWebPort = desiredWebPort;

try {
  const { server, port } = await createHttpServerWithFallback(app, desiredWebPort, fallbackRange);
  httpServer = server;
  activeWebPort = port;
  console.log(`[web] http://localhost:${activeWebPort}`);
} catch (error) {
  console.error('[web] Failed to start web server:', error?.message || error);
  console.error('[web] If the port is in use, stop the other process or set WEB_PORT to a different value.');
  process.exit(1);
}

// ---------- WebSocket Setup ----------
const wss = new WebSocketServer({ server: httpServer });
wss.on('error', (error) => {
  console.error('[websocket] Server error:', error?.message || error);
});

function wsSend(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach((c) => {
    if (c.readyState === 1) c.send(msg);
  });
}

function broadcastStats() {
  broadcast({
    type: 'stats',
    totalTracked: trackedCoins.size,
    topCoinsLimit,
    solPriceUSD,
    autoPositionsCount: autoTradingPositions.size,
    manualPositionsCount: manualPositions.size,
  });
}

function broadcastPositionUpdate(mint, config, currentPrice) {
  broadcast({
    type: 'positionUpdate',
    position: {
      mint,
      ...config,
      currentPrice
    }
  });
}

function broadcastAllPositions() {
  const positions = Array.from(autoTradingPositions.entries()).map(([mint, config]) => {
    const coin = trackedCoins.get(mint);
    return {
      mint,
      ...config,
      currentPrice: coin?.currentPrice || 0
    };
  });

  broadcast({
    type: 'autoPositions',
    positions,
    timestamp: Date.now()
  });
}

function computeGainPercent(current, launch) {
  if (!launch || launch <= 0 || current == null) return 0;
  return ((current - launch) / launch) * 100;
}

function currentTopGainersArray() {
  const arr = Array.from(trackedCoins.values())
    .filter(c => (c.marketCapUSD ?? 0) >= MIN_MCAP && (c.launchPrice ?? 0) > 0 && (c.currentPrice ?? 0) > 0)
    .map(c => ({
      ...c,
      gainPercent: computeGainPercent(c.currentPrice, c.launchPrice),
      marketCapChange: (c.marketCapUSD ?? 0) - (c.launchMarketCap ?? 0),
    }))
    .sort((a, b) => (b.gainPercent || 0) - (a.gainPercent || 0));
  return arr.slice(0, Math.max(1, Math.min(1000, topCoinsLimit)));
}

function pushFullTopGainers() {
  const gainers = currentTopGainersArray();
  broadcast({
    type: 'topGainers',
    gainers,
    timestamp: Date.now(),
    totalTracked: trackedCoins.size
  });
}

// Initial WS snapshot
wss.on('connection', (ws) => {
  console.log('[websocket] Client connected');

  wsSend(ws, {
    type: "stats",
    totalTracked: trackedCoins.size,
    topCoinsLimit,
    solPriceUSD,
    autoPositionsCount: autoTradingPositions.size,
    manualPositionsCount: manualPositions.size,
  });

  wsSend(ws, {
    type: 'topGainers',
    gainers: currentTopGainersArray(),
    timestamp: Date.now(),
    totalTracked: trackedCoins.size
  });

  // Send current auto-trading positions
  const positions = Array.from(autoTradingPositions.entries()).map(([mint, config]) => {
    const coin = trackedCoins.get(mint);
    return {
      mint,
      ...config,
      currentPrice: coin?.currentPrice || 0
    };
  });

  wsSend(ws, {
    type: 'autoPositions',
    positions,
    timestamp: Date.now()
  });

  wsSend(ws, {
    type: 'manualPositions',
    positions: Array.from(manualPositions.values()).map(serializeManualPosition),
    timestamp: Date.now()
  });

  wsSend(ws, {
    type: 'transactions',
    transactions: recentTransactions,
    timestamp: Date.now()
  });

  ws.on('message', async (raw) => {
    try {
      const data = JSON.parse(raw.toString());
      if (data.action === 'updateLimit') {
        const newLimit = parseInt(data.limit);
        if (newLimit && newLimit > 0 && newLimit <= 1000) {
          topCoinsLimit = newLimit;
          wsSend(ws, { type: 'limitUpdated', newLimit: topCoinsLimit });
          pushFullTopGainers();
        }
      } else if (data.action === 'getPositions') {
        broadcastAllPositions();
        broadcastManualPositions();
      }
    } catch (e) {
      console.error('[websocket] Message handling error:', e.message);
    }
  });

  ws.on('close', () => {
    console.log('[websocket] Client disconnected');
  });
});

// ---------- Enhanced Coin Tracking (SUPER REAL-TIME) ----------
async function addNewCoin(mint) {
  if (trackedCoins.has(mint) || seenMints.has(mint)) return;
  // Cap tracked set; drop oldest if needed
  if (trackedCoins.size >= MAX_TRACKED) {
    const oldestMint = trackedCoins.keys().next().value;
    trackedCoins.delete(oldestMint);
    
    // Remove price monitoring for dropped coin
    const monitor = realTimePriceMonitors.get(oldestMint);
    if (monitor) {
      connection.removeAccountChangeListener(monitor.subscriptionId);
      realTimePriceMonitors.delete(oldestMint);
    }
    
    // Remove ultra-fast polling for dropped coin
    const interval = priceUpdateIntervals.get(oldestMint);
    if (interval) {
      clearInterval(interval);
      priceUpdateIntervals.delete(oldestMint);
    }
  }

  seenMints.add(mint);

  const coin = {
    mint,
    name: "Unknown",
    symbol: "UNK",
    launchTime: Date.now(),
    launchPrice: 0,
    launchMarketCap: 0,
    currentPrice: 0,
    currentPriceSOL: 0,
    marketCapUSD: 0,
    highestPrice: 0,
    resolved: false,
    lastUpdated: Date.now(),
    priceHistory: [],
    status: 'pump',
    baseVault: null,
    quoteVault: null,
    decimals: 6
  };

  trackedCoins.set(mint, coin);
  console.log(`[new-coin] Added ${mint.slice(0,8)} to tracking`);

  // Try to resolve name soon; when found, push an immediate patch
  (async () => {
    const metadata = await resolveName(mint);
    if (metadata) {
      const c = trackedCoins.get(mint);
      if (!c) return;
      c.name = metadata.name;
      c.symbol = metadata.symbol;
      c.resolved = true;
      c.lastUpdated = Date.now();
      console.log(`[name-resolved] ${mint}: ${c.name} (${c.symbol})`);
      broadcast({ type: 'priceUpdate', coin: { mint, name: c.name, symbol: c.symbol } });
    }
  })();

  // Establish launch snapshot quickly, then switch to enhanced live monitoring
  setTimeout(async () => {
    const snap = await getCurveSnapshot(mint);
    if (snap && snap.priceUSD > 0) {
      coin.launchPrice = snap.priceUSD;
      coin.launchMarketCap = snap.marketCapUSD;
      coin.currentPrice = snap.priceUSD;
      coin.currentPriceSOL = snap.priceSOL;
      coin.marketCapUSD = snap.marketCapUSD;
      coin.highestPrice = snap.priceUSD;
      coin.lastUpdated = Date.now();
      coin.priceHistory.push({ price: snap.priceUSD, timestamp: Date.now() });

      // Immediately notify UI of the new row
      broadcast({ type: 'newMint', coin: { ...coin } });
      // And refresh the top list right away
      pushFullTopGainers();

      // Start real-time monitoring
      setupRealTimePriceMonitoring(mint);
    } else {
      console.log(`[price-check] Failed to get price data for ${mint.slice(0,8)}`);
    }
  }, 500);
}

// ---------- Transaction Tracking ----------
const PUMP_FUN_BUY_DISCRIMINATOR = Buffer.from([102, 6, 61, 18, 1, 218, 235, 234]);
const PUMP_FUN_SELL_DISCRIMINATOR = Buffer.from([51, 230, 133, 164, 1, 127, 131, 173]);

function toBufferLike(data) {
  if (!data) return null;
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof Uint8Array) return Buffer.from(data);
  if (Array.isArray(data)) return Buffer.from(data);
  if (typeof data === 'string') {
    try {
      const buf = Buffer.from(data, 'base64');
      if (buf.length >= 8) return buf;
    } catch {}
    try {
      const buf = bs58.decode(data);
      if (buf.length >= 8) return buf;
    } catch {}
    return Buffer.from(data, 'utf8');
  }
  if (data?.data && Array.isArray(data.data)) {
    return Buffer.from(data.data);
  }
  return null;
}

function bufferStartsWith(buf, prefix) {
  if (!buf || !prefix || buf.length < prefix.length) return false;
  for (let i = 0; i < prefix.length; i += 1) {
    if (buf[i] !== prefix[i]) return false;
  }
  return true;
}

function normalizePubkey(value) {
  if (!value) return null;
  if (typeof value === 'string') return value;
  if (value instanceof PublicKey) return value.toBase58();
  if (value?.toBase58) {
    try {
      return value.toBase58();
    } catch {}
  }
  if (value instanceof Uint8Array) return bs58.encode(Buffer.from(value));
  if (Array.isArray(value)) {
    try {
      return bs58.encode(Buffer.from(value));
    } catch {}
  }
  return String(value);
}

function parseTransactionResponse(tx, programHint) {
  if (!tx) return null;
  const { meta, transaction, slot, blockTime } = tx;
  if (!meta || !transaction) return null;
  const message = transaction.message;
  if (!message) return null;

  let signature = null;
  if (Array.isArray(transaction.signatures) && transaction.signatures.length) {
    const sigEntry = transaction.signatures[0];
    if (typeof sigEntry === 'string') {
      signature = sigEntry;
    } else if (sigEntry instanceof Uint8Array || Array.isArray(sigEntry)) {
      signature = bs58.encode(Buffer.from(sigEntry));
    } else if (typeof sigEntry?.signature === 'string') {
      signature = sigEntry.signature;
    }
  }

  let accountKeys = [];
  try {
    if (typeof message.getAccountKeys === 'function') {
      const resolved = message.getAccountKeys({ accountKeysFromLookups: meta?.loadedAddresses });
      if (resolved) {
        const total =
          resolved.staticAccountKeys.length +
          (resolved.accountKeysFromLookups?.writable?.length || 0) +
          (resolved.accountKeysFromLookups?.readonly?.length || 0);
        for (let i = 0; i < total; i += 1) {
          const pk = resolved.get(i);
          const normalized = normalizePubkey(pk);
          if (normalized) accountKeys.push(normalized);
        }
      }
    }
  } catch (err) {
    console.error('[tx-parse] Account key resolution failed:', err.message);
  }

  if (!accountKeys.length && Array.isArray(message.accountKeys)) {
    accountKeys = message.accountKeys.map((key) => normalizePubkey(key)).filter(Boolean);
  }

  if (!accountKeys.length) return null;

  const payer = accountKeys[0];
  const compiledInstructions = message.compiledInstructions || message.instructions || [];
  const logMessages = meta.logMessages || meta.log_messages || [];

  let program = programHint || null;
  let side = null;

  for (const ix of compiledInstructions) {
    const programIdx = ix.programIdIndex ?? ix.program_id_index ?? ix.programIndex;
    const programId = typeof programIdx === 'number' ? accountKeys[programIdx] : null;
    if (!programId) continue;

    let candidateProgram = null;
    if (programId === PUMP_FUN_PROGRAM) {
      candidateProgram = 'pumpfun';
    } else if (programId === PUMPSWAP_PROGRAM_ID_STRING) {
      candidateProgram = 'pumpswap';
    }

    if (!candidateProgram) continue;
    program = candidateProgram;

    const dataBuf = toBufferLike(ix.data);
    if (dataBuf) {
      if (bufferStartsWith(dataBuf, PUMP_FUN_BUY_DISCRIMINATOR)) {
        side = 'buy';
      } else if (bufferStartsWith(dataBuf, PUMP_FUN_SELL_DISCRIMINATOR)) {
        side = 'sell';
      }
    }
  }

  if (!program) {
    for (const entry of logMessages || []) {
      if (!entry) continue;
      if (entry.includes(PUMP_FUN_PROGRAM)) {
        program = 'pumpfun';
        break;
      }
      if (entry.includes(PUMPSWAP_PROGRAM_ID_STRING)) {
        program = 'pumpswap';
        break;
      }
    }
  }

  if (!side) {
    for (const entry of logMessages || []) {
      if (/Instruction:\s*Buy/i.test(entry)) {
        side = 'buy';
        break;
      }
      if (/Instruction:\s*Sell/i.test(entry)) {
        side = 'sell';
        break;
      }
    }
  }

  if (!program) return null;

  const preBalances = meta.preBalances ?? meta.pre_balances ?? [];
  const postBalances = meta.postBalances ?? meta.post_balances ?? [];
  let solDeltaLamports = null;
  if (preBalances.length && postBalances.length) {
    const before = Number(preBalances[0]) || 0;
    const after = Number(postBalances[0]) || 0;
    solDeltaLamports = after - before;
  }

  const solAmount =
    solDeltaLamports != null ? Math.abs(solDeltaLamports) / LAMPORTS_PER_SOL : null;
  const direction = solDeltaLamports != null ? (solDeltaLamports < 0 ? 'out' : 'in') : null;

  const newMints = diffNewMints(meta);
  const postTokenBalances = meta.postTokenBalances ?? meta.post_token_balances ?? [];
  let mint = newMints[0] || null;
  if (!mint && postTokenBalances.length) {
    const ownerKey = payer;
    const ownedBalance = postTokenBalances.find((bal) => {
      const owner =
        bal?.owner ||
        bal?.accountOwner ||
        bal?.ownerPubkey ||
        bal?.owner_pubkey;
      return owner && normalizePubkey(owner) === ownerKey;
    });
    mint = sanitizeMint(ownedBalance?.mint) || sanitizeMint(postTokenBalances[0]?.mint);
  }

  return {
    signature,
    slot,
    timestamp: blockTime ? blockTime * 1000 : Date.now(),
    program,
    side: side || 'unknown',
    solAmount: solAmount != null ? Number(solAmount.toFixed(6)) : null,
    direction,
    mint,
    isNewCoin: newMints.length > 0,
    payer,
  };
}

function recordTransaction(txInfo) {
  if (!txInfo?.signature) return;
  if (seenTransactionSignatures.has(txInfo.signature)) return;
  recentTransactions.unshift(txInfo);
  seenTransactionSignatures.add(txInfo.signature);
  if (recentTransactions.length > MAX_RECENT_TRANSACTIONS) {
    const removed = recentTransactions.pop();
    if (removed?.signature) {
      seenTransactionSignatures.delete(removed.signature);
    }
  }
  broadcast({ type: 'transaction', transaction: txInfo });
}

function scheduleTransactionFetch(signature, program) {
  if (!signature || !program) return;
  if (pendingTransactionFetches.has(signature) || seenTransactionSignatures.has(signature)) return;
  pendingTransactionFetches.add(signature);
  fetchTransactionWithRetry(signature, program);
}

async function fetchTransactionWithRetry(signature, program, attempt = 0) {
  try {
    const tx = await connection.getTransaction(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0, // allow fetching versioned transactions
    });
    if (!tx) {
      throw new Error('Transaction not available yet');
    }
    const parsed = parseTransactionResponse(tx, program);
    if (parsed) {
      if (!parsed.signature) parsed.signature = signature;
      recordTransaction(parsed);
    }
    pendingTransactionFetches.delete(signature);
  } catch (err) {
    if (attempt < 4) {
      setTimeout(
        () => fetchTransactionWithRetry(signature, program, attempt + 1),
        500 * (attempt + 1),
      );
    } else {
      pendingTransactionFetches.delete(signature);
      console.error(
        `[tx-tracker] Failed to process ${program} tx ${signature}:`,
        err.message,
      );
    }
  }
}

function handleTransactionLog(log, program) {
  if (!log || log.err) return;
  const signature = log.signature;
  scheduleTransactionFetch(signature, program);
}

async function startTransactionLogWatchers() {
  try {
    const pumpfunSub = await connection.onLogs(
      PUMP_FUN_PROGRAM_ID,
      (log) => handleTransactionLog(log, 'pumpfun'),
      'processed',
    );
    transactionLogSubscriptions.push(pumpfunSub);
    console.log(`[tx-tracker] Listening to pump.fun logs (sub ${pumpfunSub})`);
  } catch (err) {
    console.error('[tx-tracker] Failed to subscribe to pump.fun logs:', err.message);
  }

  try {
    const pumpswapSub = await connection.onLogs(
      PUMPSWAP_PROGRAM_ID,
      (log) => handleTransactionLog(log, 'pumpswap'),
      'processed',
    );
    transactionLogSubscriptions.push(pumpswapSub);
    console.log(`[tx-tracker] Listening to PumpSwap logs (sub ${pumpswapSub})`);
  } catch (err) {
    console.error('[tx-tracker] Failed to subscribe to PumpSwap logs:', err.message);
  }
}

startTransactionLogWatchers();

// ---------- Yellowstone gRPC (detects new mints and graduations) ----------
if (GRPC_ADDR && GEYSER_PROTO_DIR) {
  try {
    const protoFiles = [
      path.join(GEYSER_PROTO_DIR, "geyser.proto"),
      path.join(GEYSER_PROTO_DIR, "solana-storage.proto"),
    ];
    const pkgDef = protoLoader.loadSync(protoFiles, {
      keepCase: true, longs: String, enums: String, defaults: true, oneofs: true,
    });
    const proto = grpc.loadPackageDefinition(pkgDef);
    const client = new proto.geyser.Geyser(GRPC_ADDR, grpc.credentials.createInsecure());

    const POOL_ACCOUNT_DISCRIMINATOR = Buffer.from([241, 154, 109, 4, 17, 177, 109, 188]);

    // --- Stream 1: For New pump.fun Mints (Transaction Subscription) ---
    function subscribeToTransactions() {
        const stream = client.Subscribe();
        const txFilter = {
            vote: false,
            failed: false,
            account_include: [PUMP_FUN_PROGRAM].filter(Boolean),
        };
        stream.write({
            commitment: 0, // PROCESSED
            transactions: { transactions: txFilter },
        });
        console.log("[geyser-tx] Subscribed to pump.fun transactions for new mints.");

        stream.on("data", async (msg) => {
            if (msg.transaction) {
                try {
                    const txu = msg.transaction.transaction || msg.transactions_status?.transaction;
                    if (!txu?.meta) return;

                    const newMints = diffNewMints(txu.meta);
                    for (const mint of newMints) {
                        addNewCoin(mint);
                    }
                } catch (txError) {
                    console.error('[geyser-tx] Error processing transaction:', txError.message);
                }
            }
        });

        stream.on("error", (e) => console.error("[geyser-tx] Subscription error:", e.message));
        stream.on("end", () => {
            console.log("[geyser-tx] Subscription ended. Reconnecting...");
            setTimeout(subscribeToTransactions, 1000);
        });
    }

    // --- Stream 2: For PumpSwap Graduations (Account Subscription) ---
    function subscribeToAccountUpdatesNew() {
      const candidateBuilders = [
        () => ({ accounts: { accounts: { owner: [PUMPSWAP_PROGRAM_ID_STRING] } } }),
        () => ({ accounts: { accounts: { owners: [PUMPSWAP_PROGRAM_ID_STRING] } } }),
        () => ({ accounts: { accounts: [ { owner: PUMPSWAP_PROGRAM_ID_STRING } ] } }),
        () => ({ accounts: { accounts: [ { owners: [PUMPSWAP_PROGRAM_ID_STRING] } ] } }),
        () => ({ accounts: { accounts: [ { program: PUMPSWAP_PROGRAM_ID_STRING } ] } }),
        () => ({ accounts: { owners: [PUMPSWAP_PROGRAM_ID_STRING] } }),
        () => ({ accounts: { programs: [PUMPSWAP_PROGRAM_ID_STRING] } }),
      ];

      let attempt = 0;

      const trySubscribe = () => {
        const stream = client.Subscribe();
        const req = { commitment: 0, ...candidateBuilders[attempt]() };
        const label = JSON.stringify(req.accounts);
        try { stream.write(req); } catch (e) { console.error('[geyser-pool] Write failed:', e.message); }
        console.log(`[geyser-pool] Subscribing with filter variant #${attempt + 1}: ${label}`);

        const extractData = (msg) => {
          try {
            const raw = msg?.account?.data || msg?.account?.account?.data || msg?.value?.account?.data || msg?.data;
            if (!raw) return null;
            return Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
          } catch { return null; }
        };

        stream.on('data', async (msg) => {
          try {
            const data = extractData(msg);
            if (!data || data.length < 200) return;
            if (data.subarray(0, 8).equals(POOL_ACCOUNT_DISCRIMINATOR)) {
              const baseMint = new PublicKey(data.slice(43, 43 + 32)).toBase58();
              const coin = trackedCoins.get(baseMint);
              if (coin && (coin.status === 'pump' || coin.status === 'graduating')) {
                console.log(`[geyser-pool] ✅ Graduation confirmed for ${coin.symbol || baseMint}`);
                const baseVault = new PublicKey(data.slice(139, 139 + 32)).toBase58();
                const quoteVault = new PublicKey(data.slice(171, 171 + 32)).toBase58();
                let poolAddress = null;
                try {
                  poolAddress = new PublicKey(msg.account?.pubkey || msg.value?.pubkey || msg.pubkey).toBase58();
                } catch (_) {
                  poolAddress = msg.account?.pubkey || msg.value?.pubkey || msg.pubkey || null;
                }
                await processGraduation(baseMint, baseVault, quoteVault, poolAddress);
              }
            }
          } catch (e) {
            console.error('[geyser-pool] Error processing account update:', e.message);
          }
        });

        stream.on('error', (e) => {
          const m = String(e?.message || e);
          console.error('[geyser-pool] Subscription error:', m);
          if (/INVALID_ARGUMENT|failed to create filter|at least one filter required/i.test(m) && attempt < candidateBuilders.length - 1) {
            attempt++;
            console.log(`[geyser-pool] Retrying with next filter variant (#${attempt + 1})...`);
            setTimeout(trySubscribe, 500);
          } else {
            setTimeout(trySubscribe, 1000);
          }
        });

        stream.on('end', () => {
          console.log('[geyser-pool] Subscription ended. Reconnecting...');
          setTimeout(trySubscribe, 1000);
        });
      };

      trySubscribe();
    }

    // Start both subscriptions
    subscribeToTransactions();
    subscribeToAccountUpdatesNew();

  } catch (e) {
    console.error("[geyser] Setup failed:", e.message);
  }
} else {
  console.log("[geyser] Not configured - missing GRPC_ADDR or GEYSER_PROTO_DIR");
}

// ---------- Enhanced Scheduled Jobs ----------

// Fetches the SOL price every 30 seconds
cron.schedule('*/30 * * * * *', async () => {
  await updateSOLPriceWithFallback();
});

// Regularly broadcast positions
cron.schedule('*/5 * * * * *', () => {
  broadcastAllPositions();
});

// Cleanup stale coins
cron.schedule("*/10 * * * *", () => {
  const hourAgo = Date.now() - (60 * 60 * 1000);
  let removed = 0;
  for (const [mint, coin] of trackedCoins.entries()) {
    if (coin.lastUpdated < hourAgo) {
      trackedCoins.delete(mint);
      
      const monitor = realTimePriceMonitors.get(mint);
      if (monitor) {
        connection.removeAccountChangeListener(monitor.subscriptionId);
        realTimePriceMonitors.delete(mint);
      }
      
      const interval = priceUpdateIntervals.get(mint);
      if (interval) {
        clearInterval(interval);
        priceUpdateIntervals.delete(mint);
      }
      
      removed++;
    }
  }
  if (removed > 0) {
    console.log(`[cleanup] Removed ${removed} stale coins and their monitors`);
    pushFullTopGainers();
    broadcastStats();
  }
});

// Health check for real-time monitors
cron.schedule("*/5 * * * *", () => {
  console.log(`[monitor-health] Active price monitors: ${realTimePriceMonitors.size}, Tracked coins: ${trackedCoins.size}, Auto-trading positions: ${autoTradingPositions.size}, Manual positions: ${manualPositions.size}`);
});

// ---------- Startup ----------

console.log('[init] Crypto Top Gainers Tracker (ULTRA-FAST REAL-TIME + AUTO-TRADING) started');
console.log(`[init] Tracking top ${topCoinsLimit} gainers`);
console.log(`[init] Min market cap: ${MIN_MCAP}`);
console.log(`[init] Max coins tracked: ${MAX_TRACKED}`);
console.log(`[init] SOL/USD price: ${solPriceUSD}`);
console.log(`[init] Real-time monitors active: ${realTimePriceMonitors.size}`);
console.log(`[init] Auto-trading: ${tradingService ? 'ENABLED' : 'DISABLED'}`);

let isShuttingDown = false;

/**
 * Handles all cleanup tasks on server shutdown.
 */
function handleShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log('[shutdown] Shutdown signal received. Cleaning up...');

  const jsonlPath = path.join(__dirname, 'logs', priceTicksFileName);
  const finalJsonPath = jsonlPath.replace('.jsonl', '.json');
  convertJsonlToJson(jsonlPath, finalJsonPath, trackedCoins);

  console.log('[shutdown] Removing price monitors...');
  for (const [mint, monitor] of realTimePriceMonitors.entries()) {
    try {
      if (Array.isArray(monitor.subscriptionId)) {
        monitor.subscriptionId.forEach(subId => {
          connection.removeAccountChangeListener(subId);
        });
      } else {
        connection.removeAccountChangeListener(monitor.subscriptionId);
      }
    } catch (e) {
      // Errors expected when connection is closing
    }
  }

  console.log('[shutdown] Removing transaction log subscriptions...');
  transactionLogSubscriptions.forEach((subId) => {
    connection.removeOnLogsListener(subId).catch(() => {});
  });

  for (const [mint, interval] of priceUpdateIntervals.entries()) {
    clearInterval(interval);
  }
  
  console.log('[shutdown] Cleanup complete. Exiting.');
  
  setTimeout(() => process.exit(0), 500);
}

process.on('SIGINT', handleShutdown);
process.on('SIGTERM', handleShutdown);
process.on('SIGHUP', handleShutdown);

// Sell all tokens helper endpoint
app.post('/api/trade/sell-all', async (req, res) => {
  if (!tradingService) {
    return res.status(400).json({ error: 'Trading wallet not configured' });
  }
  try {
    const { mint } = req.body || {};
    if (!mint) return res.status(400).json({ error: 'mint is required' });

    const mintPk = new PublicKey(mint);
    const ata = await getAssociatedTokenAddress(mintPk, wallet.publicKey, false, TOKEN_PROGRAM_ID, ASSOCIATED_TOKEN_PROGRAM_ID);
    const bal = await connection.getTokenAccountBalance(ata).catch(() => null);
    const amountStr = bal?.value?.amount;
    if (!amountStr) {
      return res.status(400).json({ error: 'No token account or zero balance' });
    }
    let amount = 0n;
    try { amount = BigInt(amountStr); } catch { amount = 0n; }
    if (amount <= 0n) {
      return res.status(400).json({ error: 'Zero balance' });
    }

    const coin = trackedCoins.get(mint);
    const trade = await tradeWithGraduationFallback({
      mint,
      side: 'sell',
      context: 'trade:sell-all',
      pumpfunParams: {
        mint,
        tokenAmount: amount,
        minSolOutput: 0n,
      },
      pumpswapParams: {
        pool: coin?.ammId || null,
        baseAmount: amount,
        minQuoteAmount: 0n,
      },
    });
    const { result, usedPumpswap, pool } = trade;

    if (coin) {
      coin.lastTxSig = result.signature;
      coin.lastUpdated = Date.now();
      if (usedPumpswap && pool) {
        coin.status = 'pumpswap';
        coin.ammId = pool;
      }
    }

    const manualPosition = manualPositions.get(mint);
    if (manualPosition) {
      manualPosition.status = 'closed';
      manualPosition.sellReason = 'Manual sell-all';
      manualPosition.sellSignature = result.signature;
      manualPosition.sellTime = Date.now();
      manualPosition.sellPrice = coin?.currentPrice ?? manualPosition.currentPrice ?? manualPosition.buyPrice;
      manualPosition.tokenAmountBaseUnits = '0';
      if (usedPumpswap && pool) {
        manualPosition.lastKnownPool = pool;
      }
      manualPositions.set(mint, manualPosition);
      broadcastManualPositionUpdate(mint);
      setTimeout(() => {
        manualPositions.delete(mint);
        broadcastManualPositions();
      }, 60000);
    }

    res.json({
      ok: true,
      signature: result.signature,
      route: usedPumpswap ? 'pumpswap' : 'pumpfun',
      pool: pool ?? null,
    });
  } catch (error) {
    const info = error.__solana || await describeSolanaError(error, {
      context: 'trade:sell-all',
      mint: req.body?.mint,
    });
    console.error('[sell-all] error:', info.message);
    if (info.logs) {
      console.error('[sell-all] logs:', info.logs);
    }
    res.status(500).json({
      error: info.message || error.message,
      reason: info.reason || error.code || null,
      logs: info.logs || null,
    });
  }
});
