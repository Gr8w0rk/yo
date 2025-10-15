import { 
  Connection, 
  PublicKey, 
  Transaction, 
  SystemProgram,
  SYSVAR_RENT_PUBKEY,
  SYSVAR_CLOCK_PUBKEY
} from '@solana/web3.js';
import anchor from '@coral-xyz/anchor';
const { AnchorProvider, Program, BN } = anchor;
import { 
  TOKEN_PROGRAM_ID, 
  ASSOCIATED_TOKEN_PROGRAM_ID, 
  getAssociatedTokenAddress,
  createAssociatedTokenAccountInstruction
} from '@solana/spl-token';

// PumpSwap Program ID
const PUMPSWAP_PROGRAM_ID = new PublicKey('pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA');

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
}

// IDL for PumpSwap program - Updated with correct account structure
const PUMP_SWAP_IDL = {
  "version": "0.1.0",
  "name": "pump_swap",
  "instructions": [
    {
      "name": "swap",
      "accounts": [
        {
          "name": "user",
          "isMut": true,
          "isSigner": true
        },
        {
          "name": "userTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "userSolAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "pool",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "poolTokenAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "poolSolAccount",
          "isMut": true,
          "isSigner": false
        },
        {
          "name": "tokenMint",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "tokenProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "systemProgram",
          "isMut": false,
          "isSigner": false
        },
        {
          "name": "rent",
          "isMut": false,
          "isSigner": false
        }
      ],
      "args": [
        {
          "name": "amountIn",
          "type": "u64"
        },
        {
          "name": "minAmountOut",
          "type": "u64"
        }
      ]
    }
  ],
  "accounts": [
    {
      "name": "Pool",
      "type": {
        "kind": "struct",
        "fields": [
          {
            "name": "tokenMint",
            "type": "publicKey"
          },
          {
            "name": "tokenVault",
            "type": "publicKey"
          },
          {
            "name": "solVault",
            "type": "publicKey"
          },
          {
            "name": "tokenReserve",
            "type": "u64"
          },
          {
            "name": "solReserve",
            "type": "u64"
          },
          {
            "name": "feeNumerator",
            "type": "u64"
          },
          {
            "name": "feeDenominator",
            "type": "u64"
          }
        ]
      }
    }
  ]
};

export function createPumpTradingService(config) {
  const { connection, wallet, commitment = 'confirmed' } = config;
  
  if (!connection || !wallet) {
    throw new Error('Connection and wallet are required for PumpTradingService');
  }

  // Verify RPC connection
  console.log(`[pump-trading] Initializing with RPC: ${connection.rpcEndpoint}`);
  console.log(`[pump-trading] Wallet: ${wallet.publicKey.toBase58()}`);
  console.log(`[pump-trading] Commitment: ${commitment}`);

  // Create Anchor provider
  const provider = new AnchorProvider(connection, wallet, { commitment });
  const program = new Program(PUMP_SWAP_IDL, PUMPSWAP_PROGRAM_ID, provider);

  /**
   * Get pool account data
   */
  async function getPoolData(poolAddress) {
    try {
      const poolAccount = await connection.getAccountInfo(new PublicKey(poolAddress));
      if (!poolAccount) {
        throw new Error('Pool account not found');
      }
      
      // Parse pool data based on actual PumpSwap structure
      const data = poolAccount.data;
      
      // PumpSwap pool structure (based on actual program):
      // 0-8: discriminator
      // 8-40: token_mint (32 bytes)
      // 40-72: token_vault (32 bytes) 
      // 72-104: sol_vault (32 bytes)
      // 104-112: token_reserve (8 bytes)
      // 112-120: sol_reserve (8 bytes)
      // 120-128: fee_numerator (8 bytes)
      // 128-136: fee_denominator (8 bytes)
      
      const tokenMint = new PublicKey(data.slice(8, 40));
      const tokenVault = new PublicKey(data.slice(40, 72));
      const solVault = new PublicKey(data.slice(72, 104));
      const tokenReserve = new BN(data.slice(104, 112), 'le');
      const solReserve = new BN(data.slice(112, 120), 'le');
      const feeNumerator = new BN(data.slice(120, 128), 'le');
      const feeDenominator = new BN(data.slice(128, 136), 'le');

      console.log(`[pump-trading] Pool data parsed:`, {
        tokenMint: tokenMint.toBase58(),
        tokenVault: tokenVault.toBase58(),
        solVault: solVault.toBase58(),
        tokenReserve: tokenReserve.toString(),
        solReserve: solReserve.toString(),
        feeNumerator: feeNumerator.toString(),
        feeDenominator: feeDenominator.toString()
      });

      return {
        tokenMint,
        tokenVault,
        solVault,
        tokenReserve,
        solReserve,
        feeNumerator,
        feeDenominator
      };
    } catch (error) {
      console.error('[pump-trading] Error getting pool data:', error);
      throw new Error(`Failed to get pool data: ${error.message}`);
    }
  }

  /**
   * Calculate swap amount out given amount in
   */
  function calculateSwapAmountOut(amountIn, reserveIn, reserveOut, feeNumerator, feeDenominator) {
    const amountInWithFee = amountIn.mul(feeDenominator.sub(feeNumerator));
    const numerator = amountInWithFee.mul(reserveOut);
    const denominator = reserveIn.mul(feeDenominator).add(amountInWithFee);
    return numerator.div(denominator);
  }

  /**
   * Calculate swap amount in given amount out
   */
  function calculateSwapAmountIn(amountOut, reserveIn, reserveOut, feeNumerator, feeDenominator) {
    const numerator = reserveIn.mul(amountOut).mul(feeDenominator);
    const denominator = reserveOut.sub(amountOut).mul(feeDenominator.sub(feeNumerator));
    return numerator.div(denominator).add(new BN(1)); // Add 1 for rounding
  }

  /**
   * Get associated token address for user
   */
  async function getUserTokenAddress(mint, user) {
    return await getAssociatedTokenAddress(mint, user);
  }

  /**
   * Execute a swap transaction
   */
  async function executeSwap(params) {
    const {
      poolAddress,
      tokenMint,
      amountIn,
      minAmountOut,
      isBuy, // true for SOL -> Token, false for Token -> SOL
      slippage = 0.01 // 1% default slippage
    } = params;

    try {
      console.log(`[pump-trading] Executing ${isBuy ? 'buy' : 'sell'} swap:`, {
        poolAddress,
        tokenMint: tokenMint.toString(),
        amountIn: amountIn.toString(),
        minAmountOut: minAmountOut.toString()
      });

      const poolData = await getPoolData(poolAddress);
      const user = wallet.publicKey;
      
      // Get user token account
      const userTokenAccount = await getUserTokenAddress(new PublicKey(tokenMint), user);
      
      // Calculate expected amount out
      const expectedAmountOut = isBuy 
        ? calculateSwapAmountOut(amountIn, poolData.solReserve, poolData.tokenReserve, poolData.feeNumerator, poolData.feeDenominator)
        : calculateSwapAmountOut(amountIn, poolData.tokenReserve, poolData.solReserve, poolData.feeNumerator, poolData.feeDenominator);

      // Apply slippage
      const slippageMultiplier = new BN(Math.floor((1 - slippage) * 1000));
      const slippageDivisor = new BN(1000);
      const minAmountOutWithSlippage = expectedAmountOut.mul(slippageMultiplier).div(slippageDivisor);

      const finalMinAmountOut = minAmountOut ? BN.max(minAmountOut, minAmountOutWithSlippage) : minAmountOutWithSlippage;

      console.log(`[pump-trading] Swap details:`, {
        user: user.toBase58(),
        userTokenAccount: userTokenAccount.toBase58(),
        pool: poolAddress,
        poolTokenAccount: poolData.tokenVault.toBase58(),
        poolSolAccount: poolData.solVault.toBase58(),
        tokenMint: tokenMint.toString(),
        amountIn: amountIn.toString(),
        expectedAmountOut: expectedAmountOut.toString(),
        minAmountOut: finalMinAmountOut.toString()
      });

      // Create swap instruction
      const swapInstruction = await program.methods
        .swap(amountIn, finalMinAmountOut)
        .accounts({
          user,
          userTokenAccount,
          userSolAccount: user,
          pool: new PublicKey(poolAddress),
          poolTokenAccount: poolData.tokenVault,
          poolSolAccount: poolData.solVault,
          tokenMint: new PublicKey(tokenMint),
          tokenProgram: TOKEN_PROGRAM_ID,
          systemProgram: SystemProgram.programId,
          rent: SYSVAR_RENT_PUBKEY
        })
        .instruction();

      // Create transaction
      const transaction = new Transaction();
      
      // Check if user has token account, create if needed for buys
      if (isBuy) {
        const tokenAccountInfo = await connection.getAccountInfo(userTokenAccount);
        if (!tokenAccountInfo) {
          console.log(`[pump-trading] Creating token account for user: ${userTokenAccount.toBase58()}`);
          // Create associated token account instruction
          const createTokenAccountInstruction = createAssociatedTokenAccountInstruction(
            user, // payer
            userTokenAccount, // ata
            user, // owner
            new PublicKey(tokenMint) // mint
          );
          transaction.add(createTokenAccountInstruction);
        }
      }
      
      transaction.add(swapInstruction);

      // Get recent blockhash
      const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash();
      transaction.recentBlockhash = blockhash;
      transaction.feePayer = user;

      // Sign and send transaction
      const signedTransaction = await wallet.signTransaction(transaction);
      const signature = await connection.sendRawTransaction(signedTransaction.serialize(), {
        skipPreflight: false,
        preflightCommitment: commitment
      });

      console.log(`[pump-trading] Swap transaction sent: ${signature}`);

      // Wait for confirmation
      const confirmation = await connection.confirmTransaction(signature, commitment);
      
      if (confirmation.value.err) {
        throw new Error(`Transaction failed: ${confirmation.value.err}`);
      }

      return {
        signature,
        lastValidBlockHeight,
        amountIn: amountIn.toString(),
        expectedAmountOut: expectedAmountOut.toString(),
        minAmountOut: finalMinAmountOut.toString(),
        pool: poolAddress
      };

    } catch (error) {
      console.error('[pump-trading] Swap execution failed:', error);
      throw error;
    }
  }

  /**
   * Buy tokens with SOL
   */
  async function buyPumpswap(params) {
    try {
      const {
        pool,
        baseAmount,
        maxQuoteAmount,
        trackVolume = false,
        priorityFeeLamports = null
      } = params;

      console.log(`[pump-trading] buyPumpswap called with params:`, {
        pool,
        baseAmount: baseAmount?.toString(),
        maxQuoteAmount: maxQuoteAmount?.toString(),
        trackVolume,
        priorityFeeLamports
      });

      if (!pool || !baseAmount || !maxQuoteAmount) {
        throw new Error('Missing required parameters: pool, baseAmount, maxQuoteAmount');
      }

      // Get token mint from pool data
      const poolData = await getPoolData(pool);
      const tokenMint = poolData.tokenMint.toString();

      console.log(`[pump-trading] Pool data retrieved:`, {
        tokenMint,
        tokenVault: poolData.tokenVault.toBase58(),
        solVault: poolData.solVault.toBase58(),
        tokenReserve: poolData.tokenReserve.toString(),
        solReserve: poolData.solReserve.toString()
      });

      const amountIn = new BN(baseAmount); // SOL amount
      const minAmountOut = new BN(maxQuoteAmount); // Min tokens to receive

      return await executeSwap({
        poolAddress: pool,
        tokenMint,
        amountIn,
        minAmountOut,
        isBuy: true,
        slippage: 0.01
      });
    } catch (error) {
      console.error('[pump-trading] buyPumpswap failed:', error);
      throw new Error(`PumpSwap buy failed: ${error.message}`);
    }
  }

  /**
   * Sell tokens for SOL
   */
  async function sellPumpswap(params) {
    try {
      const {
        pool,
        baseAmount,
        minQuoteAmount,
        priorityFeeLamports = null
      } = params;

      console.log(`[pump-trading] sellPumpswap called with params:`, {
        pool,
        baseAmount: baseAmount?.toString(),
        minQuoteAmount: minQuoteAmount?.toString(),
        priorityFeeLamports
      });

      if (!pool || !baseAmount || !minQuoteAmount) {
        throw new Error('Missing required parameters: pool, baseAmount, minQuoteAmount');
      }

      // Get token mint from pool data
      const poolData = await getPoolData(pool);
      const tokenMint = poolData.tokenMint.toString();

      console.log(`[pump-trading] Pool data retrieved:`, {
        tokenMint,
        tokenVault: poolData.tokenVault.toBase58(),
        solVault: poolData.solVault.toBase58(),
        tokenReserve: poolData.tokenReserve.toString(),
        solReserve: poolData.solReserve.toString()
      });

      const amountIn = new BN(baseAmount); // Token amount
      const minAmountOut = new BN(minQuoteAmount); // Min SOL to receive

      return await executeSwap({
        poolAddress: pool,
        tokenMint,
        amountIn,
        minAmountOut,
        isBuy: false,
        slippage: 0.01
      });
    } catch (error) {
      console.error('[pump-trading] sellPumpswap failed:', error);
      throw new Error(`PumpSwap sell failed: ${error.message}`);
    }
  }

  /**
   * Get pool info for a token
   */
  async function getPoolInfo(tokenMint) {
    try {
      // First try to get pool using PDA derivation
      const poolPDA = derivePumpSwapPoolPDA(tokenMint);
      const poolAccount = await connection.getAccountInfo(poolPDA, 'processed');
      
      if (poolAccount) {
        console.log(`[pump-trading] Found pool using PDA: ${poolPDA.toBase58()}`);
        const poolData = await getPoolData(poolPDA.toBase58());
        return {
          address: poolPDA.toBase58(),
          ...poolData
        };
      }

      // Fallback to program accounts search
      const mint = new PublicKey(tokenMint);
      const accounts = await connection.getProgramAccounts(PUMPSWAP_PROGRAM_ID, {
        filters: [
          {
            dataSize: 200 // Adjust based on actual account size
          },
          {
            memcmp: {
              offset: 8, // tokenMint offset (after 8-byte discriminator)
              bytes: mint.toBase58()
            }
          }
        ]
      });

      if (accounts.length === 0) {
        throw new Error('Pool not found for token');
      }

      const poolAccount = accounts[0];
      const poolData = await getPoolData(poolAccount.pubkey.toString());

      return {
        address: poolAccount.pubkey.toString(),
        ...poolData
      };
    } catch (error) {
      console.error('[pump-trading] Error getting pool info:', error);
      throw error;
    }
  }

  /**
   * Get the correct pool address for a graduated coin
   */
  async function getGraduatedPoolAddress(tokenMint) {
    try {
      // First try PDA derivation
      const poolPDA = derivePumpSwapPoolPDA(tokenMint);
      const poolAccount = await connection.getAccountInfo(poolPDA, 'processed');
      
      if (poolAccount) {
        console.log(`[pump-trading] Found graduated pool using PDA: ${poolPDA.toBase58()}`);
        return poolPDA.toBase58();
      }

      // Fallback to program accounts search
      const mint = new PublicKey(tokenMint);
      const accounts = await connection.getProgramAccounts(PUMPSWAP_PROGRAM_ID, {
        filters: [
          {
            dataSize: 200 // Adjust based on actual account size
          },
          {
            memcmp: {
              offset: 8, // tokenMint offset (after 8-byte discriminator)
              bytes: mint.toBase58()
            }
          }
        ]
      });

      if (accounts.length > 0) {
        const poolAddress = accounts[0].pubkey.toBase58();
        console.log(`[pump-trading] Found graduated pool via search: ${poolAddress}`);
        return poolAddress;
      }

      throw new Error('No PumpSwap pool found for graduated token');
    } catch (error) {
      console.error('[pump-trading] Error finding graduated pool:', error);
      throw error;
    }
  }

  /**
   * Test RPC connection and verify PumpSwap program access
   */
  async function testRPCConnection() {
    try {
      console.log('[pump-trading] Testing RPC connection...');
      
      // Test basic connection
      const version = await connection.getVersion();
      console.log(`[pump-trading] RPC version: ${version['solana-core']}`);
      
      // Test PumpSwap program access
      const programAccount = await connection.getAccountInfo(PUMPSWAP_PROGRAM_ID, 'processed');
      if (!programAccount) {
        throw new Error('PumpSwap program not found on this RPC');
      }
      console.log(`[pump-trading] PumpSwap program found: ${PUMPSWAP_PROGRAM_ID.toBase58()}`);
      
      // Test program accounts access
      const accounts = await connection.getProgramAccounts(PUMPSWAP_PROGRAM_ID, {
        dataSlice: { offset: 0, length: 8 }, // Just get discriminator
        limit: 1
      });
      console.log(`[pump-trading] Found ${accounts.length} PumpSwap accounts`);
      
      return true;
    } catch (error) {
      console.error('[pump-trading] RPC connection test failed:', error);
      throw error;
    }
  }

  return {
    buyPumpswap,
    sellPumpswap,
    getPoolInfo,
    getPoolData,
    getGraduatedPoolAddress,
    testRPCConnection,
    calculateSwapAmountOut,
    calculateSwapAmountIn
  };
}
