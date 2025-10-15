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

// IDL for PumpSwap program (simplified - you may need to update this based on actual IDL)
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
      
      // Parse pool data based on IDL structure
      const data = poolAccount.data;
      const tokenMint = new PublicKey(data.slice(0, 32));
      const tokenVault = new PublicKey(data.slice(32, 64));
      const solVault = new PublicKey(data.slice(64, 96));
      const tokenReserve = new BN(data.slice(96, 104), 'le');
      const solReserve = new BN(data.slice(104, 112), 'le');
      const feeNumerator = new BN(data.slice(112, 120), 'le');
      const feeDenominator = new BN(data.slice(120, 128), 'le');

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
          systemProgram: SystemProgram.programId
        })
        .instruction();

      // Create transaction
      const transaction = new Transaction();
      
      // Check if user has token account, create if needed for buys
      if (isBuy) {
        const tokenAccountInfo = await connection.getAccountInfo(userTokenAccount);
        if (!tokenAccountInfo) {
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

      if (!pool || !baseAmount || !maxQuoteAmount) {
        throw new Error('Missing required parameters: pool, baseAmount, maxQuoteAmount');
      }

      // Get token mint from pool data
      const poolData = await getPoolData(pool);
      const tokenMint = poolData.tokenMint.toString();

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

      if (!pool || !baseAmount || !minQuoteAmount) {
        throw new Error('Missing required parameters: pool, baseAmount, minQuoteAmount');
      }

      // Get token mint from pool data
      const poolData = await getPoolData(pool);
      const tokenMint = poolData.tokenMint.toString();

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
      const mint = new PublicKey(tokenMint);
      
      // Find pool account by token mint
      const accounts = await connection.getProgramAccounts(PUMPSWAP_PROGRAM_ID, {
        filters: [
          {
            dataSize: 200 // Adjust based on actual account size
          },
          {
            memcmp: {
              offset: 0, // tokenMint offset
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

  return {
    buyPumpswap,
    sellPumpswap,
    getPoolInfo,
    getPoolData,
    calculateSwapAmountOut,
    calculateSwapAmountIn
  };
}
