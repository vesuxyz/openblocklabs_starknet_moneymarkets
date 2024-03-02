WITH TOKENS AS (
    SELECT
        'USDC' AS TOKEN_SYMBOL,
        '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8' AS TOKEN0_ADDRESS, -- Real token
        'zUSDC' AS ZTOKEN_SYMBOL,
        '0x047ad51726d891f972e74e4ad858a261b43869f7126ce7436ee0b2529a98f486' AS TOKEN1_ADDRESS, -- zToken
        6 AS TOKEN_DECIMALS
    UNION ALL
    SELECT
        'USDT',
        '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8',
        'zUSDT',
        '0x00811d8da5dc8a2206ea7fd0b28627c2d77280a515126e62baa4d78e22714c4a',
        6
    UNION ALL
    SELECT
        'DAI',
        '0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3',
        'zDAI',
        '0x062fa7afe1ca2992f8d8015385a279f49fad36299754fb1e9866f4f052289376',
        18
    UNION ALL
    SELECT
        'wBTC',
        '0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac',
        'zwBTC',
        '0x02b9ea3acdb23da566cee8e8beae3125a1458e720dea68c4a9a7a2d8eb5bbb4a',
        8
    UNION ALL
    SELECT
        'ETH',
        '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7',
        'zETH',
        '0x01b5bd713e72fdc5d63ffd83762f81297f6175a5e0a4771cdadbc1dd5fe72cb1',
        18
    UNION ALL
    SELECT
        'wstETH',
        '0x042b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2',
        'zwstETH',
        '0x0536aa7e01ecc0235ca3e29da7b5ad5b12cb881e29034d87a4290edbb20b7c28',
        18
    UNION ALL
    SELECT
        'STRK',
        '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d',
        'zSTRK',
        '0x06d8fa671ef84f791b7f601fa79fea8f6ceb70b5fa84189e3159d532162efc21',
        18
),
ZKLEND_EVENTS AS (
    SELECT
        BLOCK_NUMBER,
        BLOCK_TIMESTAMP,
        TX_HASH,
        TX_ID,
        CONTRACT_ADDRESS,
        EVENT_NAME,
        PARAMETERS
    FROM STARKNET_DATA_WAREHOUSE__T0.STARKNET.EVENTS
    WHERE CONTRACT_ADDRESS = '0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05'
        AND EVENT_NAME NOT IN (
            'BorrowFactorUpdate',
            'CollateralDisabled',
            'CollateralEnabled',
            'ContractUpgraded',
            'NewReserve',
            'OwnershipTransferred',
            'TreasuryUpdate'
            )
),
ZKLEND_INTEREST_RATE_SYNC AS (
    SELECT
        TX_HASH,
        PARAMETERS
    FROM ZKLEND_EVENTS
    WHERE EVENT_NAME = 'InterestRatesSync'
),
ZKLEND_ACCUMULATOR_SYNC AS (
    SELECT
        TX_HASH,
        PARAMETERS
    FROM ZKLEND_EVENTS
    WHERE EVENT_NAME = 'AccumulatorsSync'
),
ZKLEND_WITHDRAWAL_RAW AS (
    SELECT
        ev.BLOCK_NUMBER,
        ev.BLOCK_TIMESTAMP,
        ev.TX_HASH,
        ev.TX_ID,
        parse_json(ev.PARAMETERS):token::varchar AS TOKEN0_ADDRESS, -- REAL TOKEN
        t.TOKEN1_ADDRESS AS TOKEN1_ADDRESS, -- zToken
        ev.CONTRACT_ADDRESS,
        ev.EVENT_NAME,
        parse_json(ev.PARAMETERS):user::varchar AS USER_ADDRESS,
        ev.CONTRACT_ADDRESS AS FROM_ADDRESS, -- Sender
        parse_json(ev.PARAMETERS):user::varchar AS TO_ADDRESS, -- Receiver
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN0_RAW_AMOUNT,
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN1_RAW_AMOUNT,
        t.TOKEN_DECIMALS AS TOKEN0_DECIMALS,
        t.TOKEN_DECIMALS AS TOKEN1_DECIMALS
    FROM ZKLEND_EVENTS ev
    LEFT JOIN TOKENS t ON parse_json(ev.PARAMETERS):token::varchar = t.TOKEN0_ADDRESS
    WHERE ev.EVENT_NAME = 'Withdrawal'
        AND parse_json(ev.PARAMETERS):token::varchar IN (SELECT TOKEN0_ADDRESS FROM TOKENS)
),
WITHDRAWAL_INDEX_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_accumulator::number AS LENDING_INDEX_RAW,
        parse_json(PARAMETERS):debt_accumulator::number AS BORROW_INDEX_RAW
    FROM ZKLEND_ACCUMULATOR_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_WITHDRAWAL_RAW)
),
COMPLETE_WITHDRAW AS (
    SELECT
        r.BLOCK_NUMBER,
        r.BLOCK_TIMESTAMP,
        r.TX_HASH,
        r.TX_ID,
        r.TOKEN0_ADDRESS, -- REAL TOKEN
        r.TOKEN1_ADDRESS, -- zToken
        r.CONTRACT_ADDRESS,
        'WITHDRAW' AS EVENT_NAME,
        r.USER_ADDRESS,
        r.FROM_ADDRESS, -- Sender
        r.TO_ADDRESS, -- Receiver
        -1 * r.TOKEN0_RAW_AMOUNT AS TOKEN0_RAW_AMOUNT,
        r.TOKEN1_RAW_AMOUNT / (idx.LENDING_INDEX_RAW / POW(10,27)) AS TOKEN1_RAW_AMOUNT,
        idx.LENDING_INDEX_RAW,
        NULL AS LENDING_INDEX_RAW_1,
        idx.BORROW_INDEX_RAW,
        r.TOKEN0_DECIMALS,
        r.TOKEN1_DECIMALS
    FROM ZKLEND_WITHDRAWAL_RAW r
    LEFT JOIN WITHDRAWAL_INDEX_RATES idx
        ON r.TX_HASH = idx.TX_HASH
            AND r.TOKEN0_ADDRESS = idx.TOKEN_ADDRESS
),
ZKLEND_DEPOSIT_RAW AS (
    SELECT
        ev.BLOCK_NUMBER,
        ev.BLOCK_TIMESTAMP,
        ev.TX_HASH,
        ev.TX_ID,
        parse_json(ev.PARAMETERS):token::varchar AS TOKEN0_ADDRESS, -- REAL TOKEN
        t.TOKEN1_ADDRESS AS TOKEN1_ADDRESS, -- zToken
        ev.CONTRACT_ADDRESS,
        ev.EVENT_NAME,
        parse_json(ev.PARAMETERS):user::varchar AS USER_ADDRESS,
        parse_json(ev.PARAMETERS):user::varchar AS FROM_ADDRESS, -- Sender
        ev.CONTRACT_ADDRESS AS TO_ADDRESS, -- Receiver
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN0_RAW_AMOUNT,
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN1_RAW_AMOUNT,
        t.TOKEN_DECIMALS AS TOKEN0_DECIMALS,
        t.TOKEN_DECIMALS AS TOKEN1_DECIMALS
    FROM ZKLEND_EVENTS ev
    LEFT JOIN TOKENS t ON parse_json(ev.PARAMETERS):token::varchar = t.TOKEN0_ADDRESS
    WHERE EVENT_NAME = 'Deposit'
        AND parse_json(ev.PARAMETERS):token::varchar IN (SELECT TOKEN0_ADDRESS FROM TOKENS)
),
DEPOSIT_INTEREST_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_rate::number AS LENDING_RATE_RAW,
        parse_json(PARAMETERS):borrowing_rate::number AS BORROW_RATE_RAW
    FROM ZKLEND_INTEREST_RATE_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_DEPOSIT_RAW)
),
DEPOSIT_INDEX_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_accumulator::number AS LENDING_INDEX_RAW,
        parse_json(PARAMETERS):debt_accumulator::number AS BORROW_INDEX_RAW
    FROM ZKLEND_ACCUMULATOR_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_DEPOSIT_RAW)
),
COMPLETE_DEPOSIT AS (
    SELECT
        r.BLOCK_NUMBER,
        r.BLOCK_TIMESTAMP,
        r.TX_HASH,
        r.TX_ID,
        r.TOKEN0_ADDRESS,
        r.TOKEN1_ADDRESS,
        r.CONTRACT_ADDRESS,
        'SUPPLY' AS EVENT_NAME,
        r.USER_ADDRESS,
        r.FROM_ADDRESS,
        r.TO_ADDRESS,
        r.TOKEN0_RAW_AMOUNT,
        -1 * r.TOKEN1_RAW_AMOUNT / (idx.LENDING_INDEX_RAW / POW(10,27)) AS TOKEN1_RAW_AMOUNT,
        idx.LENDING_INDEX_RAW,
        NULL AS LENDING_INDEX_RAW_1,
        idx.BORROW_INDEX_RAW,
        r.TOKEN0_DECIMALS,
        r.TOKEN1_DECIMALS
    FROM ZKLEND_DEPOSIT_RAW r
    LEFT JOIN DEPOSIT_INDEX_RATES idx
        ON r.TX_HASH = idx.TX_HASH
            AND r.TOKEN0_ADDRESS = idx.TOKEN_ADDRESS
),
ZKLEND_BORROWING_RAW AS (
    SELECT
        ev.BLOCK_NUMBER,
        ev.BLOCK_TIMESTAMP,
        ev.TX_HASH,
        ev.TX_ID,
        parse_json(ev.PARAMETERS):token::varchar AS TOKEN0_ADDRESS, -- REAL TOKEN
        t.TOKEN1_ADDRESS AS TOKEN1_ADDRESS, -- zToken
        ev.CONTRACT_ADDRESS,
        ev.EVENT_NAME,
        parse_json(ev.PARAMETERS):user::varchar AS USER_ADDRESS,
        ev.CONTRACT_ADDRESS AS FROM_ADDRESS, -- Sender
        parse_json(ev.PARAMETERS):user::varchar AS TO_ADDRESS, -- Borrower
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN0_RAW_AMOUNT,
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN1_RAW_AMOUNT,
        t.TOKEN_DECIMALS AS TOKEN0_DECIMALS,
        t.TOKEN_DECIMALS AS TOKEN1_DECIMALS
    FROM ZKLEND_EVENTS ev
    LEFT JOIN TOKENS t ON parse_json(ev.PARAMETERS):token::varchar = t.TOKEN0_ADDRESS
    WHERE EVENT_NAME = 'Borrowing'
        AND parse_json(ev.PARAMETERS):token::varchar IN (SELECT TOKEN0_ADDRESS FROM TOKENS)
),
BORROWING_INTEREST_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_rate::number AS LENDING_RATE_RAW,
        parse_json(PARAMETERS):borrowing_rate::number AS BORROW_RATE_RAW
    FROM ZKLEND_INTEREST_RATE_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_BORROWING_RAW)
),
BORROWING_INDEX_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_accumulator::number AS LENDING_INDEX_RAW,
        parse_json(PARAMETERS):debt_accumulator::number AS BORROW_INDEX_RAW
    FROM ZKLEND_ACCUMULATOR_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_BORROWING_RAW)
),
COMPLETE_BORROWING AS (
    SELECT
        r.BLOCK_NUMBER,
        r.BLOCK_TIMESTAMP,
        r.TX_HASH,
        r.TX_ID,
        r.TOKEN0_ADDRESS,
        r.TOKEN1_ADDRESS,
        r.CONTRACT_ADDRESS,
        'BORROW' AS EVENT_NAME,
        r.USER_ADDRESS,
        r.FROM_ADDRESS,
        r.TO_ADDRESS,
        -1 * r.TOKEN0_RAW_AMOUNT AS TOKEN0_RAW_AMOUNT,
        r.TOKEN1_RAW_AMOUNT / (idx.BORROW_INDEX_RAW / POW(10,27)) AS TOKEN1_RAW_AMOUNT,
        idx.LENDING_INDEX_RAW,
        NULL AS LENDING_INDEX_RAW_1,
        idx.BORROW_INDEX_RAW,
        r.TOKEN0_DECIMALS,
        r.TOKEN1_DECIMALS
    FROM ZKLEND_BORROWING_RAW r
    LEFT JOIN BORROWING_INDEX_RATES idx
        ON r.TX_HASH = idx.TX_HASH
            AND r.TOKEN0_ADDRESS = idx.TOKEN_ADDRESS
),
ZKLEND_REPAYMENT_RAW AS (
    SELECT
        ev.BLOCK_NUMBER,
        ev.BLOCK_TIMESTAMP,
        ev.TX_HASH,
        ev.TX_ID,
        parse_json(ev.PARAMETERS):token::varchar AS TOKEN0_ADDRESS, -- REAL TOKEN
        t.TOKEN1_ADDRESS AS TOKEN1_ADDRESS, -- zToken
        ev.CONTRACT_ADDRESS,
        ev.EVENT_NAME,
        parse_json(ev.PARAMETERS):beneficiary::varchar AS USER_ADDRESS, -- Borrower
        parse_json(ev.PARAMETERS):repayer::varchar AS FROM_ADDRESS,  -- Repayer
        ev.CONTRACT_ADDRESS AS TO_ADDRESS, -- Sender
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN0_RAW_AMOUNT,
        parse_json(ev.PARAMETERS):face_amount::number AS TOKEN1_RAW_AMOUNT,
        t.TOKEN_DECIMALS AS TOKEN0_DECIMALS,
        t.TOKEN_DECIMALS AS TOKEN1_DECIMALS
    FROM ZKLEND_EVENTS ev
    LEFT JOIN TOKENS t ON parse_json(ev.PARAMETERS):token::varchar = t.TOKEN0_ADDRESS
    WHERE EVENT_NAME = 'Repayment'
        AND parse_json(ev.PARAMETERS):token::varchar IN (SELECT TOKEN0_ADDRESS FROM TOKENS)
),
REPAYMENT_INDEX_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_accumulator::number AS LENDING_INDEX_RAW,
        parse_json(PARAMETERS):debt_accumulator::number AS BORROW_INDEX_RAW
    FROM ZKLEND_ACCUMULATOR_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_REPAYMENT_RAW)
),
COMPLETE_REPAYMENT AS (
    SELECT
        r.BLOCK_NUMBER,
        r.BLOCK_TIMESTAMP,
        r.TX_HASH,
        r.TX_ID,
        r.TOKEN0_ADDRESS,
        r.TOKEN1_ADDRESS,
        r.CONTRACT_ADDRESS,
        'REPAY' AS EVENT_NAME,
        r.USER_ADDRESS,
        r.FROM_ADDRESS,
        r.TO_ADDRESS,
        r.TOKEN0_RAW_AMOUNT,
        -1 * r.TOKEN1_RAW_AMOUNT / (idx.BORROW_INDEX_RAW / POW(10,27)) AS TOKEN1_RAW_AMOUNT,
        idx.LENDING_INDEX_RAW,
        NULL AS LENDING_INDEX_RAW_1,
        idx.BORROW_INDEX_RAW,
        r.TOKEN0_DECIMALS,
        r.TOKEN1_DECIMALS
    FROM ZKLEND_REPAYMENT_RAW r
    LEFT JOIN REPAYMENT_INDEX_RATES idx
        ON r.TX_HASH = idx.TX_HASH
            AND r.TOKEN0_ADDRESS = idx.TOKEN_ADDRESS
),
ZKLEND_LIQUIDATION_RAW AS (
    SELECT
        ev.BLOCK_NUMBER,
        ev.BLOCK_TIMESTAMP,
        ev.TX_HASH,
        ev.TX_ID,
        parse_json(ev.PARAMETERS):debt_token::varchar AS TOKEN0_ADDRESS,
        ev.CONTRACT_ADDRESS,
        ev.EVENT_NAME,
        parse_json(ev.PARAMETERS):user::varchar AS USER_ADDRESS,
        parse_json(ev.PARAMETERS):liquidator::varchar AS FROM_ADDRESS,
        parse_json(ev.PARAMETERS):user::varchar AS TO_ADDRESS,
        parse_json(ev.PARAMETERS):debt_face_amount::number AS TOKEN0_RAW_AMOUNT,
        t.TOKEN_DECIMALS AS TOKEN0_DECIMALS,
        ev.PARAMETERS
    FROM ZKLEND_EVENTS ev
    LEFT JOIN TOKENS t ON parse_json(ev.PARAMETERS):debt_token::varchar = t.TOKEN0_ADDRESS
    WHERE EVENT_NAME = 'Liquidation'
        AND parse_json(ev.PARAMETERS):collateral_token::varchar IN (SELECT TOKEN0_ADDRESS FROM TOKENS)
),
LIQUIDATION_RAW_TRANSFERS AS (
    SELECT
        ev.TX_HASH,
        t.TOKEN0_ADDRESS AS TOKEN1_ADDRESS,
        parse_json(ev.PARAMETERS):raw_value::number AS TOKEN1_AMOUNT,
        parse_json(ev.PARAMETERS):accumulator::number AS LENDING_INDEX_RAW_1,
        t.TOKEN_DECIMALS AS TOKEN1_DECIMALS
    FROM STARKNET_DATA_WAREHOUSE__T0.STARKNET.EVENTS ev
    LEFT JOIN TOKENS t ON ev.CONTRACT_ADDRESS = t.TOKEN1_ADDRESS
    WHERE ev.CONTRACT_ADDRESS IN (SELECT TOKEN1_ADDRESS FROM TOKENS)
        AND ev.EVENT_NAME = 'RawTransfer'
        AND ev.TX_HASH IN (SELECT TX_HASH FROM ZKLEND_LIQUIDATION_RAW)
),
LIQUIDATION_INDEX_RATES AS (
    SELECT
        TX_HASH,
        parse_json(PARAMETERS):token::varchar AS TOKEN_ADDRESS,
        parse_json(PARAMETERS):lending_accumulator::number AS LENDING_INDEX_RAW,
        parse_json(PARAMETERS):debt_accumulator::number AS BORROW_INDEX_RAW
    FROM ZKLEND_ACCUMULATOR_SYNC
    WHERE TX_HASH IN (SELECT TX_HASH FROM ZKLEND_LIQUIDATION_RAW)
),
COMPLETE_LIQUIDATION AS (
    SELECT
        r.BLOCK_NUMBER,
        r.BLOCK_TIMESTAMP,
        r.TX_HASH,
        r.TX_ID,
        r.TOKEN0_ADDRESS,
        lrt.TOKEN1_ADDRESS,
        r.CONTRACT_ADDRESS,
        'LIQUIDATION' AS EVENT_NAME,
        r.USER_ADDRESS,
        r.FROM_ADDRESS,
        r.TO_ADDRESS,
        r.TOKEN0_RAW_AMOUNT,
        -1 * lrt.TOKEN1_AMOUNT AS TOKEN1_RAW_AMOUNT,
        idx.LENDING_INDEX_RAW,
        lrt.LENDING_INDEX_RAW_1,
        idx.BORROW_INDEX_RAW,
        r.TOKEN0_DECIMALS,
        lrt.TOKEN1_DECIMALS
    FROM ZKLEND_LIQUIDATION_RAW r
    LEFT JOIN LIQUIDATION_RAW_TRANSFERS lrt ON r.TX_HASH = lrt.TX_HASH
    -- LEFT JOIN LIQUIDATION_INTEREST_RATES ir
    --     ON r.TX_HASH = ir.TX_HASH
    --         AND r.TOKEN0_ADDRESS = ir.TOKEN_ADDRESS
    LEFT JOIN LIQUIDATION_INDEX_RATES idx
        ON r.TX_HASH = idx.TX_HASH
            AND r.TOKEN0_ADDRESS = idx.TOKEN_ADDRESS
)
SELECT * FROM COMPLETE_DEPOSIT
UNION
SELECT * FROM COMPLETE_WITHDRAW
UNION
SELECT * FROM COMPLETE_BORROWING
UNION
SELECT * FROM COMPLETE_REPAYMENT
UNION
SELECT * FROM COMPLETE_LIQUIDATION 