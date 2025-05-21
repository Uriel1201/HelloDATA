use rusqlite::{Connection, Result, params}; // 0.35.0
use chrono::NaiveDate; // 0.4.41
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error de base de datos: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Error de fecha: {0}")]
    DateParse(#[from] chrono::ParseError),
}

#[derive(Debug)]
struct Transaction {
    sender: i32,
    receiver: i32,
    amount: f64,
    dates: Option<NaiveDate>,
}

#[derive(Debug)]
struct Changes {
    user_id: Option<i32>, 
    change: Option<f64>,
}

impl Transaction {
    fn new(sender: i32, receiver: i32, amount: f64, date_str: &str) -> Result<Self, chrono::ParseError> {
        Ok(Self {
            sender,
            receiver,
            amount,
            dates: Some(NaiveDate::parse_from_str(date_str, "%d-%b-%y")?),
        })
    }
}

macro_rules! transaction {
    ($sender:expr, $receiver:expr, $amount:expr, $dates:expr) => {
        Transaction::new($sender, $receiver, $amount, $dates)?
    };
}

fn main() -> Result<(), AppError> {
    
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE TRANSACTIONS(
                     SENDER INTEGER, 
                     RECEIVER INTEGER,
                     AMOUNT NUMERIC, 
                     TRANSACTION_DATE DATE)", (),
    )?;
    
    let transactions = vec![
    transaction!(5, 2, 10.0, "12-feb-20"),
    transaction!(1, 3, 15.0, "13-feb-20"),
    transaction!(2, 1, 20.0, "13-feb-20"),
    transaction!(2, 3, 25.0, "14-feb-20"),
    transaction!(3, 1, 20.0, "15-feb-20"),
    transaction!(3, 2, 15.0, "15-feb-20"),
    transaction!(1, 4, 5.0, "16-feb-20"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("INSERT INTO TRANSACTIONS(SENDER, RECEIVER, AMOUNT, TRANSACTION_DATE) 
                               values (?1, ?2, ?3, ?4)")?;
    for t in transactions {
        stmt.execute(params![t.sender, t.receiver, t.amount, t.dates])?;
    }
    }
    tx.commit()?;
    
    {
    println!("TRANSACTIONS SAMPLE(5):");
    let mut stmt = conn.prepare("SELECT * FROM TRANSACTIONS LIMIT 5")?;
    let transaction_iter = stmt.query_map([], |row| {Ok(Transaction {
                                                            sender: row.get(0)?,
                                                            receiver: row.get(1)?,
                                                            amount: row.get(2)?,
                                                            dates: row.get(3)?,
                                                  }
                                                        )
                                                    })?;
    for t in transaction_iter {
        println!("Found: {:?}", t.unwrap());
    }
    }
    
    {
    let mut stmt = conn.prepare(
    "WITH SENDERS AS (
    SELECT 
        SENDER, 
        SUM(AMOUNT) AS SENDED
    FROM
        TRANSACTIONS
    GROUP BY
        SENDER), 
        RECEIVERS AS (
    SELECT
        RECEIVER,
        SUM(AMOUNT) AS RECEIVED 
    FROM
        TRANSACTIONS
    GROUP BY
        RECEIVER)
    SELECT
        COALESCE(S.SENDER, R.RECEIVER)                    AS USER_ID,
        1.0*(COALESCE(R.RECEIVED, 0) - COALESCE(S.SENDED, 0)) AS NET_CHANGE
    FROM
        RECEIVERS R
        FULL OUTER JOIN SENDERS   S ON R.RECEIVER = S.SENDER
    ORDER BY
        2 DESC")?;
        
    println!("\nNET CHANGES:");
    let changes_iter = stmt.query_map([], |row| {Ok(Changes {
                                                        user_id: row.get("USER_ID")?,
                                                        change: row.get("NET_CHANGE")?,
                                                    }
                                                 )}
                                     )?;
                                     
    for changes in changes_iter {
        let changes = changes?;
        println!("User {:?} -> Net_Change: {:?}",
                 changes.user_id,
                 changes.change
        );
    }
    }
    
    Ok(())
}
