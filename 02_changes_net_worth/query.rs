use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use thiserror::Error;
use std::collections::HashMap;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error in Database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Date Error: {0}")]
    DateParse(#[from] chrono::ParseError),
}

// This struct is used to simulate the table in a database 
#[derive(Debug)]
struct Transaction {
             sender:           i32, 
             receiver:         i32,
             amount:           f64,
             transaction_date: String,
}

#[derive(Debug)]
struct TransactionF {
              sender:   i32,
              receiver: i32,
              amount:   f64,
}

#[derive(Debug)]
struct User {
              transaction: f64,
}

impl Transaction {
    fn new(sender: i32, receiver: i32, amount: f64, date_str: &str) -> Self {
        Self {
              sender,
              receiver,
              amount,
              transaction_date: date_str.to_string(),
        }
    }
}

impl User {
    fn new(transaction: f64) -> Self {
        Self {
              transaction,
        }
    }
}


macro_rules! transaction {
    ($id_sender:expr, $id_receiver:expr, $amount:expr, $date:expr) => {
        Transaction::new($id_sender, $id_receiver, $amount, $date)
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;

    conn.execute("    
                 CREATE TABLE 
                     TRANSACTIONS (
                         SENDER           INTEGER,
                         RECEIVER         INTEGER,
                         AMOUNT           DECIMAL,
                         TRANSACTION_DATE VARCHAR(9)
                     )", 
                 (),
    )?;

    let transactions = vec![
    transaction!(5,
                 2,
                 10.0,
                 "12-feb-20"),
    transaction!(1,
                 3,
                 15.0,
                 "13-feb-20"),
    transaction!(2,
                 1,
                 20.0,
                 "13-feb-20"),
    transaction!(2,
                 3,
                 25.0,
                 "14-feb-20"),
    transaction!(3,
                 1,
                 20.0,
                 "15-feb-20"),
    transaction!(3,
                 2,
                 15.0,
                 "15-feb-20"),
    transaction!(1,
                 4,
                 5.0,
                 "16-feb-20"),
    ];
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  TRANSACTIONS 
                                  (SENDER, RECEIVER, AMOUNT, TRANSACTION_DATE)
                              VALUES
                                  (?1, ?2, ?3, ?4)"
                      )?;
    for t in transactions {
        stmt.execute(params![t.sender, t.receiver, t.amount, t.transaction_date])?;
    }
    }
    tx.commit()?;
    
    let mut stmt = conn.prepare("SELECT 
                                     * 
                                 FROM 
                                     TRANSACTIONS"
                        )?;
    let transactions_iter = stmt.query_map([],
                                           |row| {
                                            RusqliteResult::Ok(Transaction {
                                                                   sender:           row.get(0)?,
                                                                   receiver:         row.get(1)?,
                                                                   amount:           row.get(2)?,
                                                                   transaction_date: row.get(3)?,
                                                               }
                                                            )
                                           }
                                 )?;
    
    println!("--- RAW DATA ---");
    for i in transactions_iter {
        println!("Transaction Found: {:?}", i.unwrap());
    }
   
    let mut all_transactions_v:Vec<TransactionF> = Vec::new();
    let mut stmt = conn.prepare("SELECT 
                                     SENDER, 
                                     RECEIVER, 
                                     AMOUNT 
                                 FROM 
                                     TRANSACTIONS"
                        )?;
                        
    let transactions_iter = stmt.query_map([], 
                                           |row| {                                          
                                                  RusqliteResult::Ok(TransactionF {
                                                                         sender:   row.get("SENDER")?,
                                                                         receiver: row.get("RECEIVER")?,
                                                                         amount:   row.get("AMOUNT")?,
                                                                     }
                                                                  )
                                                 }
                                 )?; 
                 
    for t in transactions_iter {
        match t {
            Ok(transaction_v) => all_transactions_v.push(transaction_v),
            Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                AppError::Database(e)
                      ),
        }
    } 
    
    let mut transactions_by_id:HashMap<i32, Vec<User>> = HashMap::new();
    for t in all_transactions_v {
        let a = -1.0 * t.amount;
        let b = t.amount;
        transactions_by_id.entry(t.sender).or_insert_with(Vec::new).push(User::new(a));
        transactions_by_id.entry(t.receiver).or_insert_with(Vec::new).push(User::new(b));
    }
    
    println!("\n--- NET CHANGES MADE BY EACH USER ---");
    for (i, list) in &transactions_by_id {
        let c:f64 = list.iter().map(|u| u.transaction).sum();
        println!("User_id: {} => Net Change {}", i, c);
    }
   
    Ok(())
}
