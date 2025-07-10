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
                        
                         TRANSACTION_DATE VARCHAR(9)", 
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
    transaction!(3, 4, 7.0, "PickleRick"),
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
    /*
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
        println!("Found: {:?}", i.unwrap());
    }
    
    let mut all_users_v:Vec<UserF> = Vec::new();
    
    {
     let mut stmt = conn.prepare("SELECT * FROM USERS")?;
     let users_iter = stmt.query_map([],
                                     |row| {
                                            RusqliteResult::Ok( User {
                                                                      id:          row.get(0)?,
                                                                      action:      row.get(1)?,
                                                                      action_date: row.get(2)?,
                                                                     }
                                                            )
                                           }
                           )?;
                           
     println!("\n--- USER'S DATA ---");
     for u in users_iter {
        println!("Found: {:?}", u.unwrap()); 
     }
    }
    let mut stmt = conn.prepare("SELECT ID, ACTION_DATE FROM USERS")?;
    let users_iter = stmt.query_map([], 
                                    |row| {
                                           let date_str: String = row.get("ACTION_DATE")?;
                                           let parsed_date = NaiveDate::parse_from_str(&date_str, 
                                                                                       "%d-%b-%y"
                                                                        ).map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, 
                                                                                                                                rusqlite::types::Type::Text, 
                                                                                                                                Box::new(e)
                                                                                                       )
                                                                          )?;
    
                                               RusqliteResult::Ok(UserF {
                                                                         id:          row.get("ID")?,
                                                                         action_date: Some(parsed_date),
                                                                        }
                                                                )
                                          }
                            )?; 
                 
                
    for u in users_iter {
        match u {
            Ok(user_v) => all_users_v.push(user_v),
                          Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                              AppError::Database(e)
                                    ),
        }
    } 
    
    
    let mut users_by_id:HashMap<i32, Vec<UserF>> = HashMap::new();
    for u in all_users_v {
        users_by_id.entry(u.id).or_insert_with(Vec::new).push(u);
    }
    
    println!("\n--- ELAPSED TIME BETWEEN TWO LAST ACTIONS MADE BY EACH USER ---");
    for (id, user_list) in &users_by_id {
    
        let mut valid_dates: Vec<NaiveDate> = user_list.iter()
                                                       .filter_map(|u| u.action_date)
                                                       .collect();

        valid_dates.sort();
        if valid_dates.len() >= 2 {
            let last_date = valid_dates[valid_dates.len() - 1];
            let second_last_date = valid_dates[valid_dates.len() - 2];

            let duration_diff: Duration = last_date.signed_duration_since(second_last_date);

            println!(
                "ID {} => ELAPSED_TIME: {} DAYS",
                id, duration_diff.num_days()
            );
        } else {
            println!(
                "ID {} => NONE",
                id
            );
        }
    } */
    
    Ok(())
}
