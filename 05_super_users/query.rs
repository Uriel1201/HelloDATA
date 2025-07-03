use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use chrono::NaiveDate; // 0.4.41
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
struct User {
             user_id:          i32, 
             product_id:       i32,
             transaction_date: String,
}

#[derive(Debug)]
struct UserF {
              user_id:          i32,
              transaction_date: Option<NaiveDate>,
}

impl User {
    fn new(user_id: i32, product_id: i32, date_str: &str) -> Self {
        Self {
              user_id,
              product_id,
              transaction_date: date_str.to_string(),
        }
    }
}

macro_rules! user {
    ($user:expr, $product:expr, $date:expr) => {
        User::new($user, $product, $date)
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;
    
    conn.execute(
                 "CREATE TABLE USERS (
                      USER_ID          INTEGER,
                      PRODUCT_ID       INTEGER,
                      TRANSACTION_DATE CHAR(9)
                  )", 
                 (),
    )?;
    
    let users = vec![
    
            user!(1,
                  101,
                  "12-feb-20"),
        
            user!(2,
                  105,
                  "13-feb-20"),
        
            user!(1,
                  111,
                  "14-feb-20"),
        
            user!(3,
                  121,
                  "15-feb-20"),
        
            user!(1,
                  101,
                  "16-feb-20"),
        
            user!(2,
                  105,
                  "17-feb-20"),
        
            user!(4,
                  101,
                  "16-feb-20"),
       
            user!(3,
                  105,
                  "15-feb-20"),
            
            user!(3,
                  207,
                  "SOYNARUTO"),
                  
            user!(2,
                  401,
                  "AllaMadrid")
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("INSERT INTO 
                                   USERS (USER_ID, PRODUCT_ID, TRANSACTION_DATE) 
                                   VALUES (?1, ?2, ?3)")?;
    for u in users {
        stmt.execute(params![u.user_id, u.product_id, u.transaction_date])?;
    }
    }
    tx.commit()?;
    
    {
     let mut stmt = conn.prepare("SELECT 
                                      * 
                                  FROM 
                                      USERS")?;
     let users_iter = stmt.query_map([],
                                     |row| {
                                            RusqliteResult::Ok( User {
                                                                      user_id:          row.get(0)?,
                                                                      product_id:       row.get(1)?,
                                                                      transaction_date: row.get(2)?,
                                                                     }
                                                            )
                                           }
                           )?;
                           
     println!("\n--- USER'S DATA ---");
     for u in users_iter {
        println!("Found: {:?}", u.unwrap()); 
     }
    }
    
    let mut stmt = conn.prepare("SELECT 
                                     USER_ID, 
                                     TRANSACTION_DATE 
                                 FROM 
                                     USERS")?;
    let users_iter = stmt.query_map([], 
                                    |row| {
                                           let date_str: String = row.get("TRANSACTION_DATE")?;
                                           let parsed_date = NaiveDate::parse_from_str(&date_str, 
                                                                                       "%d-%b-%y"
                                                                        ).map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, 
                                                                                                                                rusqlite::types::Type::Text, 
                                                                                                                                Box::new(e)
                                                                                                       )
                                                                          )?;
    
                                               RusqliteResult::Ok(UserF {
                                                                         user_id:          row.get("USER_ID")?,
                                                                         transaction_date: Some(parsed_date),
                                                                        }
                                                                )
                                          }
                            )?; 
                 
    let mut all_users_v:Vec<UserF> = Vec::new();
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
        users_by_id.entry(u.user_id).or_insert_with(Vec::new).push(u);
    }
    
    println!("\n--- USERS BECOMING SUPERUSERS ---");
    
    for (id, user_list) in &users_by_id {
    
        let mut valid_dates: Vec<NaiveDate> = user_list.iter()
                                                       .filter_map(|u| u.transaction_date)
                                                       .collect();

        valid_dates.sort();
        if valid_dates.len() >= 2 {
            let superuser = valid_dates[1];

            println!("USER_ID {} => DATE: {} ",
                     id, superuser
            );
        } else {
            println!("USER_ID {} => NONE",
                     id
            );
        } 
    }
    
    Ok(())  
}
