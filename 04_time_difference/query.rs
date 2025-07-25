use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use chrono::{NaiveDate, Duration}; // 0.4.41
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
             id:          i32, 
             action:      String,
             action_date: String,
}

#[derive(Debug)]
struct UserF {
              id:          i32,
              action_date: Option<NaiveDate>,
}

impl User {
    fn new(id: i32, action: &str, date_str: &str) -> Self {
        Self {
              id,
              action:      action.to_string(),
              action_date: date_str.to_string(),
        }
    }
}

macro_rules! user {
    ($id_user:expr, $actions:expr, $dates:expr) => {
        User::new($id_user, $actions, $dates)
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;
    
    conn.execute(
                 "CREATE TABLE USERS (
                  ID          INTEGER,
                  ACTION      VARCHAR(9),
                  ACTION_DATE CHAR(9)
                  )", 
                 (),
    )?;
    
    let users = vec![
    
            user!(1, "Start", "13-feb-20"),
            user!(1, "Cancel", "13-feb-20"),
            user!(2, "Start", "11-feb-20"),
            user!(2, "Publish", "14-feb-20"),
            user!(3, "Start", "15-feb-20"),
            user!(3, "Cancel", "15-feb-20"),
            user!(4, "Start", "18-feb-20"),
            user!(1, "Publish", "19-feb-20"),
            user!(1, "SINGING", "41-feb-20"),
            user!(10, "DANCING", "UZUMAKI naruto"),
            user!(3, "FIGHTING", "02-feb-1540"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("INSERT INTO USERS (ID, ACTION, ACTION_DATE) 
                               values (?1, ?2, ?3)")?;
    for u in users {
        stmt.execute(params![u.id, u.action, u.action_date])?;
    }
    }
    tx.commit()?;
    
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
    }
    
    Ok(())
}
