use rusqlite::{Connection, Result, params}; // 0.35.0
use chrono::NaiveDate; // 0.4.41
use thiserror::Error;

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
    id: i32, 
    action: String,
    action_date: String,
}

#[derive(Debug)]
struct UserF {
    id: i32, 
    action: String,
    action_date: Option<NaiveDate>,
}

impl User {
    fn new(id: i32, action: &str, date_str: &str) -> Self {
        Self {
            id,
            action: action.to_string(),
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
         ID INTEGER,
         ACTION VARCHAR(9),
         ACTION_DATE CHAR(9)
     )", (),
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
    
    {
    println!("USERS TABLE:");
    let mut stmt = conn.prepare("SELECT * FROM USERS")?;
    let users_iter = stmt.query_map([], |row| {let date_str: String = row.get(2)?;
                                               let parsed_date = Some(NaiveDate::parse_from_str(&date_str, "%d-%b-%y").map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?);
    
                                               Ok(UserF {
                                                      id: row.get(0)?,
                                                      action: row.get(1)?,
                                                      action_date: parsed_date,
                                                  }
                                               )
                                              })?; 
                                        
    for u in users_iter {
        println!("Found: {:?}", u.unwrap());
    }
    }
    
    Ok(())
}
