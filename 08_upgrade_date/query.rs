use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use thiserror::Error;
use chrono::{NaiveDate, Duration};
use std::collections::{HashSet, HashMap};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error in Database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Date Error: {0}")]
    DateParse(#[from] chrono::ParseError),
}

#[derive(Debug)]
struct ColumnInfo {
    col_id: i32,
    name: String, 
    #[allow(dead_code)] 
    r#type: String, 
    notnull: bool, 
}

// This struct is used to simulate the table in a database 
#[derive(Debug)]
struct User {
             user_id:   i32, 
             name:      String,
             join_date: String,
}

#[derive(Debug)]
struct Event {
            user_id:     i32, 
            type_:       String,
            access_date: String, 
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct UserF {
             user_id: i32,
             date:    Option<NaiveDate>,
}

impl User {
    fn new(user_id:i32, name_str:&str, date_str:&str) -> Self {
        Self {
              user_id,
              name:      name_str.to_string(),
              join_date: date_str.to_string(),
        }
    }
}

impl Event {
    fn new(user_id:i32, type_str:&str, date_str:&str) -> Self {
        Self {
              user_id,
              type_:       type_str.to_string(),
              access_date: date_str.to_string(),
        }
    }
}

macro_rules! user {
    ($id:expr, $name:expr, $date:expr) => {
        User::new($id, $name, $date)
    };
}

macro_rules! event {
    ($id:expr, $typ:expr, $date:expr) => {
        Event::new($id, $typ, $date)
    };
}

fn main() -> Result<(), AppError> {
    let mut conn = Connection::open_in_memory()?;

    conn.execute("CREATE TABLE 
                      USERS (USER_ID   INTEGER,
                             NAME      VARCHAR(9),
                             JOIN_DATE VARCHAR(9)
                      )"
                 , 
                 (),
    )?;
    
    conn.execute("CREATE TABLE 
                      EVENTS (USER_ID     INTEGER,
                              TYPE        VARCHAR(2),
                              ACCESS_DATE VARCHAR(9)
                      )"
                 , 
                 (),
    )?;
    
    let all_users = vec![
    user!(1,
          "John",
          "14-Feb-20"),
    user!(2,
          "Jane",
          "14-Feb-20"),
    user!(3,
          "Jill",
          "15-Feb-20"),
    user!(4,
          "Josh",
          "15-Feb-20"),
    user!(5,
          "Jean",
          "16-Feb-20"),
    user!(6,
          "Justin",
          "17-Feb-20"),
    user!(7,
          "Jeremy",
          "18-Feb-20"),
    ];

    let all_events = vec![
    event!(1,
           "F1",
           "1-Mar-20"),
    event!(2,
           "F2",
           "2-Mar-20"),
    event!(2,
           "P",
           "12-Mar-20"),
    event!(3,
           "F2",
           "15-Mar-20"),
    event!(4,
           "F2",
           "15-Mar-20"),
    event!(1,
           "P",
           "16-Mar-20"),
    event!(3,
           "P",
           "22-Mar-20"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  USERS (USER_ID, NAME, JOIN_DATE)
                              VALUES
                                  (?1, ?2, ?3)"
                      )?;
    for u in all_users {
        stmt.execute(params![u.user_id, u.name, u.join_date])?;
    }
    }
    tx.commit()?;
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  EVENTS (USER_ID, TYPE, ACCESS_DATE)
                              VALUES
                                  (?1, ?2, ?3)"
                      )?;
    for e in all_events {
        stmt.execute(params![e.user_id, e.type_, e.access_date])?;
    }
    }
    tx.commit()?;
    
    println!("\n--- RAW DATA ---\nSCHEMA (USERS Table)");
    let mut _stmt = conn.prepare("PRAGMA table_info('USERS')")?;
    let column_info_iter1 = _stmt.query_map([], |row| {
        RusqliteResult::Ok(ColumnInfo {
            col_id: row.get(0)?,
            name: row.get(1)?,
            r#type: row.get(2)?,
            notnull: row.get(3)?,
        })
    })?;
    
    for c in column_info_iter1 {
        let c = c?;
        println!("column_id: {}; column_name: {}; type: {}; is_not_null: {}", c.col_id, c.name, c.r#type, c.notnull);
    }
    
    let mut _stmt = conn.prepare("PRAGMA table_info('EVENTS')")?;
    let column_info_iter = _stmt.query_map([], |row| {
        RusqliteResult::Ok(ColumnInfo {
            col_id: row.get(0)?,
            name: row.get(1)?,
            r#type: row.get(2)?,
            notnull: row.get(3)?,
        })
    })?;
    
    println!("SCHEMA (EVENTS Table)");
    for c in column_info_iter {
        let c = c?;
        println!("column_id: {}; column_name: {}; type: {}; is_not_null: {}", c.col_id, c.name, c.r#type, c.notnull);
    }
    /*
    let mut stmt = conn.prepare("SELECT 
                                     USER_ID 
                                 FROM 
                                     MOBILE"
                        )?;
                        
    let all_mobile_v:HashSet<User> = stmt.query_map([],
                                                    |row| {
                                                           RusqliteResult::Ok(User {
                                                                                    user_id: row.get("USER_ID")?,
                                                                              }
                                                           )
                                                    }
                                          )?.filter_map(Result::ok)
                                            .collect();
    println!("\n(unique mobile users)");
    for m in &all_mobile_v {
        println!("Found: {:?}", m);
    }
    
    let mut stmt = conn.prepare("SELECT 
                                     USER_ID 
                                 FROM 
                                     WEB"
                        )?;
                        
    let all_web_v:HashSet<User> = stmt.query_map([],
                                                 |row| {
                                                        RusqliteResult::Ok(User {
                                                                                 user_id: row.get("USER_ID")?,
                                                                           }
                                                        )
                                                        }
                                       )?.filter_map(Result::ok)
                                         .collect();
    println!("\n(unique web users)");
    for w in &all_web_v {
        println!("Found: {:?}", w);
    }
    
    println!("\n--- USER'S STATISTICS ---");
    let mut both:i32 = 0;
    let mut only_web:i32 = 0;
    for w in &all_web_v {
        if all_mobile_v.contains(w) {
            both += 1;
        } else {
            only_web += 1;
        }
    }
    
    let only_mobile:i32 = (all_mobile_v.len() as i32) - both;
    let total:i32 = only_mobile + only_web + both;
    let rate_mobile:f64 = (only_mobile as f64) / (total as f64);
    let rate_web:f64 = (only_web as f64) / (total as f64);
    let rate_both:f64 = (both as f64) / (total as f64);
    
    println!("Total Users: {}", total);
    println!("(Fraction of Users Using Web or Mobile)\nboth:        {}\nonly_web:    {}\nonly_mobile: {}", rate_both, rate_web, rate_mobile);
    */
    Ok(())
}
