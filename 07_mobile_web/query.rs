use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use thiserror::Error;
use std::collections::HashSet;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error in Database: {0}")]
    Database(#[from] rusqlite::Error),
}

// This struct is used to simulate the table in a database 
#[derive(Debug)]
struct Mobile {
               user_id:  i32, 
               page_url: String,
}

#[derive(Debug)]
struct Web {
            user_id:  i32, 
            page_url: String,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct User {
             user_id: i32,
}

impl Mobile {
    fn new(user_id: i32, page_str: &str) -> Self {
        Self {
              user_id,
              page_url: page_str.to_string(),
        }
    }
}

impl Web {
    fn new(user_id: i32, page_str: &str) -> Self {
        Self {
              user_id,
              page_url: page_str.to_string(),
        }
    }
}

macro_rules! mobile {
    ($id:expr, $page:expr) => {
        Mobile::new($id, $page)
    };
}

macro_rules! web {
    ($id:expr, $page:expr) => {
        Web::new($id, $page)
    };
}

fn main() -> Result<(), AppError> {

    
    let mut conn = Connection::open_in_memory()?;

    conn.execute("
                 CREATE TABLE 
                     MOBILE (
                         USER_ID INTEGER,
                         PAGE_URL CHAR
                     )
                 ", 
                 (),
    )?;
    
    conn.execute("
                 CREATE TABLE 
                     WEB (
                         USER_ID INTEGER,
                         PAGE_URL CHAR
                     )
                 "
                 , 
                 (),
    )?;
    
    let all_mobile = vec![
    mobile!(1,
            "A"),
    mobile!(2,
            "B"),
    mobile!(3,
            "C"),
    mobile!(4,
            "A"),
    mobile!(9,
            "B"),
    mobile!(2,
            "C"),
    mobile!(10,
            "B"),
    ];

    let all_web = vec![
    web!(6,
         "A"),
    web!(2,
         "B"),
    web!(3,
         "C"),
    web!(7,
         "A"),
    web!(4,
         "B"),
    web!(8,
         "C"),
    web!(5,
         "B"),
    ];
    
   
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  MOBILE (USER_ID, PAGE_URL)
                              VALUES
                                  (?1, ?2)"
                      )?;
    for m in all_mobile {
        stmt.execute(params![m.user_id, m.page_url])?;
    }
    }
    tx.commit()?;
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  WEB (USER_ID, PAGE_URL)
                              VALUES
                                  (?1, ?2)"
                      )?;
    for w in all_web {
        stmt.execute(params![w.user_id, w.page_url])?;
    }
    }
    tx.commit()?;
    
    let mut stmt = conn.prepare("SELECT 
                                     USER_ID 
                                 FROM 
                                     MOBILE"
                        )?;
                        
    let mobile_iter = stmt.query_map([],
                                     |row| {
                                            RusqliteResult::Ok(User {
                                                                     user_id: row.get("USER_ID")?,
                                                               }
                                                            )
                                    }
                           )?;
                                 
    let mut stmt = conn.prepare("SELECT 
                                     USER_ID 
                                 FROM 
                                     WEB"
                        )?;
                        
    let web_iter = stmt.query_map([],
                                  |row| {
                                    RusqliteResult::Ok(User {
                                                             user_id: row.get("USER_ID")?,
                                                       }
                                                    )
                                    }
                        )?;
    
    println!("--- RAW DATA ---");
    let mut all_mobile_v:HashSet<User> = HashSet::new();
    let mut all_web_v:HashSet<User> = HashSet::new();
    
    for m in mobile_iter {
        match m {
                 Ok(mobile_v) => {all_mobile_v.insert(mobile_v);},
                 Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                     AppError::Database(e)
                           ),
        }
    }
    
    println!("(unique mobile users)");
    for m in &all_mobile_v {
        println!("Found: {:?}", m);
    }
    
    for w in web_iter {
        match w {
                 Ok(web_v) => {all_web_v.insert(web_v);},
                 Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                     AppError::Database(e)
                           ),
        }
    }
    println!("(unique web users)");
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
    println!("fraction of users using web or mobile =>\nboth: {}\nonly_web: {}\nonly_mobile: {}", rate_both, rate_web, rate_mobile);
    
    Ok(())
}
