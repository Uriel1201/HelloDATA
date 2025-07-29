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
             user_id: i32, 
             action:  String,
             date:    String,
}

#[derive(Debug)]
struct UserF {
             user_id: i32, 
             action:  String,
}

#[derive(Debug)]
struct Count {
             start_count:   Option<usize>,
             publish_count: Option<usize>,
             cancel_count:  Option<usize>,
}

#[derive(Debug)]
struct Action {
             action: String,
             count:  usize,
}

impl User {
    fn new(user_id: i32, action_str: &str, date_str: &str) -> Self {
        Self {
              user_id,
              action: action_str.to_string(),
              date: date_str.to_string(),
        }
    }
}

impl Count {
    fn new(start_count:Option<usize>, publish_count:Option<usize>, cancel_count:Option<usize>) -> Self {
        Self {
              start_count,
              publish_count,
              cancel_count,
        }
    }
}

impl Action {
    fn new(action:String, count:usize) -> Self {
        Self {
              action,
              count,
        }
    }
}

macro_rules! user {
    ($id:expr, $action:expr, $date:expr) => {
        User::new($id, $action, $date)
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;

    conn.execute("
                 CREATE TABLE USERS (USER_ID INTEGER, ACTION VARCHAR(9), DATES VARCHAR(9))", 
                 (),
    )?;
    
    let users = vec![
    user!(1,
          "start",
          "01-jan-20"),
    user!(1,
          "cancel",
          "02-jan-20"),
    user!(2,
          "start",
          "03-jan-20"),
    user!(2,
          "publish",
          "04-jan-20"),
    user!(3,
          "start",
          "05-jan-20"),
    user!(3,
          "cancel",
          "06-jan-20"),
    user!(1,
          "start",
          "07-jan-20"),
    user!(1,
          "publish",
          "08-jan-20"),
    user!(0,
          "publish",
          "'08-jan-20"),
    ];
   
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  USERS 
                                  (USER_ID, ACTION, DATES)
                              VALUES
                                  (?1, ?2, ?3)"
                      )?;
    for u in users {
        stmt.execute(params![u.user_id, u.action, u.date])?;
    }
    }
    tx.commit()?;
    
    println!("\n--- RAW DATA ---\n(schema)");
    let mut _stmt = conn.prepare("PRAGMA table_info('USERS')")?;
    let column_info_iter = _stmt.query_map([], |row| {
        RusqliteResult::Ok(ColumnInfo {
            col_id: row.get(0)?,
            name: row.get(1)?,
            r#type: row.get(2)?,
            notnull: row.get(3)?,
        })
    })?;
    
    for c in column_info_iter {
        let c = c?;
        println!("column_id: {}; column_name: {}; type: {}; is_not_null: {}", c.col_id, c.name, c.r#type, c.notnull);
    }

    let mut stmt = conn.prepare("SELECT 
                                     USER_ID, 
                                     ACTION 
                                 FROM 
                                     USERS"
                        )?;
                        
    let all_users_v:Vec<UserF> = stmt.query_map([], 
                                                    |row| {                                          
                                                    RusqliteResult::Ok(UserF {
                                                                           user_id: row.get("USER_ID")?,
                                                                           action:  row.get("ACTION")?,
                                                                       }
                                                    )
                                                    }
                                          )?
                                         .filter_map(Result::ok).collect();
                                
    for u in &all_users_v {
        println!("found: {:?}", u);
    }
    
    println!("\n--- USER'S STATISTICS ---");
    let mut counts_by_action:HashMap<(i32, String), usize> = HashMap::new();
    for u in all_users_v {
        *counts_by_action.entry((u.user_id, u.action)).or_insert(0)+=1;
    }
    
    let mut id_actions:HashMap<i32, Vec<Action>> = HashMap::new();
    for (i, c) in counts_by_action {
        id_actions.entry(i.0).or_insert_with(Vec::new).push(Action::new(i.1, c));
    }

    let mut users_count:HashMap<i32, Count> = HashMap::new();
   
    for (i, list) in id_actions {
        let result_count:&mut Count = users_count.entry(i).or_insert_with(|| Count::new(Some(0), Some(0), Some(0)));
        for v in list {
            match v.action.as_str() {
                "start" => {result_count.start_count = Some(v.count)},
                "publish" => {result_count.publish_count = Some(v.count)},
                "cancel" => {result_count.cancel_count = Some(v.count)},
                _ => {()}
            }
        }
    }

    for (i, action) in users_count {
        let publish_rate:Option<f64> = action.start_count.and_then(|start_val| {action.publish_count.map(|publish_val| {if start_val == 0 {None} 
                                                                                                                        else {Some(publish_val as f64 / start_val as f64)}
                                                                                                                       }
                                                                                                     )
                                                                               }
                                                          ).flatten();
        let cancel_rate:Option<f64> = action.start_count.and_then(|start_val| {action.cancel_count.map(|cancel_val| {if start_val == 0 {None} else {Some(cancel_val as f64 / start_val as f64)}})})
                                                        .flatten();
        println!("user_id {} =>\npublish rate: {:?}\ncancel rate: {:?}\n", i, publish_rate, cancel_rate);
    }
    
    Ok(())
}
