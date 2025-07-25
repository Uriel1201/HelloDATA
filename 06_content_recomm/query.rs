use rusqlite::{Connection, Result as RusqliteResult, params}; // 0.35.0
use thiserror::Error;
use std::collections::{HashMap, HashSet};

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error in Database: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Date Error: {0}")]
    DateParse(#[from] chrono::ParseError),
}

// This struct is used to simulate the table in a database 
#[derive(Debug)]
struct Friend {
             user_id: i32, 
             friend:  i32,
}

#[derive(Debug)]
struct Recommendation {
             page:    Option<String>,
}

#[derive(Debug)]
struct PageLike {
             user_id:   i32, 
             page_like: String,
}

impl Friend {
    fn new(user_id: i32, friend: i32) -> Self {
        Self {
              user_id,
              friend,
        }
    }
}

impl PageLike {
    fn new(user_id: i32, page_str: &str) -> Self {
        Self {
              user_id,
              page_like: page_str.to_string(),
        }
    }
}


impl Recommendation {
    fn new(page: Option<String>) -> Self {
        Self {
              page,
        }
    }
}

macro_rules! friend {
    ($id:expr, $friend:expr) => {
        Friend::new($id, $friend)
    };
}

macro_rules! like {
    ($id:expr, $page:expr) => {
        PageLike::new($id, $page)
    };
}

fn main() -> Result<(), AppError> {

    
    let mut conn = Connection::open_in_memory()?;

    conn.execute("
                 CREATE TABLE 
                     FRIENDS (
                         USER_ID INTEGER,
                         FRIEND INTEGER)
                 ", 
                 (),
    )?;
    
    conn.execute("
                 CREATE TABLE 
                     LIKES (
                         USER_ID INTEGER,
                         PAGE_LIKES CHAR)
                 "
                 , 
                 (),
    )?;
    
    let friends = vec![
    friend!(1,
            2),
    friend!(1,
            3),
    friend!(1,
            4),
    friend!(2,
            1),
    friend!(3,
            1),
    friend!(3,
            4),
    friend!(4,
            1),
    friend!(4,
            3),
    ];
    
    let likes = vec![
    like!(1,
          "A"),
    like!(1,
          "B"),
    like!(1,
          "C"),
    like!(2,
          "A"),
    like!(3,
          "B"),
    like!(3,
          "C"),
    like!(4,
          "B"),
    ];
   
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  FRIENDS (USER_ID, FRIEND)
                              VALUES
                                  (?1, ?2)"
                      )?;
    for f in friends {
        stmt.execute(params![f.user_id, f.friend])?;
    }
    }
    tx.commit()?;
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("
                              INSERT INTO 
                                  LIKES (USER_ID, PAGE_LIKES)
                              VALUES
                                  (?1, ?2)"
                      )?;
    for li in likes {
        stmt.execute(params![li.user_id, li.page_like])?;
    }
    }
    tx.commit()?;
    
    let mut stmt = conn.prepare("SELECT 
                                     * 
                                 FROM 
                                     FRIENDS"
                        )?;
    let friends_iter = stmt.query_map([],
                                    |row| {
                                    RusqliteResult::Ok(Friend {
                                                               user_id: row.get(0)?,
                                                               friend:  row.get(1)?,
                                                       }
                                                    )
                                    }
                                 )?;
    let mut stmt = conn.prepare("SELECT 
                                     * 
                                 FROM 
                                     LIKES"
                        )?;
    let likes_iter = stmt.query_map([],
                                    |row| {
                                    RusqliteResult::Ok(PageLike {
                                                                 user_id:    row.get(0)?,
                                                                 page_like:  row.get(1)?,
                                                       }
                                                    )
                                    }
                                 )?;
    
    println!("--- RAW DATA ---");
    let mut all_friends_v:Vec<Friend> = Vec::new();
    let mut all_likes_v:Vec<PageLike> = Vec::new();
    
    for f in friends_iter {
        match f {
            Ok(friend_v) => all_friends_v.push(friend_v),
            Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                AppError::Database(e)
                      ),
        }
    }
    for f in &all_friends_v {
        println!("Found: {:?}", f);
    }
    
    for li in likes_iter {
        match li {
            Ok(like_v) => all_likes_v.push(like_v),
            Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                AppError::Database(e)
                      ),
        }
    }
    for li in &all_likes_v {
        println!("Found: {:?}", li);
    }
    
    println!("\n--- RECOMMENDATION ---");
    
    let mut friend_likes:HashMap<i32, Vec<Recommendation>> = HashMap::new();
    for li in &all_likes_v {
        friend_likes.entry(li.user_id).or_insert_with(Vec::new).push(Recommendation::new(Some(li.page_like.clone())));
    }
    
    let mut recommendations: Vec<(&Friend, &Recommendation)> = vec![];
    for li in &all_friends_v {
        if let Some(list_rec) = friend_likes.get(&li.friend) {
            for r in list_rec {
                recommendations.push((li, r));
            }
        }
    }
    
    let mut anti_set:HashSet<(i32, Option<String>)> = HashSet::new();
    for li in all_likes_v {
        anti_set.insert((li.user_id, Some(li.page_like)));
    }
    
    
    let mut anti_unique:HashSet<(i32, Option<String>)> = HashSet::new();
    for (f, r) in recommendations {
        let anti_key = (f.user_id, r.page.clone());
        if !anti_set.contains(&anti_key) {
            anti_unique.insert(anti_key);
        }
    }
    
    for a in anti_unique {
        println!("user_id: {} => recommendation: {:?}", a.0, a.1);
    }
    
    Ok(())
  }
