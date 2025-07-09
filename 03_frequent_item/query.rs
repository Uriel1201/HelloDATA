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

#[derive(Debug)]
struct Item {
    item: String, 
    date: String,
}
#[derive(Debug)]
struct ItemF {
    item: String, 
    date: Option<NaiveDate>,
}
#[derive(Debug)]
struct Count {
    item: String, 
    count: usize,
}

impl Item {
    fn new(item: &str, date_str: &str) -> Self {
        Self {
            item: item.to_string(),
            date: date_str.to_string(),
        }
    }
}

impl Count {
    fn new(item: String,  count: usize) -> Self {
        Self {
            item,
            count,
        }
    }
}

macro_rules! item {
    ($item:expr, $date:expr) => {
        Item::new($item, $date)
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;
    
    conn.execute(
    "CREATE TABLE ITEMS(
         DATE VARCHAR(9),
         ITEM VARCHAR(9)
     )", (),
    )?;
    
    let items = vec![
    item!("apple", "01-jan-20"),
    item!("apple", "01-jan-20"),
    item!("pear", "01-jan-20"),
    item!("pear", "01-jan-20"),
    item!("pear", "02-jan-20"),
    item!("pear", "02-jan-20"),
    item!("pear", "02-jan-20"),
    item!("orange", "02-jan-20"),
    item!("Dewey", "fuaaaa!!!"),
    item!("Thanos", "67-jan-93"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("INSERT INTO ITEMS(DATE, ITEM) 
                               values (?1, ?2)")?;
    for i in items {
        stmt.execute(params![i.date, i.item])?;
    }
    }
    tx.commit()?;
    
    
    println!("--- ITEM'S DATA ---");
    let mut stmt = conn.prepare("SELECT
                                     DATE, 
                                     ITEM
                                 FROM
                                     ITEMS"
                        )?;
                        
    let items_iter = stmt.query_map([], |row| {Ok(Item {
                                                      item: row.get(1)?,
                                                      date: row.get(0)?,
                                                  }
                                               )
                                              })?;
    for i in items_iter {
        println!("Found: {:?}", i.unwrap());
    }
    
    let items_iter = stmt.query_map([], 
                                    |row| {
                                           let date_str: String = row.get("DATE")?;
                                           let parsed_date = NaiveDate::parse_from_str(&date_str, 
                                                                                       "%d-%b-%y"
                                                                        ).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, 
                                                                                                                                rusqlite::types::Type::Text, 
                                                                                                                                Box::new(e)
                                                                                                       )
                                                                          )?;
    
                                               RusqliteResult::Ok(ItemF {
                                                                         item: row.get("ITEM")?,
                                                                         date: Some(parsed_date),
                                                                        }
                                                                )
                                          }
                            )?; 
                 
    let mut all_items_v:Vec<ItemF> = Vec::new();
    for i in items_iter {
        match i {
            Ok(item_) => all_items_v.push(item_),
            Err(e) => eprintln!("Hi my friend!, there's a problem: {:?}",
                                AppError::Database(e)
                      ),
        }
    }
    
    let mut items_grouped: HashMap<(String, Option<NaiveDate>), Vec<ItemF>> = HashMap::new();
    for i in all_items_v {
        items_grouped.entry((i.item.clone(), i.date)).or_insert_with(Vec::new).push(i);
    }
    
    let mut item_count: HashMap<Option<NaiveDate>, Vec<Count>> = HashMap::new();
    for (i,list) in items_grouped {
        item_count.entry(i.1).or_insert_with(Vec::new).push(Count::new(i.0, list.len()));
    }
    
    println!("\n-- MOST FREQUENTED ITEM BY EACH DATE --");
    for (i, list) in &item_count {
        let max_count = list.iter().map(|item| item.count).max();
        let max_items: Vec<&Count> = list.iter().filter(|item| {item.count == max_count.unwrap_or(0)}).collect();
        println!("date: {:?}", i);
        for item in max_items {
            println!("    item: {}; count: {}", item.item, item.count);
        }
    }

    Ok(())
}
