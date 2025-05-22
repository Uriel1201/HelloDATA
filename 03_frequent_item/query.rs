use rusqlite::{Connection, Result, params}; // 0.35.0
use chrono::NaiveDate; // 0.4.41
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Error de base de datos: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Error de fecha: {0}")]
    DateParse(#[from] chrono::ParseError),
}

#[derive(Debug)]
struct Items {
    item: String, 
    dates: Option<NaiveDate>,
}

impl Items {
    fn new(item: &str, date_str: &str) -> Result<Self, chrono::ParseError> {
        Ok(Self {
            item: item.to_string(),
            dates: Some(NaiveDate::parse_from_str(date_str, "%d-%b-%y")?),
        })
    }
}

macro_rules! items {
    ($item:expr, $dates:expr) => {
        Items::new($item, $dates)?
    };
}

fn main() -> Result<(), AppError> {

    let mut conn = Connection::open_in_memory()?;
    
    conn.execute(
    "CREATE TABLE ITEMS(
         DATES DATE,
         ITEM VARCHAR(9)
     )", (),
    )?;
    
    let items = vec![
    items!("apple", "01-jan-20"),
    items!("apple", "01-jan-20"),
    items!("pear", "01-jan-20"),
    items!("pear", "01-jan-20"),
    items!("pear", "02-jan-20"),
    items!("pear", "02-jan-20"),
    items!("pear", "02-jan-20"),
    items!("orange", "02-jan-20"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("INSERT INTO ITEMS(DATES, ITEM) 
                               values (?1, ?2)")?;
    for i in items {
        stmt.execute(params![i.dates, i.item])?;
    }
    }
    tx.commit()?;
    
    {
    println!("ITEMS SAMPLE(5):");
    let mut stmt = conn.prepare("SELECT * FROM ITEMS LIMIT 5")?;
    let items_iter = stmt.query_map([], |row| {Ok(Items {
                                                      item: row.get(1)?,
                                                      dates: row.get(0)?,
                                                  }
                                               )
                                              })?;
    for i in items_iter {
        println!("Found: {:?}", i.unwrap());
    }
    }
    
    {
    let mut stmt = conn.prepare(
    "WITH FREQUENCIES AS (
        SELECT
            DATES,
            ITEM,
            COUNT(*) AS FREQUENCY 
        FROM
            ITEMS
        GROUP BY
            DATES,
            ITEM
    ), ITEMS_RANKING AS (
        SELECT
            DATES,
            ITEM,
            RANK()
            OVER(PARTITION BY DATES
                 ORDER BY
                     FREQUENCY DESC
            ) AS RANK
        FROM
            FREQUENCIES
    )
    SELECT
        DATES,
        ITEM
    FROM
        ITEMS_RANKING
    WHERE
        RANK = 1"
    )?;
    println!("\nMOST FREQUENTED ITEMS:");
    
    let max_iter = stmt.query_map([], |row| {Ok(Items {
                                                    item: row.get("ITEM")?,
                                                    dates: row.get("DATES")?,
                                                }
                                               )}
                                 )?;
                                     
    for ma in max_iter {
        let ma = ma?;
        println!("Item: {:?} -> in date: {:?}",
                 ma.item,
                 ma.dates
        );
    }
    }
    
    Ok(())
}
