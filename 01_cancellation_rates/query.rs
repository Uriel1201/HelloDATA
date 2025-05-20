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
struct UserAction {
    user_id: i32,
    action: String,
    dates: Option<NaiveDate>,
}

#[derive(Debug)]
struct UserStats {
    user_id: i32, 
    publish_rate: Option<f64>,
    cancel_rate: Option<f64>,
}

impl UserAction {
    fn new(user_id: i32, action: &str, date_str: &str) -> Result<Self, chrono::ParseError> {
        Ok(Self {
            user_id,
            action: action.to_string(),
            dates: Some(NaiveDate::parse_from_str(date_str, "%d-%b-%y")?),
        })
    }
}

macro_rules! user_action {
    ($id:expr, $action:expr, $dates:expr) => {
        UserAction::new($id, $action, $dates)?
    };
}

fn main() -> Result<(), AppError> {
    
    let mut conn = Connection::open_in_memory()?;
    
    conn.execute("CREATE TABLE USERS(
                     USER_ID INTEGER, 
                     ACTION VARCHAR(9), 
                     DATES DATE)", (),
    )?;
    
    let users = vec![
    user_action!(1, "start", "01-jan-20"),
    user_action!(1, "cancel", "02-jan-20"),
    user_action!(2, "start", "03-jan-20"),
    user_action!(2, "publish", "04-jan-20"),
    user_action!(3, "start", "05-jan-20"),
    user_action!(3, "cancel", "06-jan-20" ),
    user_action!(1, "start", "07-jan-20"),
    user_action!(1, "publish", "08-jan-20"),
    ];
    
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("insert into users(USER_ID, ACTION, DATES) 
                               values (?1,?2,?3)")?;
    for u in users {
        stmt.execute(params![u.user_id, u.action, u.dates])?;
    }
    }
    tx.commit()?;
    
    {
    println!("USERS TABLE SAMPLE(5):");
    let mut stmt = conn.prepare("SELECT * FROM USERS LIMIT 5")?;
    let user_iter = stmt.query_map([], |row| {Ok(UserAction{
                                                            user_id: row.get(0)?,
                                                            action: row.get(1)?,
                                                            dates: row.get(2)?,
                                                           }
                                                )
                                             })?;
    for u in user_iter {
        println!("Found user {:?}", u.unwrap());
    }
    }
    
    {
    let mut stmt = conn.prepare("WITH TOTALS AS (
                                 SELECT
                                 USER_ID,
                                 SUM(
                                 CASE
                                     WHEN ACTION = 'start' THEN
                                          1
                                     ELSE
                                          0
                                     END
                                 ) AS TOTAL_STARTS,
                                 SUM(
                                 CASE
                                     WHEN ACTION = 'cancel' THEN
                                          1
                                     ELSE
                                          0
                                     END
                                 ) AS TOTAL_CANCELS,
                                 SUM(
                                     CASE
                                     WHEN ACTION = 'publish' THEN
                                          1
                                     ELSE
                                          0
                                     END
                                 ) AS TOTAL_PUBLISHES
                                 FROM
                                 USERS
                                 GROUP BY USER_ID
                                 ) SELECT USER_ID,
                                          ROUND(1.0 * TOTAL_PUBLISHES / NULLIF(TOTAL_STARTS, 0),
                                                2) AS PUBLISH_RATE,
                                          ROUND(1.0 * TOTAL_CANCELS / NULLIF(TOTAL_STARTS, 0),
                                                2) AS CANCEL_RATE
                                   FROM TOTALS")?;
    
    let stats_iter = stmt.query_map([], |row| {Ok(UserStats{
                                                            user_id: row.get("USER_ID")?,
                                                            publish_rate: row.get("PUBLISH_RATE")?,
                                                            cancel_rate: row.get("CANCEL_RATE")?,
                                                           }
                                                )
                                              }
                                   )?;
    
    println!("\nUSER STATISTICS:");
    for stat in stats_iter {
        let stat = stat?;
        println!("User {} -> Publish: {}%, Cancel: {}%",
                 stat.user_id,
                 stat.publish_rate.unwrap_or(0.0)*100.0,
                 stat.cancel_rate.unwrap_or(0.0)*100.0
        );
    }
    }
    Ok(())
}
                         
