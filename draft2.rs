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
    Ok(())
}
