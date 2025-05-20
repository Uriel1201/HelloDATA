use rusqlite::{Connection, Result, params}; // 0.35.0
use chrono::NaiveDate; // 0.4.41

#[derive(Debug)]
struct UserAction {
    user_id: i32,
    action: String,
    dates: Option<NaiveDate>,
}

fn main() -> Result<()> {
    let mut conn = Connection::open_in_memory()?;
    conn.execute("CREATE TABLE USERS(
                     USER_ID INTEGER, 
                     ACTION VARCHAR(9), 
                     DATES DATE)", (),
    )?;
    let tres_usuarios = vec![
    UserAction {
        user_id: 1,
        action: "start".to_string(),
        dates: Some(NaiveDate::parse_from_str("01-jan-20", "%d-%b-%y").unwrap()),
    },
    UserAction {
        user_id: 1,
        action: "cancel".to_string(),
        dates: Some(NaiveDate::parse_from_str("02-jan-20", "%d-%b-%y").unwrap()),
    },
    UserAction {
        user_id: 2,
        action: "start".to_string(),
        dates: Some(NaiveDate::parse_from_str("03-jan-20", "%d-%b-%y").unwrap()),
    },
    ];
    let tx = conn.transaction()?;
    {
    let mut stmt = tx.prepare("insert into users(USER_ID, ACTION, DATES) 
                               values (?1,?2,?3)")?;
    for users in tres_usuarios {
        stmt.execute(params![users.user_id, users.action, users.dates])?;
    }
    }
    tx.commit()?;
    Ok(())
}
                         
