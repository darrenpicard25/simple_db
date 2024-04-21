use simple_db::SimpleDB;

fn main() -> std::io::Result<()> {
    let db = SimpleDB::new();

    db.put("Hello", "new world");

    let value = db
        .get("Hello")
        .map(|bytes| String::from_utf8(bytes).unwrap());

    println!("Value: {:?}", value);

    db.delete("Hello");

    let value = db
        .get("Hello")
        .map(|bytes| String::from_utf8(bytes).unwrap());

    println!("Value: {:?}", value);

    Ok(())
}
