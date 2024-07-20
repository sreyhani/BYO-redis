#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use redis_starter_rust::rdb::read_rdb_file;

    #[test]
    fn should_read_a_key() {
        let val = read_rdb_file("tests/dump.rdb".to_owned()).expect("failed to read rdb");
        let expect = HashMap::from([("mykey".to_owned(), "myval".to_owned())]);
        assert_eq!(val.key_vals, expect);
    }
}
