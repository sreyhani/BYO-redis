use std::sync::Arc;

use anyhow::{anyhow, Result};

pub type SystemConfigArc = Arc<SystemConfig>;

#[derive(Debug, PartialEq)]
pub struct SystemConfig {
    db_dir: Option<String>,
    db_file_name: Option<String>,
}

impl SystemConfig {
    pub fn get_config(&self, key: &String) -> Option<String> {
        match key.as_str() {
            "dir" => self.db_dir.clone(),
            "dbfilename" => self.db_file_name.clone(),
            _ => None,
        }
    }

    pub fn get_rdb_path(&self) -> Option<String> {
        if self.db_dir.is_none() {
            return None;
        }
        Some(self.db_dir.clone().unwrap() + "/" + &self.db_file_name.clone().unwrap())
    }
}

pub fn parse_args(args: impl Iterator<Item = String>) -> Result<SystemConfig> {
    let mut config = SystemConfig {
        db_dir: None,
        db_file_name: None,
    };
    let mut peek = args.peekable();
    peek.next();
    if peek.peek() == Some(&"--dir".to_owned()) {
        peek.next();
        let dir = peek
            .next()
            .ok_or(anyhow!("should provide value for --dir"))?;
        config.db_dir = Some(dir);
        if peek.peek() == Some(&"--dbfilename".to_owned()) {
            peek.next();
            let file_name = peek
                .next()
                .ok_or(anyhow!("should provide value for --dbfilename"))?;
            config.db_file_name = Some(file_name);
        } else {
            return Err(anyhow!("should provide --dbfilename with --dir"));
        }
    }
    Ok(config)
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use crate::config::SystemConfig;

    use super::parse_args;

    fn check_err<T>(res: Result<T>, err_message: &str) {
        match res {
            Ok(_) => panic!("Expected Err"),
            Err(e) => assert!(
                e.to_string().contains(err_message),
                "failed with error: {}",
                err_message
            ),
        }
    }

    #[test]
    fn should_err_if_db_dir_has_no_value() {
        let args = vec!["exec", "--dir"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        check_err(res, "provide value for --dir");
    }

    #[test]
    fn should_err_if_dbfilename_has_no_value() {
        let args = vec!["exec", "--dir", "filedir", "--dbfilename"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        check_err(res, "provide value for --dbfilename");
    }

    #[test]
    fn should_err_if_dbfielname_is_not_specified_with_dir() {
        let args = vec!["exec", "--dir", "filedir"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        check_err(res, "provide --dbfilename with --dir");
    }

    #[test]
    fn should_return_config_with_given_values() {
        let args = vec!["exec", "--dir", "filedir", "--dbfilename", "filename"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        let expected_config = SystemConfig {
            db_dir: Some("filedir".to_owned()),
            db_file_name: Some("filename".to_owned()),
        };
        assert_eq!(res.unwrap(), expected_config);
    }
}
