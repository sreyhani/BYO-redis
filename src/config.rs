use std::{
    default,
    fmt::{write, Display},
    sync::Arc,
};

use anyhow::{anyhow, Result};

pub type SystemConfigArc = Arc<SystemConfig>;

#[derive(Debug, PartialEq, Clone)]
pub enum Role {
    Master,
    Slave,
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Slave => write!(f, "slave"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SystemConfig {
    db_dir: Option<String>,
    db_file_name: Option<String>,
    port: Option<String>,
    role: Role,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            db_dir: None,
            db_file_name: None,
            port: None,
            role: Role::Master,
        }
    }
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

    pub fn get_port(&self) -> String {
        if self.port.is_none() {
            return "6379".to_owned();
        }
        self.port.clone().unwrap()
    }

    pub fn get_role(&self) -> Role {
        self.role.clone()
    }

}

pub fn parse_args(args: impl Iterator<Item = String>) -> Result<SystemConfig> {
    let mut config = SystemConfig::default();
    let mut peek = args.peekable();
    peek.next();
    while let Some(x) = peek.next() {
        match x.as_str() {
            "--dir" => {
                let dir = peek
                    .next()
                    .ok_or(anyhow!("should provide value for --dir"))?;
                config.db_dir = Some(dir);
            }
            "--dbfilename" => {
                let file_name = peek
                    .next()
                    .ok_or(anyhow!("should provide value for --dbfilename"))?;
                config.db_file_name = Some(file_name);
            }
            "--port" => {
                let port = peek
                    .next()
                    .ok_or(anyhow!("should provide value for --port"))?;
                config.port = Some(port)
            }
            "--replicaof" => {
                config.role = Role::Slave;
                let _ = peek.next().ok_or(anyhow!("should provide value for --replicaof"))?;
            }
            _ => {}
        }
    }
    if config.db_dir != None && config.db_file_name == None {
        return Err(anyhow!("should provide --dbfilename with --dir"));
    }
    Ok(config)
}

#[cfg(test)]
mod test {
    use anyhow::Result;

    use crate::config::{Role, SystemConfig};

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
    fn should_err_if_port_has_no_value() {
        let args = vec!["exec", "--port"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        check_err(res, "provide value for --port");
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
        let args = vec![
            "exec",
            "--dir",
            "filedir",
            "--dbfilename",
            "filename",
            "--port",
            "7070",
        ];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        let expected_config = SystemConfig {
            db_dir: Some("filedir".to_owned()),
            db_file_name: Some("filename".to_owned()),
            port: Some("7070".to_owned()),
            role: Role::Master
        };
        assert_eq!(res.unwrap(), expected_config);
    }

    #[test]
    fn should_return_slave_config() {
        let args = vec![
            "exec",
            "--port",
            "7070",
            "--replicaof",
            "localhost 7171"
        ];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        let expected_config = SystemConfig {
            db_dir: None,
            db_file_name: None,
            port: Some("7070".to_owned()),
            role: Role::Slave
        };
        assert_eq!(res.unwrap(), expected_config);
    }
}
