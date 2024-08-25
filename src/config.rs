use std::{fmt::Display, sync::Arc};

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

#[derive(Debug, PartialEq, Clone)]
pub struct ReplicationConfig {
    role: Role,
    id: String,
    offset: u32,
    master_ip: String,
    master_port: String,
}

impl ReplicationConfig {
    pub fn is_slave(&self) -> bool {
        self.role == Role::Slave
    }

    pub fn get_ip_port(&self) -> (String, String) {
        (self.master_ip.clone(), self.master_port.clone())
    }
}

impl Display for ReplicationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "role:{}\n", self.role)?;
        write!(f, "master_replid:{}\n", self.id)?;
        write!(f, "master_repl_offset:{}", self.offset)
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            role: Role::Master,
            id: "0bc2cc0c5c37aee9000f72bdbb894c472a444051".to_owned(),
            offset: 0,
            master_ip: String::default(),
            master_port: String::default(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SystemConfig {
    db_dir: Option<String>,
    db_file_name: Option<String>,
    port: Option<String>,
    replication_config: ReplicationConfig,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            db_dir: None,
            db_file_name: None,
            port: None,
            replication_config: ReplicationConfig::default(),
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

    pub fn get_replication_config(&self) -> ReplicationConfig {
        self.replication_config.clone()
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
                config.replication_config.role = Role::Slave;
                let ip_port = peek
                    .next()
                    .ok_or(anyhow!("should provide value for --replicaof"))?;
                let mut parts = ip_port.split(' ');
                let (ip, port) = (parts.next().unwrap(), parts.next().unwrap());
                config.replication_config.master_ip = ip.to_owned();
                config.replication_config.master_port = port.to_owned();
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

    use crate::config::{ReplicationConfig, Role, SystemConfig};

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
            replication_config: ReplicationConfig::default(),
        };
        assert_eq!(res.unwrap(), expected_config);
    }

    #[test]
    fn should_return_slave_config() {
        let args = vec!["exec", "--port", "7070", "--replicaof", "localhost 7171"];
        let res = parse_args(args.into_iter().map(|arg| arg.to_owned()));
        let expected_config = SystemConfig {
            db_dir: None,
            db_file_name: None,
            port: Some("7070".to_owned()),
            replication_config: ReplicationConfig {
                role: Role::Slave,
                master_ip: "localhost".to_owned(),
                master_port: "7171".to_owned(),
                ..ReplicationConfig::default()
            },
        };
        assert_eq!(res.unwrap(), expected_config);
    }
}
