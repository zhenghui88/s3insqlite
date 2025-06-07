pub fn read_config() -> (String, String) {
    use std::fs;
    let config_content =
        fs::read_to_string("tests/config.toml").expect("Failed to read config.toml");
    let config: toml::Value = toml::from_str(&config_content).expect("Failed to parse config.toml");
    let endpoint = format!(
        "http://{}:{}",
        config["bind_address"]
            .as_str()
            .expect("Missing bind_address"),
        config["port"].as_integer().expect("Missing port")
    );
    let bucket = config["buckets"][0]
        .as_str()
        .expect("Missing bucket name")
        .to_string();
    (endpoint, bucket)
}
