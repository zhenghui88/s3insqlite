use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Serialize)]
pub struct ListBucketResult {
    pub name: String,
    pub prefix: String,
    pub delimiter: Option<char>,
    pub max_keys: i32,
    pub is_truncated: bool,
    pub encoding_type: Option<String>,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub start_after: Option<String>,
    pub contents: Vec<S3Object>,
    pub common_prefixes: Vec<CommonPrefix>,
}

#[derive(Debug, Serialize)]
pub struct S3Object {
    pub key: String,
    pub size: usize,
    pub last_modified: DateTime<Utc>,
    pub etag: String,
    pub storage_class: String,
}

#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    pub prefix: String,
}

impl ListBucketResult {
    pub fn new(bucket: &str, prefix: &str, delimiter: Option<char>) -> Self {
        Self {
            name: bucket.to_string(),
            prefix: prefix.to_string(),
            delimiter,
            max_keys: i32::MAX, // Default to maximum of i32 to allow all keys
            is_truncated: false,
            encoding_type: None,
            continuation_token: None,
            next_continuation_token: None,
            start_after: None,
            contents: Vec::new(),
            common_prefixes: Vec::new(),
        }
    }

    pub fn to_xml(&self) -> String {
        let mut xml = String::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
        );

        xml.push_str(&format!("<Name>{}</Name>", self.name));
        xml.push_str(&format!("<Prefix>{}</Prefix>", self.prefix));

        if let Some(delimiter) = self.delimiter {
            xml.push_str(&format!("<Delimiter>{}</Delimiter>", delimiter));
        }

        if let Some(ref encoding_type) = self.encoding_type {
            xml.push_str(&format!("<EncodingType>{}</EncodingType>", encoding_type));
        }

        if let Some(ref token) = self.continuation_token {
            xml.push_str(&format!("<ContinuationToken>{}</ContinuationToken>", token));
        }

        xml.push_str(&format!("<KeyCount>{}</KeyCount>", self.contents.len()));
        xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", self.max_keys));
        xml.push_str(&format!("<IsTruncated>{}</IsTruncated>", self.is_truncated));

        if let Some(ref token) = self.next_continuation_token {
            xml.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                token
            ));
        }

        if let Some(ref start_after) = self.start_after {
            xml.push_str(&format!("<StartAfter>{}</StartAfter>", start_after));
        }

        // Add contents
        for object in &self.contents {
            xml.push_str("<Contents>");
            xml.push_str(&format!("<Key>{}</Key>", object.key));
            xml.push_str(&format!(
                "<LastModified>{}</LastModified>",
                object
                    .last_modified
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));
            xml.push_str(&format!("<ETag>{}</ETag>", object.etag));
            xml.push_str(&format!("<Size>{}</Size>", object.size));
            xml.push_str(&format!(
                "<StorageClass>{}</StorageClass>",
                object.storage_class
            ));
            xml.push_str("</Contents>");
        }

        // Add common prefixes
        for prefix in &self.common_prefixes {
            xml.push_str("<CommonPrefixes>");
            xml.push_str(&format!("<Prefix>{}</Prefix>", prefix.prefix));
            xml.push_str("</CommonPrefixes>");
        }

        xml.push_str("</ListBucketResult>");
        xml
    }

    // Set encoding type (url or none)
    pub fn set_encoding_type(&mut self, encoding_type: Option<String>) {
        self.encoding_type = encoding_type;
    }

    // Set continuation token parameters
    pub fn set_continuation(&mut self, token: Option<String>, next_token: Option<String>) {
        self.continuation_token = token;
        self.next_continuation_token = next_token;
    }

    // Set maximum keys
    pub fn set_max_keys(&mut self, max_keys: i32) {
        self.max_keys = max_keys;
    }

    // Set start-after parameter
    pub fn set_start_after(&mut self, start_after: Option<String>) {
        self.start_after = start_after;
    }

    fn add_content(
        &mut self,
        key: String,
        size: usize,
        last_modified: DateTime<Utc>,
        md5_hash: Option<String>,
    ) {
        self.contents.push(S3Object {
            key,
            size,
            last_modified,
            etag: md5_hash
                .map(|h| format!("\"{}\"", h))
                .unwrap_or_else(|| "\"00000000000000000000000000000000\"".to_string()),
            storage_class: "STANDARD".to_string(),
        });
    }

    // Process keys with MD5 hashes
    pub fn process_keys(&mut self, keys: Vec<(String, usize, DateTime<Utc>, Option<String>)>) {
        if self.delimiter.is_none() {
            // No delimiter, add all keys to contents
            for (key, size, last_modified, md5_hash) in keys {
                if key.starts_with(&self.prefix) {
                    self.add_content(key, size, last_modified, md5_hash);
                }
            }
            return;
        }

        // If delimiter is set, we need to find common prefixes
        let delimiter = self.delimiter.unwrap();
        let mut prefixes = HashSet::new();

        for (key, size, last_modified, md5_hash) in keys {
            // Check if key contains delimiter after prefix
            if let Some(suffix) = key.strip_prefix(&self.prefix) {
                if let Some(pos) = suffix.find(delimiter) {
                    // Extract common prefix
                    let common_prefix = format!("{}{}", self.prefix, &suffix[..=pos]);
                    prefixes.insert(common_prefix);
                } else {
                    // No delimiter found, add to contents
                    self.add_content(key, size, last_modified, md5_hash);
                }
            } else {
                continue; // Key does not start with prefix
            }
        }

        // Add all common prefixes
        for prefix in prefixes {
            self.common_prefixes.push(CommonPrefix { prefix });
        }
    }
}
