//! Minimal PromQL selector matcher used by `--matcher` and external filters.

use crate::error::{Error, Result};
use crate::index::Labels;
use regex::Regex;

#[derive(Debug, Clone)]
pub struct Matcher {
    label: String,
    op: MatchOp,
    value: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone, Default)]
pub struct Selector {
    matchers: Vec<Matcher>,
}

impl Selector {
    pub fn parse(input: &str) -> Result<Self> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(Self::default());
        }
        let inner = trimmed
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
            .ok_or_else(|| Error::Matcher("expected a selector enclosed in { }".into()))?;
        let mut matchers = Vec::new();
        for part in split_matchers(inner) {
            if part.trim().is_empty() {
                continue;
            }
            matchers.push(Matcher::parse(part.trim())?);
        }
        Ok(Self { matchers })
    }

    pub fn matches(&self, labels: &Labels) -> bool {
        self.matchers.iter().all(|matcher| matcher.matches(labels))
    }

    pub fn is_empty(&self) -> bool {
        self.matchers.is_empty()
    }
}

impl Matcher {
    fn parse(input: &str) -> Result<Self> {
        let (index, len, op) = [
            ("=~", MatchOp::Regex),
            ("!~", MatchOp::NotRegex),
            ("!=", MatchOp::NotEqual),
            ("=", MatchOp::Equal),
        ]
        .into_iter()
        .filter_map(|(symbol, op)| input.find(symbol).map(|index| (index, symbol.len(), op)))
        .min_by_key(|(index, len, _)| (*index, std::cmp::Reverse(*len)))
        .ok_or_else(|| Error::Matcher(format!("missing operator in matcher {input:?}")))?;
        let label = input[..index].trim();
        if label.is_empty() {
            return Err(Error::Matcher(format!("empty label in matcher {input:?}")));
        }
        let raw_value = input[index + len..].trim();
        let value = strip_quotes(raw_value).to_string();
        let regex = if matches!(op, MatchOp::Regex | MatchOp::NotRegex) {
            Some(
                Regex::new(&format!("^(?:{value})$"))
                    .map_err(|e| Error::Matcher(format!("invalid regex in {input:?}: {e}")))?,
            )
        } else {
            None
        };
        Ok(Self {
            label: label.to_string(),
            op,
            value,
            regex,
        })
    }

    fn matches(&self, labels: &Labels) -> bool {
        let actual = labels.get(&self.label).map_or("", String::as_str);
        match self.op {
            MatchOp::Equal => actual == self.value,
            MatchOp::NotEqual => actual != self.value,
            MatchOp::Regex => self.regex.as_ref().is_some_and(|re| re.is_match(actual)),
            MatchOp::NotRegex => !self.regex.as_ref().is_some_and(|re| re.is_match(actual)),
        }
    }
}

fn strip_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn split_matchers(input: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut quote = None;
    let mut escape = false;
    for (index, ch) in input.char_indices() {
        if escape {
            escape = false;
            continue;
        }
        if ch == '\\' {
            escape = true;
            continue;
        }
        if let Some(q) = quote {
            if ch == q {
                quote = None;
            }
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
        } else if ch == ',' {
            out.push(&input[start..index]);
            start = index + ch.len_utf8();
        }
    }
    out.push(&input[start..]);
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn matches_equality_and_regex() {
        let selector = Selector::parse(r#"{job="api",pod=~"api-.*",env!="dev"}"#).unwrap();
        assert!(selector.matches(&labels(&[
            ("job", "api"),
            ("pod", "api-7"),
            ("env", "prod")
        ])));
        assert!(!selector.matches(&labels(&[
            ("job", "web"),
            ("pod", "api-7"),
            ("env", "prod")
        ])));
        assert!(!selector.matches(&labels(&[
            ("job", "api"),
            ("pod", "web-7"),
            ("env", "prod")
        ])));
        assert!(!selector.matches(&labels(&[("job", "api"), ("pod", "api-7"), ("env", "dev")])));
    }

    #[test]
    fn missing_label_is_empty_string() {
        assert!(Selector::parse(r#"{team=""}"#)
            .unwrap()
            .matches(&Labels::new()));
        assert!(!Selector::parse(r#"{team!=""}"#)
            .unwrap()
            .matches(&Labels::new()));
    }

    #[test]
    fn rejects_bad_regex_and_missing_operator() {
        assert!(Selector::parse(r#"{job=~"["}"#).is_err());
        assert!(Selector::parse("{job}").is_err());
    }
}
