use reqwest;
use scraper::{Selector,Html};
use sha2::{Digest, Sha256};
use urlencoding::decode;

use aws_config;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{Client,Error};
use aws_sdk_dynamodb::types::{KeySchemaElement, ScalarAttributeType, AttributeDefinition, KeyType, ProvisionedThroughput, AttributeValue};

const DOCS_URL_BASE:&str = "https://docs.aws.amazon.com/";
const DOCS_ROOT_URL:&str = "https://docs.aws.amazon.com/ja_jp/";
const TABLE: &str = "AWS_DOCS";
const KEY: &str = "url";

//
// get dynamodb from config
//
async fn get_db_client()-> Result<Client, Error> {
    let config = aws_config::defaults(BehaviorVersion::latest())
        .test_credentials()
        .load()
        .await;
    let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config)
        .endpoint_url(
            "http://localhost:8000",
        )
        .build();
    let client = Client::from_conf(dynamodb_local_config);
    Ok(client)
}

async fn create_table(
    client: &Client,
    table: &str,
    key: &str,
) -> Result<(), Error> {
    let tables = client.list_tables().send().await?;
    if tables.table_names().contains(&TABLE.to_string()) {
        return Ok(())
    }
    let a_name: String = key.into();
    let table_name: String = table.into();

    let ad = AttributeDefinition::builder()
        .attribute_name(&a_name)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let ks = KeySchemaElement::builder()
        .attribute_name(&a_name)
        .key_type(KeyType::Hash)
        .build()?;


    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(10)
        .write_capacity_units(5)
        .build()?;

    let create_table_response = client
        .create_table()
        .table_name(table_name)
        .key_schema(ks)
        .attribute_definitions(ad)
        .provisioned_throughput(pt)
        .send()
        .await;

    match create_table_response {
        Ok(_out) => {
            println!("Added table {} with KEY {}", table, key);
            Ok(())
        }
        Err(e) => {
            eprintln!("Got an error creating table:");
            eprintln!("{}", e);
            Err(Error::from(e))
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold("".to_owned(), |s, b| format!("{}{:x}", s, b) )
}

async fn get_html(url:&String) -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let resp = client.get(url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36")
        .send().await?.text().await?;
    Ok(resp)
}

fn build_url(base_url:&String, link:&str) -> String {
    let base = reqwest::Url::parse(base_url).unwrap();
    let mut l = link.to_string();
    if l.contains("#"){
        let v:Vec<&str> = l.split("#").collect();
        l = v[0].to_string();
    }
    if l.contains("?"){
        let v:Vec<&str> = l.split("?").collect();
        l = v[0].to_string();
    }
    let url = base.join(&l).unwrap();
    return url.to_string();
}

async fn get_links(document: &Html, selector:Selector,attr:&str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut links: Vec<String> = Vec::new();
    for element in document.select(&selector) {
        let link = element.value().attr(attr).unwrap_or("");
        let url = build_url(&DOCS_URL_BASE.to_string(), link);
        if url.starts_with(DOCS_URL_BASE) {
            links.push(url);
        }
    }
    Ok(links)
}
fn get_hash(str:&String) -> String {
    let mut hasher = Sha256::new();
    hasher.update(str.as_bytes());
    let hash = hasher.finalize();
    hex(&hash)
}

async fn put_link(link:&String, client:&Client, content_hash:String) -> Result<(), Box<dyn std::error::Error>> {
    let hash_str = get_hash(&link);
    let _item = client
        .put_item()
        .table_name(TABLE)
        .item("url", AttributeValue::S(link.to_string()))
        .item("url_hash", AttributeValue::S(hash_str))
        .item("content_hash", AttributeValue::S(content_hash))
        .send()
        .await?;
    Ok(())
}

async fn crawl(start_url:&str, cache:&mut Vec<String>,client:&Client) -> Result<bool, Box<dyn std::error::Error>> {
    let mut links: Vec<String> = Vec::new();
    let body = get_html(&start_url.to_string()).await?;
    let document = Html::parse_document(&body);
    // select anchor list
    let selector = Selector::parse("a").unwrap();
    let mut anchor_links = get_links(&document, selector,"href").await?;
    links.append(&mut anchor_links);
    // select input list
    let selector = Selector::parse("input").unwrap();
    for element in document.select(&selector) {
        let value = element.value().attr("value").unwrap_or("");
        let decoded = decode(value).unwrap();
        let doc = Html::parse_document(&decoded);
        let selector = Selector::parse("list-card-item").unwrap();
        let mut anchor_links = get_links(&doc, selector, "href").await?;
        links.append(&mut anchor_links);
    }
    for link in links {
        if cache.contains(&link) {
            continue;
        }
        cache.push(link.clone());
        let html = get_html(&link).await?;
        let content_hash = get_hash(&html);
        put_link(&link, client,content_hash).await?;
    }
    Ok(()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Root URL: {}", DOCS_ROOT_URL);
    let client = get_db_client().await?;
    create_table(&client, TABLE, KEY).await?;
    let mut cache:Vec<String> = Vec::new();
    crawl(DOCS_ROOT_URL, &mut cache, &client).await?;

    Ok(())
}
